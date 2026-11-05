mod adopt;
mod backend;
mod claude;
mod config;
mod handoff;
mod limit;
mod markdown;
mod opencode;
mod outbox;
mod telegram;
#[cfg(test)]
mod testkit;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, Notify};

use backend::{Backend, Event};
use config::Config;
use handoff::Turn;
use outbox::{Item, Kind, Policy, SentRecord};
use telegram::{Bot, Message};

/// Persisted per-chat state. Сессии у claude и opencode разные, поэтому
/// session_id хранится отдельно для каждого бэкенда.
#[derive(Default, Clone, Serialize, Deserialize)]
struct ChatState {
    /// Claude-сессия (--resume).
    session_id: Option<String>,
    /// OpenCode-сессия (--session).
    #[serde(default)]
    opencode_session_id: Option<String>,
    /// Агент для чата; None — глобальное значение CLAUDETG_BACKEND.
    #[serde(default)]
    backend: Option<Backend>,
    work_dir: Option<PathBuf>,
    /// Когда сам отправить «Продолжай» (unix-время, UTC): агент упёрся в лимит.
    #[serde(default)]
    resume_at: Option<i64>,
    /// То же время для человека: «01.11 10:41 Europe/Moscow».
    #[serde(default)]
    resume_label: Option<String>,
    /// Основной агент замещается другим из-за лимита (claude → opencode).
    /// Значение — агент, к которому вернёмся при сбросе лимита.
    #[serde(default)]
    fallback_from: Option<Backend>,
    /// Расшифровка контекста claude; подклеивается к первому сообщению opencode.
    #[serde(default)]
    handoff: Option<String>,
    /// Заметка о смене opencode; подклеивается к первому сообщению claude.
    #[serde(default)]
    gap_note: Option<String>,
    /// Последние обмены чата — источник заметок, когда файл сессии не найден.
    #[serde(default)]
    recent: Vec<Turn>,
}

impl ChatState {
    fn session_for(&self, backend: Backend) -> Option<&String> {
        match backend {
            Backend::Claude => self.session_id.as_ref(),
            Backend::OpenCode => self.opencode_session_id.as_ref(),
        }
    }

    fn set_session(&mut self, backend: Backend, id: String) {
        match backend {
            Backend::Claude => self.session_id = Some(id),
            Backend::OpenCode => self.opencode_session_id = Some(id),
        }
    }

    fn reset_sessions(&mut self) {
        self.session_id = None;
        self.opencode_session_id = None;
    }
}

struct App {
    cfg: Config,
    bot: Bot,
    chats: Mutex<HashMap<i64, ChatState>>,
    /// Agent run in progress, per chat.
    running: Mutex<HashMap<i64, Running>>,
    /// Задачи, накопленные окном агрегации: человек печатает задачу частями —
    /// запускаем одной задачей после тишины, но не дольше предельного окна.
    batches: Mutex<HashMap<i64, Batch>>,
    /// Предыдущее сообщение чата: последовательные повторы схлопываем в одно.
    last_text: Mutex<HashMap<i64, String>>,
    /// Пузырь статуса на каждый чат: одно сообщение, которое бот правит на
    /// протяжении задачи, вместо потока сообщений «выполняю команду».
    status: Mutex<HashMap<i64, Bubble>>,
    /// Последние отправленные в чат тексты — по ним отсекаем повторы.
    recent_out: Mutex<HashMap<i64, Vec<String>>>,
}

struct Running {
    cancel: Arc<Notify>,
    /// Messages that arrived during the current turn; sent together as the next turn.
    queue: Vec<String>,
}

/// Сколько последних отправленных текстов чата помнить для защиты от повторов
/// (и в памяти запущенного бота, и после перезапуска — по журналу sent).
const RECENT_SENDS: usize = 8;

struct Batch {
    texts: Vec<String>,
    /// Момент первой накопленной задачи — от него отсчитывается предельное окно.
    first_at: tokio::time::Instant,
    /// Поколение окна: таймер старого поколения не закрывает обновлённое окно.
    gen: u64,
}

/// Пузырь статуса: единственное сообщение о том, чем сейчас занят агент. Бот
/// создаёт его один раз на задачу и дальше правит — так в чате не появляется
/// 9-20 сообщений в минуту.
#[derive(Default)]
struct Bubble {
    message_id: Option<i64>,
    /// Последняя строка: что агент делает сейчас.
    line: String,
    /// Сколько инструментов агент запустил и сколько служебных строк не ушло в чат.
    actions: usize,
    dropped: usize,
    started: Option<tokio::time::Instant>,
    /// Когда пузырь правили последний раз — чтобы не дёргать Telegram каждые 100 мс.
    rendered: Option<tokio::time::Instant>,
}

impl Bubble {
    /// Текст пузыря: строка о текущем действии плюс счётчики.
    fn render(&self) -> String {
        let mut parts = Vec::new();
        if self.actions > 0 {
            parts.push(format!("действий: {}", self.actions));
        }
        if let Some(started) = self.started {
            parts.push(format!("времени: {} с", started.elapsed().as_secs()));
        }
        if self.dropped > 0 {
            parts.push(format!("служебных строк скрыто: {}", self.dropped));
        }
        let counter = parts.join(" · ");
        if counter.is_empty() {
            self.line.clone()
        } else {
            format!("{}\n{}", self.line, counter)
        }
    }
}

impl App {
    fn load_state(cfg: &Config) -> HashMap<i64, ChatState> {
        std::fs::read_to_string(&cfg.state_file)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    async fn save_state(&self) {
        let data = serde_json::to_string_pretty(&*self.chats.lock().await).unwrap_or_default();
        if let Some(dir) = self.cfg.state_file.parent() {
            let _ = tokio::fs::create_dir_all(dir).await;
        }
        if let Err(e) = tokio::fs::write(&self.cfg.state_file, data).await {
            log::error!("cannot save state to {}: {e}", self.cfg.state_file.display());
        }
    }

    async fn chat_state(&self, chat_id: i64) -> ChatState {
        self.chats.lock().await.get(&chat_id).cloned().unwrap_or_default()
    }

    async fn update_chat(&self, chat_id: i64, f: impl FnOnce(&mut ChatState)) {
        f(self.chats.lock().await.entry(chat_id).or_default());
        self.save_state().await;
    }

    /// Агент, с которым сейчас работает чат.
    async fn backend_of(&self, chat_id: i64) -> Backend {
        self.chat_state(chat_id).await.backend.unwrap_or(self.cfg.backend)
    }

    /// Ставит текст в исходящую очередь: бот отправит его пачкой вместе с
    /// другими сообщениями шага — так в чате не будет десятка сообщений подряд.
    async fn queue(&self, chat_id: i64, kind: Kind, text: &str) {
        if let Err(e) = outbox::enqueue(&self.cfg.outbox_dir, &Item::new(chat_id, kind, text)) {
            log::error!("cannot queue a message for {chat_id}: {e}");
            self.bot.send_markdown(chat_id, text).await;
        }
    }

    /// Пузырь статуса: создаёт или правит одно сообщение чата.
    async fn status(&self, chat_id: i64, line: &str) {
        if !self.cfg.status_bubble {
            return;
        }
        let (text, message_id) = {
            let mut bubbles = self.status.lock().await;
            let bubble = bubbles.entry(chat_id).or_default();
            if !line.trim().is_empty() {
                bubble.line = line.trim().to_string();
            }
            bubble.started.get_or_insert_with(tokio::time::Instant::now);
            (bubble.render(), bubble.message_id)
        };
        let keep = if let Some(id) = message_id {
            if self.bot.edit_text(chat_id, id, &text).await {
                Some(id)
            } else {
                self.bot.send_text_ids(chat_id, &text).await.unwrap_or_default().into_iter().next()
            }
        } else {
            self.bot.send_text_ids(chat_id, &text).await.unwrap_or_default().into_iter().next()
        };
        let mut bubbles = self.status.lock().await;
        if let Some(bubble) = bubbles.get_mut(&chat_id) {
            bubble.message_id = keep;
            bubble.rendered = Some(tokio::time::Instant::now());
        }
    }

    /// Строка о запущенном инструменте: только в пузырь статуса, в чат она не идёт.
    async fn status_progress(&self, chat_id: i64, line: &str) {
        if !self.cfg.status_bubble {
            return;
        }
        let need = {
            let mut bubbles = self.status.lock().await;
            let bubble = bubbles.entry(chat_id).or_default();
            bubble.actions += 1;
            if bubble.line == line {
                // то же действие повторяется — править сообщение незачем
                false
            } else {
                bubble.line = line.to_string();
                bubble
                    .rendered
                    .map(|t| t.elapsed() >= Duration::from_millis(1500))
                    .unwrap_or(true)
            }
        };
        if need {
            self.status(chat_id, "").await;
        }
    }

    /// Счётчик служебных строк, которые не ушли в чат.
    async fn status_dropped(&self, chat_id: i64) {
        self.status.lock().await.entry(chat_id).or_default().dropped += 1;
    }

    /// Задача кончилась: пузырь статуса убираем из чата, он больше не нужен.
    async fn status_finish(&self, chat_id: i64) {
        let bubble = self.status.lock().await.remove(&chat_id);
        if let Some(bubble) = bubble {
            if let Some(id) = bubble.message_id {
                self.bot.delete(chat_id, id).await;
            }
        }
    }

    fn log_sent(&self, chat_id: i64, ids: &[i64], kind: Kind, text: &str) {
        if ids.is_empty() {
            return;
        }
        let preview: String = text.chars().take(160).collect();
        let records: Vec<SentRecord> = ids
            .iter()
            .map(|id| SentRecord {
                chat: chat_id,
                message_id: *id,
                ts: outbox::now_ms(),
                kind,
                preview: preview.clone(),
            })
            .collect();
        if let Err(e) = outbox::append_sent(&self.cfg.sent_log, &records) {
            log::warn!("cannot write {}: {e}", self.cfg.sent_log.display());
        }
    }

    /// Отправляет всё, что накопилось в очереди чата: повторы и служебную
    /// болтовню не отправляем, остальное склеиваем в одно сообщение.
    async fn drain_outbox(&self, chat_id: i64) {
        let all = outbox::pending(&self.cfg.outbox_dir);
        let mut mine: Vec<(PathBuf, Item)> =
            all.into_iter().filter(|(_, it)| it.chat == chat_id).collect();
        if mine.is_empty() {
            return;
        }
        // Файлы очереди убираем только после отправки: сетевой сбой не должен
        // съедать сообщение — оно дойдёт, когда связь вернётся.
        // Но и вечной очереди нет: пережидая лимит Telegram или отсутствие связи,
        // тексты устаревают, и в чат приходит «вчерашний ответ» («бот гонит старые
        // сообщения»). Просроченное снимаем с очереди и говорим об этом прямо.
        let ttl_ms = i64::try_from(self.cfg.send_ttl_secs.saturating_mul(1000)).unwrap_or(i64::MAX);
        let stale = outbox::stale_paths(&mine, outbox::now_ms(), ttl_ms);
        if !stale.is_empty() {
            log::warn!(
                "chat {chat_id}: {}/{} сообщений в очереди старше {} мин — в чат не уйдут",
                stale.len(),
                mine.len(),
                self.cfg.send_ttl_secs / 60
            );
            outbox::discard(&stale);
            mine.retain(|(p, _)| !stale.iter().any(|s| s == p));
            if mine.is_empty() {
                return;
            }
        }
        // Файлы идут отдельно от текстов: их нельзя слить в одно сообщение, а
        // сбой отправки оставляет в очереди только сами файлы — тексты шага
        // уйдут своим чередом.
        let (files, texts): (Vec<&(PathBuf, Item)>, Vec<&(PathBuf, Item)>) =
            mine.iter().partition(|(_, it)| it.is_file());
        if !files.is_empty() {
            self.send_files(chat_id, &files).await;
        }
        let text_paths: Vec<PathBuf> = texts.iter().map(|(p, _)| p.clone()).collect();
        let mut kept: Vec<String> = Vec::new();
        let mut kind = Kind::Agent;
        for (_, item) in texts {
            let text = item.text.trim();
            if text.is_empty() {
                continue;
            }
            if item.kind == Kind::Answer {
                kind = Kind::Answer;
            }
            if self.cfg.drop_chatter && outbox::is_chatter(text) {
                log::info!("chat {chat_id}: служебная строка не отправлена: {text}");
                self.status_dropped(chat_id).await;
                continue;
            }
            {
                let mut recent = self.recent_out.lock().await;
                if outbox::is_repeat(text, recent.entry(chat_id).or_default()) {
                    log::info!("chat {chat_id}: повтор не отправлен");
                    self.status_dropped(chat_id).await;
                    continue;
                }
            }
            kept.push(text.to_string());
        }
        if kept.is_empty() {
            outbox::discard(&text_paths);
            return;
        }
        let merged = outbox::merge(&kept);
        match self.bot.send_markdown_ids(chat_id, &merged).await {
            Ok(ids) => {
                outbox::discard(&text_paths);
                self.log_sent(chat_id, &ids, kind, &merged);
            }
            Err(e) => {
                log::warn!("chat {chat_id}: сообщение не отправлено ({e:#}), ждём связи");
                return;
            }
        }
        let mut recent = self.recent_out.lock().await;
        let recent = recent.entry(chat_id).or_default();
        for text in kept {
            recent.push(text);
        }
        // помним последние 8 сообщений чата
        if recent.len() > 8 {
            let extra = recent.len() - 8;
            recent.drain(0..extra);
        }
    }

    /// Отдаёт файлы очереди и снимает с очереди только те, что реально ушли.
    /// Файла нет на диске — снимаем молча: отправлять нечем.
    async fn send_files(&self, chat_id: i64, files: &[&(PathBuf, Item)]) {
        let (mut gone, mut sent_paths, mut ids) = (Vec::new(), Vec::new(), Vec::new());
        let mut sent_entries = Vec::new();
        for (path, item) in files {
            let attach = item.path.clone().unwrap_or_default();
            if !attach.exists() {
                let shown = attach.display().to_string();
                log::warn!("chat {chat_id}: файла уже нет на диске — {shown} снят с очереди");
                gone.push(path.clone());
                continue;
            }
            // тот же файл уже отдавали в последние восемь сообщений чата: второй раз
            // слать не нужно (сравниваем путь целиком, а не подпись)
            let key = attach.display().to_string();
            let duplicated = self
                .recent_out
                .lock()
                .await
                .get(&chat_id)
                .is_some_and(|r| r.iter().any(|t| *t == key));
            if duplicated {
                log::info!("chat {chat_id}: файл уже отправлен — {key}");
                gone.push(path.clone());
                continue;
            }
            let caption = item.text.trim();
            let name = attach.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| attach.display().to_string());
            match self.bot.send_document(chat_id, &attach, (!caption.is_empty()).then_some(caption)).await {
                Ok(message_id) => {
                    sent_paths.push(key);
                    sent_entries.push(path.clone());
                    ids.push(message_id);
                    log::info!("chat {chat_id}: файл отправлен — {name}");
                }
                Err(e) => log::warn!("chat {chat_id}: файл {name} не отправлен ({e:#}), ждём связи"),
            }
        }
        outbox::discard(&gone);
        outbox::discard(&sent_entries);
        if !ids.is_empty() {
            self.log_sent(chat_id, &ids, Kind::Answer, &sent_paths.join(" "));
            let mut recent = self.recent_out.lock().await;
            let recent = recent.entry(chat_id).or_default();
            recent.extend(sent_paths);
            if recent.len() > 8 {
                let extra = recent.len() - 8;
                recent.drain(0..extra);
            }
        }
    }
}

/// Разбор подкоманд: Some(результат) — команда выполнена и в поллинг идти не надо.
/// Неизвестная подкоманда не должна тихо поднимать второй поллинг-бот:
/// он вступает в конфликт с сервисом и забирает сообщения у него.
async fn cli_dispatch(
    bot: &Bot,
    cfg: &Config,
    args: &[String],
    text: &str,
) -> Result<Option<Result<()>>> {
    // `claudetg send <chat_id> < file.md` — поставить Markdown в очередь чата и
    // выйти: живой бот отправит её пачкой вместе с остальными сообщениями шага.
    // Если бот не работает, отправляем сами — сообщение не должно пропасть.
    if args.get(1).map(String::as_str) == Some("send") {
        return Ok(Some(cli_send(bot, cfg, args.get(2).map(String::as_str), text).await));
    }
    // `claudetg clean [--chat <id>] [--all] [--keep N] [--dry]` — убрать из чата
    // свои сообщения по журналу отправленного.
    if args.get(1).map(String::as_str) == Some("clean") {
        return Ok(Some(clean_messages(bot, cfg, &args[2..]).await));
    }
    // `claudetg send-file <path> [подпись]` — прислать файл документом в чат
    // CLAUDETG_CHAT_ID (бот выставляет его процессу claude) или --chat <id>.
    if args.get(1).map(String::as_str) == Some("send-file") {
        let chat = std::env::var("CLAUDETG_CHAT_ID").ok();
        return Ok(Some(cli_send_file(bot, cfg, chat.as_deref(), &args[2..]).await));
    }
    match args.get(1).map(String::as_str) {
        None => {}
        Some("run") | Some("start") | Some("poll") => {}
        Some(other) => {
            anyhow::bail!(
                "неизвестная подкоманда {other:?}; доступно: run (по умолчанию), send, send-file, clean"
            );
        }
    }
    Ok(None)
}

/// `claudetg send <chat_id> < text`: живой бот ставит текст в очередь, а без
/// живого бота отправляем сами — сообщение не должно пропасть.
async fn cli_send(bot: &Bot, cfg: &Config, chat_arg: Option<&str>, text: &str) -> Result<()> {
    let chat_id: i64 = chat_arg
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("usage: claudetg send <chat_id> < message.md"))?;
    if outbox::bot_alive(&cfg.pid_file) {
        outbox::enqueue(&cfg.outbox_dir, &Item::new(chat_id, Kind::Agent, text.to_string()))?;
        log::info!("сообщение поставлено в очередь: {}", cfg.outbox_dir.display());
    } else {
        log::warn!("бот не работает ({}), отправляем напрямую", cfg.pid_file.display());
        if !bot.send_markdown_ids(chat_id, text).await.is_ok() {
            anyhow::bail!("не удалось отправить в чат {chat_id}");
        }
    }
    Ok(())
}

/// `claudetg send-file [--chat <id>] <path> [подпись]`: файл документом в чат.
/// Чат берётся из аргумента, иначе из CLAUDETG_CHAT_ID (его выставляет бот).
/// Живой бот ставит файл в очередь — так отдача файла пережидает лимит Telegram,
/// не уходит повторно и попадает в журнал отправленного. Без живого бота файл
/// отправляем сами: он не должен пропасть.
async fn cli_send_file(bot: &Bot, cfg: &Config, env_chat: Option<&str>, rest: &[String]) -> Result<()> {
    let mut rest: Vec<String> = rest.to_vec();
    let mut chat = env_chat.map(str::to_string);
    if let Some(i) = rest.iter().position(|a| a == "--chat") {
        chat = rest.get(i + 1).cloned();
        rest.drain(i..(i + 2).min(rest.len()));
    }
    let usage = "usage: claudetg send-file [--chat <id>] <path> [caption]";
    let chat_id: i64 = chat
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("{usage}: chat id not set (CLAUDETG_CHAT_ID)"))?;
    let path = rest.first().ok_or_else(|| anyhow::anyhow!("{usage}"))?;
    let caption = rest.get(1).map(String::as_str).unwrap_or("");
    if outbox::bot_alive(&cfg.pid_file) {
        outbox::enqueue(&cfg.outbox_dir, &Item::file(chat_id, Kind::Agent, path, caption))?;
        log::info!("файл поставлен в очередь: {}", cfg.outbox_dir.display());
        Ok(())
    } else {
        log::warn!("бот не работает ({}), отправляем напрямую", cfg.pid_file.display());
        let caption = (!caption.is_empty()).then_some(caption);
        bot.send_document(chat_id, std::path::Path::new(path), caption).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().collect();
    // Версию печатаем до чтения конфига: она нужна и без настроек.
    if matches!(args.get(1).map(String::as_str), Some("--version" | "-V" | "version")) {
        println!("claudetg {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let cfg = Config::from_env()?;
    let bot = Bot::new(&cfg.bot_token, cfg.proxy_url.as_deref(), cfg.api_base.as_deref())?;
    // текст для `send` читаем из stdin: его передаёт агент или человек из консоли
    let send_text = if args.get(1).map(String::as_str) == Some("send") {
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)?;
        buf
    } else {
        String::new()
    };
    if let Some(r) = cli_dispatch(&bot, &cfg, &args, &send_text).await? {
        return r;
    }
    run_bot(cfg, bot).await
}

/// Запуск поллинг-бота: представление о себе, подготовка состояния, фоновые
/// задачи (очередь и автопродолжения) и цикл getUpdates.
async fn run_bot(cfg: Config, bot: Bot) -> Result<()> {
    let me = bot.get_me().await?;
    let mut ids: Vec<String> = cfg.allowed_users.iter().map(i64::to_string).collect();
    ids.sort();
    log::info!(
        "started as @{} (proxy: {}), allowed users: {}",
        me["username"].as_str().unwrap_or("?"),
        cfg.proxy_url.as_deref().unwrap_or("none"),
        ids.join(", ")
    );
    if cfg.allowed_users.iter().any(|id| *id == 123_456_789) {
        log::warn!(
            "ALLOWED_USER_IDS содержит 123456789 — это заглушка из deploy/claudetg.env.example; \
             сообщения настоящих пользователей будут игнорироваться"
        );
    }

    let app = Arc::new(App {
        chats: Mutex::new(App::load_state(&cfg)),
        running: Mutex::new(HashMap::new()),
        batches: Mutex::new(HashMap::new()),
        last_text: Mutex::new(HashMap::new()),
        status: Mutex::new(HashMap::new()),
        // память о повторах переживает перезапуск бота: берём последние тексты
        // из журнала отправленного, иначе рестарт отправляет чату то же самое
        recent_out: Mutex::new(outbox::seed_recent(&outbox::read_sent(&cfg.sent_log), RECENT_SENDS)),
        bot,
        cfg,
    });

    // Pid-файл: по нему `claudetg send` понимает, что очередь разберёт бот.
    outbox::write_pid(&app.cfg.pid_file)?;
    log::info!(
        "очередь исходящих: {} (тишина {} с, максимум {} с и {} сообщ.), отсекаем болтовню: {}",
        app.cfg.outbox_dir.display(),
        app.cfg.send_quiet_secs,
        app.cfg.send_max_secs,
        app.cfg.send_max_items,
        app.cfg.drop_chatter
    );
    log::info!("вложения из чата сохраняем в {}", app.cfg.attach_dir.display());

    // Перезапуск бота не убивает смену агента (KillMode=process): отмечаем,
    // какие сессии всё ещё ведутся процессом от прежнего запуска.
    {
        let table = adopt::proc_cmdlines();
        let me = std::process::id();
        let chats = app.chats.lock().await;
        for (chat_id, state) in chats.iter() {
            let backend = state.backend.unwrap_or(app.cfg.backend);
            if let Some(session) = state.session_for(backend) {
                if let Some(pid) = adopt::find_session_pid(&table, session, me) {
                    log::info!("chat {chat_id}: смену ведёт процесс {pid} от прежнего запуска бота");
                }
            }
        }
    }

    tokio::spawn(flush_outbox(app.clone()));
    tokio::spawn(resume_scheduler(app.clone()));

    let mut offset = 0;
    loop {
        match poll_once(&app, &mut offset).await {
            Ok(()) => {}
            Err(e) => {
                log::warn!("getUpdates: {e:#}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

/// Один виток поллинга: забираем обновления и заводём на каждое задачу.
/// Смещение сдвигаем сразу — обработанное обновление не повторится, даже
/// если следующая выборка упадёт.
async fn poll_once(app: &Arc<App>, offset: &mut i64) -> Result<()> {
    for update in app.bot.get_updates(*offset).await? {
        *offset = update.update_id + 1;
        if let Some(msg) = update.message {
            let app = app.clone();
            tokio::spawn(async move { handle_message(app, msg).await });
        }
    }
    Ok(())
}

async fn handle_message(app: Arc<App>, msg: Message) {
    let chat_id = msg.chat.id;
    let user_id = msg.from.as_ref().map(|u| u.id).unwrap_or_default();
    if !app.cfg.allowed_users.contains(&user_id) {
        log::warn!("ignored message from unauthorized user {user_id} in chat {chat_id}");
        return;
    }
    // Сначала вложения: человек прислал файл — качаем и отдаём агенту локальный путь.
    let files = save_attachments(&app, chat_id, &msg).await;
    let text = msg.text.or(msg.caption).unwrap_or_default().trim().to_string();
    if text.is_empty() && files.is_empty() {
        app.bot.send_text(chat_id, "Пока понимаю только текст и файлы.").await;
        return;
    }
    if text.is_empty() {
        let listed = list_files(&files);
        submit_text(
            &app,
            chat_id,
            format!(
                "Пользователь прислал файл(ы). Они уже скачаны, локальные пути:\n{listed}\n\
                 Открой их и разберись, что там: коротко опиши содержание и спроси, что с ними сделать."
            ),
        )
        .await;
        return;
    }
    if !files.is_empty() {
        let listed = list_files(&files);
        submit_text(&app, chat_id, format!("{text}\n\nФайлы от пользователя (уже скачаны):\n{listed}")).await;
        return;
    }

    if text.starts_with('/') {
        let (cmd, arg) = text.split_once(char::is_whitespace).unwrap_or((&text, ""));
        let cmd = cmd.split('@').next().unwrap_or(cmd);
        handle_command(&app, chat_id, cmd, arg.trim()).await;
        return;
    }

    submit_text(&app, chat_id, text).await;
}

/// Строки со списком скачанных файлов: путь, имя, тип и размер.
fn list_files(files: &[telegram::Attachment]) -> String {
    files
        .iter()
        .map(|f| {
            format!(
                "· {} — {}, {}, {}",
                f.local.display(),
                f.name,
                f.kind,
                human_size(f.size)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Скачивает вложения сообщения в каталог бота. Неудачу поясняет в чате одной
/// строкой; возвращает то, что удалось сохранить.
async fn save_attachments(app: &Arc<App>, chat_id: i64, msg: &Message) -> Vec<telegram::Attachment> {
    let mut saved = Vec::new();
    for file in msg.attachments() {
        let kind = match file.mime_type.as_deref().map(|m| m.split('/').next().unwrap_or("")) {
            Some("image") => "изображение",
            Some("audio") => "аудио",
            Some("video") => "видео",
            _ if file.width.is_some() => "изображение",
            _ if file.file_name.is_some() => "документ",
            _ => "файл",
        };
        let name = file.file_name.clone().unwrap_or_else(|| "файл".into());
        let limit = app.cfg.attach_max_mb * 1024 * 1024;
        if app.cfg.attach_max_mb > 0 && file.file_size.unwrap_or(0) > limit {
            app.bot
                .send_text(
                    chat_id,
                    &format!(
                        "Файл «{name}» больше лимита {} МиБ — не скачиваю; пришли его, пожалуйста, ссылкой.",
                        app.cfg.attach_max_mb
                    ),
                )
                .await;
            continue;
        }
        let base = telegram::safe_name(file.file_name.as_deref(), &format!("file-{}", msg.id));
        let local = unique_path(&app.cfg.attach_dir, msg.id, &base);
        match app.bot.download(file, &local).await {
            Ok(size) => {
                let dims = match (file.width, file.height) {
                    (Some(w), Some(h)) => format!(", {w}×{h}"),
                    _ => String::new(),
                };
                log::info!("chat {chat_id}: {kind} {base} ({size} байт{dims}) → {}", local.display());
                saved.push(telegram::Attachment { local, name: base, kind, size });
            }
            Err(e) => {
                log::warn!("chat {chat_id}: не скачали {kind} {base}: {e:#}");
                app.bot
                    .send_text(chat_id, &format!("Не скачал {kind} «{name}»: {e:#}"))
                    .await;
            }
        }
    }
    saved
}

/// Имя файла в каталоге вложений: добавляем id сообщения, чтобы не затирать
/// прежние загрузки, и счётчик, если такое имя уже занято.
fn unique_path(dir: &std::path::Path, message_id: i64, name: &str) -> PathBuf {
    let first = dir.join(format!("{message_id}-{name}"));
    if !first.exists() {
        return first;
    }
    let (stem, ext) = match name.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() => (s.to_string(), format!(".{e}")),
        _ => (name.to_string(), String::new()),
    };
    let mut n = 2;
    loop {
        let candidate = dir.join(format!("{message_id}-{stem}-{n}{ext}"));
        if !candidate.exists() {
            return candidate;
        }
        n += 1;
    }
}

/// Размер по-человечески: байты, КиБ, МиБ.
fn human_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    if bytes < KB {
        format!("{bytes} байт")
    } else if bytes < 1024 * KB {
        format!("{:.1} КиБ", bytes as f64 / KB as f64)
    } else {
        format!("{:.1} МиБ", bytes as f64 / (1024.0 * KB as f64))
    }
}

/// Принять текстовое сообщение: одинаковые подряд схлопываем в одно, остальные
/// копим окном агрегации — человек печатает задачу частями, запускаем одной
/// задачей после тишины (но не дольше предельного окна от первой части).
async fn submit_text(app: &Arc<App>, chat_id: i64, text: String) {
    if app.last_text.lock().await.get(&chat_id).map(String::as_str) == Some(text.as_str()) {
        // Тот же текст второй раз подряд — случайная повторная отправка.
        // Отвечать отдельным сообщением не будем: это лишний шум в чате.
        log::info!("chat {chat_id}: тот же текст подряд — не запускаем второй раз");
        return;
    }
    app.last_text.lock().await.insert(chat_id, text.clone());

    let quiet = app.cfg.agg_quiet_secs;
    let cap = app.cfg.agg_window_secs;
    if quiet == 0 || cap == 0 {
        run_agent(app.clone(), chat_id, text).await;
        return;
    }

    let (gen, first) = {
        let mut batches = app.batches.lock().await;
        let batch = batches.entry(chat_id).or_insert_with(|| Batch {
            texts: Vec::new(),
            first_at: tokio::time::Instant::now(),
            gen: 0,
        });
        batch.texts.push(text);
        batch.gen += 1;
        (batch.gen, batch.texts.len() == 1)
    };
    let delay = {
        let batches = app.batches.lock().await;
        let b = batches.get(&chat_id).expect("окно только что создано");
        agg_deadline(tokio::time::Instant::now(), b.first_at, quiet, cap)
            .saturating_duration_since(tokio::time::Instant::now())
    };
    if first {
        // Не отдельное сообщение, а строка в пузыре статуса: её видно, пока бот
        // ждёт дополнений, и в чате она не остаётся.
        app.status(chat_id, &format!("Задача принята, жду дополнения ещё {quiet} с"))
            .await;
    }
    let app = app.clone();
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let Some(texts) = take_batch(&app.batches, chat_id, gen).await else { return };
        if texts.is_empty() {
            return;
        }
        run_agent(app, chat_id, texts.join("\n\n")).await;
    });
}

/// Часы окна: запуск через `quiet` после последней части задачи, но от первой —
/// не позже `cap`. Если предельный срок уже прошёл, запуск возможен сразу.
fn agg_deadline(
    now: tokio::time::Instant,
    first_at: tokio::time::Instant,
    quiet: u64,
    cap: u64,
) -> tokio::time::Instant {
    (now + Duration::from_secs(quiet)).min(first_at + Duration::from_secs(cap))
}

/// Забрать накопленное окно, если оно не обновилось (другое поколение) и не
/// отменено (/cancel снял запись). Иначе — тихо выходим: таймер устарел.
async fn take_batch(
    batches: &Mutex<HashMap<i64, Batch>>,
    chat_id: i64,
    gen: u64,
) -> Option<Vec<String>> {
    let mut batches = batches.lock().await;
    match batches.get_mut(&chat_id) {
        Some(b) if b.gen == gen => {
            let texts = std::mem::take(&mut b.texts);
            batches.remove(&chat_id);
            Some(texts)
        }
        _ => None,
    }
}

async fn handle_command(app: &Arc<App>, chat_id: i64, cmd: &str, arg: &str) {
    match cmd {
        "/start" | "/help" => {
            let (q, c) = (app.cfg.agg_quiet_secs, app.cfg.agg_window_secs);
            let agg = if q == 0 || c == 0 {
                String::new()
            } else if q >= c {
                format!(
                    "Несколько сообщений подряд складываю в одну задачу ({c} с максимум).\n\
                     Повтор того же текста подряд не дублирую.\n\n"
                )
            } else {
                format!(
                    "Несколько сообщений подряд складываю в одну задачу: жду {q} с тишины, \
                     максимум {c} с. Повтор того же текста подряд не дублирую.\n\n"
                )
            };
            let attach = app.cfg.attach_dir.display().to_string();
            app.bot
                .send_text(
                    chat_id,
                    &format!(
                        "Пиши задачу — я передам её агенту и пришлю ответ.\n\n{agg}\
                         Файлы из чата скачиваю в {attach} — агент читает их оттуда.\n\
                         Сообщения в чат склеиваю: несколько коротких за один шаг приходят \
                         одним сообщением, повторы и «выполняю команду» в чат не уходят. \
                         Пока агент работает, в чате одна живая строка о том, что он делает.\n\n\
                         /new — начать новую сессию\n\
                         /backend [claude|opencode] — показать или сменить агента\n\
                         /cancel — прервать выполнение, очистить очередь и автопродолжение\n\
                         /resume — когда продолжу сам после лимита\n\
                         /cwd [путь] — показать/сменить рабочую директорию (сбрасывает сессию)\n\
                         /status — текущая сессия и директория\n\n\
                         Если у claude кончится лимит, я передам контекст opencode и буду работать с ним, \
                         а когда лимит claude восстановится — сам вернусь в claude-сессию."
                    ),
                )
                .await;
        }
        "/backend" => {
            if arg.is_empty() {
                let backend = app.backend_of(chat_id).await;
                let other = match backend {
                    Backend::Claude => "opencode",
                    Backend::OpenCode => "claude",
                };
                app.bot
                    .send_text(
                        chat_id,
                        &format!(
                            "🤖 Сейчас агент: {}. Переключить: /backend {other}",
                            backend.name()
                        ),
                    )
                    .await;
            } else {
                match arg.parse::<Backend>() {
                    Ok(backend) => {
                        let was_fallback = app.chat_state(chat_id).await.fallback_from.is_some();
                        app.update_chat(chat_id, |s| {
                            s.backend = Some(backend);
                            if was_fallback {
                                // Ручной выбор сильнее автоматического замещения.
                                s.fallback_from = None;
                                s.resume_at = None;
                                s.resume_label = None;
                                s.handoff = None;
                                s.gap_note = None;
                            }
                        })
                        .await;
                        let state = app.chat_state(chat_id).await;
                        let note = match state.session_for(backend) {
                            Some(id) => format!(
                                "Сессия {} продолжится: {}…",
                                backend.name(),
                                short_session(id)
                            ),
                            None => format!(
                                "Новое сообщение начнёт новую сессию {}.",
                                backend.name()
                            ),
                        };
                        let cancelled = if was_fallback { " Автовозврат по лимиту отменён." } else { "" };
                        app.bot
                            .send_text(chat_id, &format!("🤖 Переключил на {}. {note}{cancelled}", backend.name()))
                            .await;
                    }
                    Err(e) => app.bot.send_text(chat_id, &e).await,
                }
            }
        }
        "/resume" => {
            let state = app.chat_state(chat_id).await;
            match (&state.resume_label, state.fallback_from) {
                (Some(label), Some(main)) => {
                    let now = app.backend_of(chat_id).await;
                    app.bot
                        .send_text(
                            chat_id,
                            &format!(
                                "⏳ Лимит {main} исчерпан: смену ведёт {now}, вернусь к {main} в {label}. /cancel — отменить.",
                            ),
                        )
                        .await
                }
                (Some(label), None) => {
                    app.bot
                        .send_text(chat_id, &format!("⏳ Продолжу сам: {label}. /cancel — отменить."))
                        .await
                }
                (None, _) => app.bot.send_text(chat_id, "Автопродолжение не запланировано.").await,
            }
        }
        "/new" => {
            app.update_chat(chat_id, |s| s.reset_sessions()).await;
            app.bot.send_text(chat_id, "🆕 Следующее сообщение начнёт новую сессию.").await;
        }
        "/cancel" => {
            let had_resume = app.chat_state(chat_id).await.resume_at.is_some();
            if had_resume {
                app.update_chat(chat_id, |s| {
                    s.resume_at = None;
                    s.resume_label = None;
                })
                .await;
                app.bot.send_text(chat_id, "⏹ Автопродолжение отменено.").await;
            }
            let dropped = match app.running.lock().await.get_mut(&chat_id) {
                Some(run) => {
                    run.cancel.notify_one();
                    Some(std::mem::take(&mut run.queue).len())
                }
                None => None,
            };
            match dropped {
                Some(n) if n > 0 => {
                    app.bot.send_text(chat_id, &format!("🗑 Очередь очищена ({n} сообщ.).")).await
                }
                Some(_) => {}
                None if !had_resume => {
                    app.bot.send_text(chat_id, "Сейчас ничего не выполняется.").await
                }
                None => {}
            }
            let window = app.batches.lock().await.remove(&chat_id).map(|b| b.texts.len());
            if let Some(n) = window.filter(|n| *n > 0) {
                app.bot
                    .send_text(
                        chat_id,
                        &format!("🗑 Незапущенное окно агрегации сброшено ({n} сообщ.)."),
                    )
                    .await;
            }
        }
        "/cwd" => {
            if arg.is_empty() {
                let dir = work_dir(app, chat_id).await;
                app.bot.send_text(chat_id, &format!("📁 {}", dir.display())).await;
                return;
            }
            let path = PathBuf::from(shellexpand_home(arg));
            match tokio::fs::canonicalize(&path).await {
                Ok(p) if p.is_dir() => {
                    let shown = p.display().to_string();
                    app.update_chat(chat_id, |s| {
                        s.work_dir = Some(p);
                        s.reset_sessions();
                    })
                    .await;
                    app.bot
                        .send_text(chat_id, &format!("📁 Рабочая директория: {shown}\nСессия сброшена."))
                        .await;
                }
                _ => app.bot.send_text(chat_id, &format!("Нет такой директории: {arg}")).await,
            }
        }
        "/status" => {
            let state = app.chat_state(chat_id).await;
            let backend = state.backend.unwrap_or(app.cfg.backend);
            let queued = app.running.lock().await.get(&chat_id).map(|r| r.queue.len());
            // Если записи о задаче нет, сессию всё равно может вести процесс от
            // прежнего запуска бота — проверяем это, чтобы не врать про «нет».
            let adopted = if queued.is_none() {
                adopted_agent_pid(app, chat_id).await
            } else {
                None
            };
            let window = app
                .batches
                .lock()
                .await
                .get(&chat_id)
                .map_or(0, |b| b.texts.len());
            let window_line = if window == 0 {
                String::new()
            } else {
                format!("\nОжидает окна агрегации: {window} сообщ.")
            };
            let fallback_line = match (state.fallback_from, state.resume_label.as_deref()) {
                (Some(main), Some(label)) => format!(
                    "\nЗамещение: {main} дожидается лимита, смену ведёт {}, автовозврат в {label}",
                    backend.name()
                ),
                _ => String::new(),
            };
            let text = format!(
                "Агент: {}\nСессия ({}): {}\nДиректория: {}\nВыполняется: {}\nАвтопродолжение: {}{window_line}{fallback_line}",
                backend.name(),
                backend.name(),
                state
                    .session_for(backend)
                    .map(String::as_str)
                    .unwrap_or("нет"),
                work_dir(app, chat_id).await.display(),
                match queued {
                    Some(0) => "да".to_string(),
                    Some(n) => format!("да, в очереди {n}"),
                    None => match adopted {
                        Some(pid) => format!("да, смена от прошлого запуска бота (процесс {pid})"),
                        None => "нет".to_string(),
                    },
                },
                state.resume_label.as_deref().unwrap_or("нет"),
            );
            app.bot.send_text(chat_id, &text).await;
        }
        _ => {
            // Unknown commands (e.g. /compact) are passed to the agent as-is.
            let text = if arg.is_empty() { cmd.to_string() } else { format!("{cmd} {arg}") };
            run_agent(app.clone(), chat_id, text).await;
        }
    }
}

async fn work_dir(app: &App, chat_id: i64) -> PathBuf {
    app.chat_state(chat_id).await.work_dir.unwrap_or_else(|| app.cfg.work_dir.clone())
}

fn shellexpand_home(path: &str) -> String {
    match (path.strip_prefix('~'), std::env::var("HOME")) {
        (Some(rest), Ok(home)) => format!("{home}{rest}"),
        _ => path.to_string(),
    }
}

/// Короткий префикс id сессии для сообщений человеку.
fn short_session(id: &str) -> String {
    id.chars().take(18).collect()
}

/// Продолжение, которое бот сам ставит в очередь после перехода на замещение:
/// смена opencode начинается сразу, без ожидания сообщения пользователя.
const SHIFT_PROMPT: &str = "Продолжай работу из переданного выше контекста: \
подведи итог — что уже сделано и что осталось, — и приступай к следующему шагу. \
Если для продолжения нужен ответ пользователя, задай ему один короткий вопрос.";

/// PID процесса агента, который ведёт сессию этого чата, но был запущен не нами
/// (бот перезапустился, а смена осталась жива благодаря KillMode=process).
async fn adopted_agent_pid(app: &Arc<App>, chat_id: i64) -> Option<u32> {
    let state = app.chat_state(chat_id).await;
    let backend = state.backend.unwrap_or(app.cfg.backend);
    let session = state.session_for(backend).cloned()?;
    tokio::task::spawn_blocking(move || {
        adopt::find_session_pid(&adopt::proc_cmdlines(), &session, std::process::id())
    })
    .await
    .ok()
    .flatten()
}

/// Ждём, пока прежняя смена закончится. `cap` — предел ожидания: заснувшего
/// процесса ждать вечно нельзя, иначе чат встанет навсегда.
async fn wait_adopted_agent(app: &Arc<App>, chat_id: i64, cap: Duration) -> bool {
    let started = std::time::Instant::now();
    loop {
        if adopted_agent_pid(app, chat_id).await.is_none() {
            return true;
        }
        if started.elapsed() >= cap {
            return false;
        }
        app.bot.send_typing(chat_id).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Starts an agent turn on the chat's backend, or queues the prompt if one is
/// already running for this chat. Queued messages are sent together as one
/// turn after the current one finishes.
async fn run_agent(app: Arc<App>, chat_id: i64, prompt: String) {
    let mut cancel = Arc::new(Notify::new());
    {
        let mut running = app.running.lock().await;
        if let Some(run) = running.get_mut(&chat_id) {
            run.queue.push(prompt);
            let n = run.queue.len();
            drop(running);
            app.status(chat_id, &format!("Сообщение в очереди ({n}): выполню после текущей задачи"))
                .await;
            return;
        }
        running.insert(chat_id, Running { cancel: cancel.clone(), queue: Vec::new() });
    }

    // Смена могла достаться от прежнего запуска бота: не поднимаем второй
    // процесс поверх работающего и не теряем сообщение — сначала дожидаемся.
    if adopted_agent_pid(&app, chat_id).await.is_some() {
        app.status(chat_id, "Эту сессию ведёт процесс с прошлого запуска бота — жду его конца").await;
        if !wait_adopted_agent(&app, chat_id, Duration::from_secs(app.cfg.shift_wait_secs)).await {
            log::warn!("chat {chat_id}: прежняя смена не закончилась за 15 минут, начинаем новую");
        }
    }

    let mut prompt = prompt;
    loop {
        let shifted = run_turn(&app, chat_id, prompt, cancel).await;

        let mut running = app.running.lock().await;
        let Some(run) = running.get_mut(&chat_id) else { return };
        if shifted {
            run.queue.push(SHIFT_PROMPT.to_string());
        }
        if run.queue.is_empty() {
            running.remove(&chat_id);
            drop(running);
            // Задача кончилась: пузырь статуса убираем из чата, он больше не нужен.
            app.status_finish(chat_id).await;
            return;
        }
        let queued = std::mem::take(&mut run.queue);
        cancel = Arc::new(Notify::new());
        run.cancel = cancel.clone();
        let lines = queued
            .iter()
            .map(|t| format!("· {}", brief(t, 120)))
            .collect::<Vec<_>>()
            .join("\n");
        drop(running);
        app.status(chat_id, &format!("Беру следующую задачу из очереди:\n{lines}"))
            .await;
        prompt = queued.join("\n\n");
    }
}

/// Раз в секунду смотрит исходящую очередь: чат замолчал на `quiet` секунд,
/// или накопилось `max_items` сообщений, или прошло `max_secs` от первого —
/// отправляем накопленное одним сообщением.
async fn flush_outbox(app: Arc<App>) {
    let policy = Policy {
        quiet: Duration::from_secs(app.cfg.send_quiet_secs),
        max: Duration::from_secs(app.cfg.send_max_secs),
        max_items: app.cfg.send_max_items,
    };
    let mut ticker = tokio::time::interval(Duration::from_millis(700));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut waiting = false;
    loop {
        ticker.tick().await;
        // Telegram ответил «ждите N секунд» — слать до конца срока нельзя,
        // каждая попытка лишь удлиняет запрет (на живом лимите бот так и делал:
        // тысячи повторов за вечер и очередь не уходила никогда).
        if app.bot.send_paused() {
            if !waiting {
                waiting = true;
                log::info!(
                    "пауза от Telegram ещё {} с — отправка очереди стоит",
                    (app.bot.paused_until() - outbox::now_ms()) / 1000
                );
            }
            continue;
        }
        if waiting {
            waiting = false;
            log::info!("пауза Telegram закончилась, очередь снова идёт");
        }
        let items = outbox::pending(&app.cfg.outbox_dir);
        if items.is_empty() {
            continue;
        }
        for chat in outbox::due_chats(&items, outbox::now_ms(), &policy) {
            app.drain_outbox(chat).await;
        }
    }
}

/// `claudetg clean [--chat <id>] [--all] [--keep N] [--dry]` — убирает из чата
/// свои же сообщения по журналу отправленного: служебные заметки и болтовню
/// (по умолчанию) или всё подряд (--all). Последние N сообщений (--keep, по
/// умолчанию 5) не трогаем. Чужие сообщения бот не удаляет: в личной переписке
/// Telegram этого не разрешает.
async fn clean_messages(bot: &Bot, cfg: &Config, args: &[String]) -> Result<()> {
    let usage = "usage: claudetg clean [--chat <id>] [--all] [--keep N] [--dry]";
    // чат: явный --chat, иначе переменная окружения, которую бот даёт агенту
    let mut chat = args.iter().position(|a| a == "--chat").and_then(|i| args.get(i + 1).cloned());
    if chat.is_none() {
        chat = std::env::var("CLAUDETG_CHAT_ID").ok();
    }
    let chat_id: i64 = chat
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("{usage}: чат не задан (--chat <id> или CLAUDETG_CHAT_ID)"))?;
    let all = args.iter().any(|a| a == "--all");
    let dry = args.iter().any(|a| a == "--dry");
    let keep: usize = args
        .iter()
        .position(|a| a == "--keep")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    let records = outbox::read_sent(&cfg.sent_log);
    let mine: Vec<_> = records.iter().filter(|r| r.chat == chat_id).cloned().collect();
    let targets = outbox::clean_targets(&mine, keep, !all);
    println!(
        "в журнале {} из {} своих сообщений в чат {chat_id}{}",
        targets.len(),
        mine.len(),
        if dry { " — пробный прогон, ничего не удаляем" } else { "" }
    );
    if targets.is_empty() {
        println!(
            "чистить нечего: журнал {} пуст или все сообщения помечены как последние",
            cfg.sent_log.display()
        );
        return Ok(());
    }
    let mut deleted = 0usize;
    let mut handled = Vec::new();
    for r in &targets {
        if dry {
            println!("удалим: сообщение {} — {}", r.message_id, r.preview.replace('\n', " "));
            continue;
        }
        if bot.delete(chat_id, r.message_id).await {
            deleted += 1;
        }
        // Сообщение мог удалить человек или Telegram (слишком старое) — запись
        // из журнала убираем в любом случае, иначе она повиснет навсегда.
        handled.push(r.message_id);
    }
    if !dry {
        let removed: std::collections::HashSet<i64> = handled.into_iter().collect();
        let rest: Vec<_> = records
            .iter()
            .filter(|r| !(r.chat == chat_id && removed.contains(&r.message_id)))
            .cloned()
            .collect();
        outbox::write_sent(&cfg.sent_log, &rest)?;
        println!("удалено {deleted} из {}; в журнале осталось {} записей", targets.len(), rest.len());
    }
    Ok(())
}

async fn run_turn(app: &Arc<App>, chat_id: i64, prompt: String, cancel: Arc<Notify>) -> bool {
    let state = app.chat_state(chat_id).await;
    let backend = state.backend.unwrap_or(app.cfg.backend);
    let in_fallback = state.fallback_from.is_some();
    let dir = work_dir(app, chat_id).await;
    let session_id = state.session_for(backend).cloned();

    // Для журнала обменов — исходная задача без служебных вставок.
    let logged_user = trunc(&prompt, 300);

    // При замещении первое сообщение несёт контекст предшественника:
    // opencode получает расшифровку claude-сессии, claude — заметку о смене.
    let prompt = match (backend, state.handoff.clone(), state.gap_note.clone()) {
        (Backend::OpenCode, Some(context), _) if !context.is_empty() => {
            app.update_chat(chat_id, |s| s.handoff = None).await;
            format!("{}\n\n---\n{context}\n\n---\n{prompt}", handoff::HANDOFF_HEADER)
        }
        (Backend::Claude, _, Some(note)) => {
            app.update_chat(chat_id, |s| s.gap_note = None).await;
            format!("{note}\n\n---\n{prompt}")
        }
        _ => prompt,
    };
    let (tx, mut rx) = mpsc::unbounded_channel();

    let run = {
        let app = app.clone();
        tokio::spawn(async move {
            backend::run(backend, &app.cfg, &dir, chat_id, session_id.as_deref(), &prompt, tx, cancel).await
        })
    };

    // Пока агент работает: держим индикатор «печатает…» и одну живую строку о том,
    // что он делает сейчас. Отдельных сообщений про каждый инструмент не будет.
    let mut ticker = tokio::time::interval(Duration::from_secs(4));
    loop {
        tokio::select! {
            ev = rx.recv() => match ev {
                Some(Event::Progress(p)) => app.status_progress(chat_id, &p).await,
                None => break,
            },
            _ = ticker.tick() => {
                app.status(chat_id, "").await;
                app.bot.send_typing(chat_id).await;
            }
        }
    }

    match run.await {
        Ok(Ok(outcome)) => {
            if let Some(id) = outcome.session_id.clone() {
                app.update_chat(chat_id, |s| s.set_session(backend, id)).await;
            }
            let mut text = outcome.text;
            if outcome.is_error && !text.starts_with('⛔') {
                text = format!("⚠️ {text}");
            }
            if let Some(cost) = outcome.cost_usd {
                log::info!("chat {chat_id}: {backend_name} turn finished, cost ${cost:.4}", backend_name = backend.name());
            }
            // Ответ идёт через очередь: всё, что агент написал по пути через
            // `claudetg send`, склеится с ним в одно сообщение.
            app.queue(chat_id, Kind::Answer, &text).await;
            app.drain_outbox(chat_id).await;
            record_turn(app, chat_id, logged_user, trunc(&text, 400), in_fallback).await;
            return schedule_resume_if_limited(app, chat_id, &text).await;
        }
        // Ошибки смены тоже через очередь: если связи нет в этот момент,
        // сообщение не потеряется, а уйдёт, когда связь вернётся.
        Ok(Err(e)) => {
            app.queue(chat_id, Kind::Answer, &format!("⚠️ Ошибка запуска {}: {e:#}", backend.name()))
                .await;
            app.drain_outbox(chat_id).await;
        }
        Err(e) => {
            app.queue(chat_id, Kind::Answer, &format!("⚠️ Внутренняя ошибка: {e}")).await;
            app.drain_outbox(chat_id).await;
        }
    }
    false
}

/// Обмен записывается в журнал чата — из него делаются заметки при замещении.
async fn record_turn(app: &Arc<App>, chat_id: i64, user: String, agent: String, during_fallback: bool) {
    app.update_chat(chat_id, |s| {
        s.recent.push(Turn { user, agent, during_fallback });
        if s.recent.len() > handoff::RECENT_TURNS {
            s.recent.remove(0);
        }
    })
    .await;
}

fn trunc(s: &str, n: usize) -> String {
    match s.chars().take(n).collect::<String>() {
        t if t.chars().count() < s.chars().count() => format!("{t}…"),
        t => t,
    }
}

/// Начало задачи одной строкой: переносы сворачиваем, длинное обрезаем.
fn brief(text: &str, n: usize) -> String {
    trunc(&text.split_whitespace().collect::<Vec<_>>().join(" "), n)
}

/// Агент упёрся в лимит — запланировать продолжение.
///
/// Если лимит у claude и замещение включено: сразу передать контекст opencode,
/// переключить чат на него и вернуться в claude-сессию при сбросе лимита.
/// Иначе (замещение выключено или контекст не собрать) — прежнее поведение:
/// дождаться сброса и отправить «Продолжай» в ту же сессию.
/// Возвращает true, если чат переключён на замещение — run_agent поставит в
/// очередь продолжение, чтобы смена началась сразу.
async fn schedule_resume_if_limited(app: &Arc<App>, chat_id: i64, text: &str) -> bool {
    let Some((at, label)) = limit::resume_at(text, chrono::Utc::now()) else { return false };
    let shown = label.clone();
    let state = app.chat_state(chat_id).await;
    let backend = state.backend.unwrap_or(app.cfg.backend);

    let handoff_text = if backend == Backend::Claude && app.cfg.limit_fallback {
        let context = state
            .session_id
            .as_deref()
            .and_then(|sid| {
                handoff::claude_transcript(&app.cfg.claude_projects_dir, sid, handoff::MAX_TRANSCRIPT_CHARS)
            })
            // Файл сессии не найден (например, claude работает на другой машине) —
            // берём то, что помнит сам бот.
            .unwrap_or_else(|| handoff::render_turns(&state.recent, handoff::MAX_TRANSCRIPT_CHARS));
        (!context.is_empty()).then_some(context)
    } else {
        None
    };

    let switched = handoff_text.is_some();
    app.update_chat(chat_id, |s| {
        s.resume_at = Some(at.timestamp());
        s.resume_label = Some(label);
        if let Some(context) = handoff_text {
            s.handoff = Some(context);
            s.fallback_from = Some(Backend::Claude);
            s.backend = Some(Backend::OpenCode);
        }
    })
    .await;
    log::info!(
        "chat {chat_id}: claude limit hit at {at}, fallback={switched}, resume scheduled"
    );
    if switched {
        app.bot
            .send_text(
                chat_id,
                &format!(
                    "⏳ У claude исчерпан лимит. Передаю контекст opencode и продолжаю с ним; \
                     в {shown} сам вернусь в claude-сессию. /cancel — отменить автовозврат, \
                     /backend claude — вернуться вручную.",
                ),
            )
            .await;
    } else {
        app.bot
            .send_text(
                chat_id,
                &format!("⏳ Лимит исчерпан. Продолжу сам в {shown} — отправлю «Продолжай». /cancel — отменить."),
            )
            .await;
    }
    switched
}

/// Раз в 20 секунд: чаты, которым пора продолжить после лимита.
///
/// Время хранится в файле состояния, поэтому перезапуск бота расписание не
/// теряет: если момент прошёл, пока бот лежал, продолжение уйдёт сразу.
async fn resume_scheduler(app: Arc<App>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(20));
    loop {
        ticker.tick().await;
        let mut tasks = resume_due(app.clone()).await;
        while tasks.join_next().await.is_some() {}
    }
}

/// One pass over the chats whose resume moment has come: tell the person,
/// clear the schedule, start the continuation. Returns the spawned turns so
/// tests can wait for them; the scheduler joins them itself.
async fn resume_due(app: Arc<App>) -> tokio::task::JoinSet<()> {
    let now = chrono::Utc::now().timestamp();
    let due: Vec<i64> = app
        .chats
        .lock()
        .await
        .iter()
        .filter(|(_, s)| s.resume_at.is_some_and(|at| at <= now))
        .map(|(id, _)| *id)
        .collect();
    resume_chats(app, due).await
}

/// Продолжения по расписанию (выделено из тикера, чтобы проход можно было
/// проверить целиком, без ожидания 20-секундного интервала).
async fn resume_chats(app: Arc<App>, due: Vec<i64>) -> tokio::task::JoinSet<()> {
    let mut tasks = tokio::task::JoinSet::new();
    for chat_id in due {
        let state = app.chat_state(chat_id).await;
        let returning = state.fallback_from;
        if let Some(main_backend) = returning {
            // Смена закончилась: claude снова доступен, собираем заметку о работе opencode.
            let gap: Vec<Turn> = state.recent.iter().filter(|t| t.during_fallback).cloned().collect();
            let note = if gap.is_empty() { None } else { Some(handoff::gap_note(&gap)) };
            app.update_chat(chat_id, |s| {
                s.fallback_from = None;
                s.backend = Some(main_backend);
                s.gap_note = note;
            })
            .await;
            log::info!("chat {chat_id}: claude limit reset, returning from fallback");
            app.bot
                .send_text(
                    chat_id,
                    &format!(
                        "⏰ Лимит {} обновился — возвращаюсь к нему (сессия та же), \
                         с заметкой о смене opencode.",
                        main_backend.name()
                    ),
                )
                .await;
        } else {
            app.bot.send_text(chat_id, "⏰ Лимит обновился — продолжаю.").await;
        }
        app.update_chat(chat_id, |s| {
            s.resume_at = None;
            s.resume_label = None;
        })
        .await;
        let app = app.clone();
        tasks.spawn(async move { run_agent(app, chat_id, "Продолжай".to_string()).await });
    }
    tasks
}

#[cfg(test)]
mod tests {
    use super::*;

    // ------- Сквозные проверки: чат -> агент -> чат на подставном Telegram -------
    //
    // Подставы вместо настоящего Telegram и настоящего Claude: видно каждое
    // обращение в чат, промпт, ушедший агенту, и состояние очереди. Так
    // проверяется вся цепочка целиком, а не только отдельные функции.

    use crate::testkit::{agent_prompt, fake_agent, test_config, update_photo, update_text, FakeTg};

    /// Пара «подставной Telegram + приложение» для сквозных тестов.
    /// Настройки правятся до Arc(App): тот же тег — тот же каталог и тот же
    /// файл состояния, поэтому «перезапуск» бота проверяется новым вызовом.
    fn e2e_with(tag: &str, tweak: impl FnOnce(&mut Config)) -> (Arc<App>, FakeTg) {
        logging_on();
        let tg = FakeTg::start();
        let mut cfg = test_config(tag, tg.base());
        tweak(&mut cfg);
        // состояние чата читаем из файла: так проверяется «перезапуск» бота
        let chats = App::load_state(&cfg);
        let app = App {
            cfg,
            bot: tg.bot(),
            chats: Mutex::new(chats),
            running: Mutex::new(HashMap::new()),
            batches: Mutex::new(HashMap::new()),
            last_text: Mutex::new(HashMap::new()),
            status: Mutex::new(HashMap::new()),
            recent_out: Mutex::new(HashMap::new()),
        };
        (Arc::new(app), tg)
    }

    fn e2e(tag: &str) -> (Arc<App>, FakeTg) {
        e2e_with(tag, |_| {})
    }

    /// «Перезапуск» бота: тот же каталог (очередь, журнал, состояние чатов),
    /// но настройки можно поправить — например, подменить бинарник агента.
    fn e2e_reopen(tag: &str, tweak: impl FnOnce(&mut Config)) -> (Arc<App>, FakeTg) {
        let tg = FakeTg::start();
        let mut cfg = crate::testkit::test_config_fresh(tag, tg.base(), false);
        tweak(&mut cfg);
        let chats = App::load_state(&cfg);
        let recent = outbox::seed_recent(&outbox::read_sent(&cfg.sent_log), RECENT_SENDS);
        let app = App {
            cfg,
            bot: tg.bot(),
            chats: Mutex::new(chats),
            running: Mutex::new(HashMap::new()),
            batches: Mutex::new(HashMap::new()),
            last_text: Mutex::new(HashMap::new()),
            status: Mutex::new(HashMap::new()),
            recent_out: Mutex::new(recent),
        };
        (Arc::new(app), tg)
    }

    /// То же, но агент сразу подменён скриптом (бинарники настраиваем до Arc).
    fn e2e_agent(tag: &str, agent: &std::path::Path) -> (Arc<App>, FakeTg) {
        e2e_with(tag, |cfg| {
            cfg.claude_bin = agent.display().to_string();
            cfg.opencode_bin = agent.display().to_string();
        })
    }

    /// JSON сообщения из обновления подставы — чтобы разбирать его тем же
    /// парсером, что и настоящие ответы getUpdates.
    fn message_of(update_json: &str) -> Message {
        let v: serde_json::Value = serde_json::from_str(update_json).expect("обновление");
        serde_json::from_value(v["result"][0]["message"].clone()).expect("сообщение")
    }

    #[tokio::test]
    async fn full_roundtrip_chat_agent_chat() {
        let agent = fake_agent(
            "roundtrip",
            &[
                r#"{"type":"assistant","session_id":"S-42","message":{"content":[{"type":"tool_use","name":"Read","input":{"file_path":"/tmp/отчёт.md"}}]}}"#,
                r#"{"type":"result","result":"Готово: разобрался."}"#,
            ],
            0, false,
        );
        let (app, tg) = e2e_agent("roundtrip", &agent);
        run_turn(&app, 11, "разберись".into(), Arc::new(Notify::new())).await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1, "в чат ушло одно сообщение: {sent:?}");
        assert!(sent[0].contains("Готово: разобрался"));
        // сессия агента сохранена — следующий ход продолжит её же
        assert_eq!(app.chat_state(11).await.session_id.as_deref(), Some("S-42"));
        // расшифровка хода попала в журнал чата — из неё делается заметка при смене
        assert_eq!(app.chat_state(11).await.recent.len(), 1);
        assert!(outbox::pending(&app.cfg.outbox_dir).is_empty(), "очередь пуста после отправки");
        assert!(std::fs::read_to_string(&app.cfg.sent_log).unwrap().contains("Готово"));
        // состояние чата переживает перезапуск бота
        app.save_state().await;
        assert!(
            std::fs::read_to_string(&app.cfg.state_file).unwrap().contains("S-42"),
            "сессия записана в файл состояния"
        );
    }

    #[tokio::test]
    async fn unauthorized_chat_is_ignored_entirely() {
        let (app, tg) = e2e("unauth");
        handle_message(app.clone(), message_of(&update_text(1, 11, 999, "сделай дело"))).await;
        assert!(tg.requests().is_empty(), "в чат не ушло ничего");
        assert!(outbox::pending(&app.cfg.outbox_dir).is_empty());
    }

    #[tokio::test]
    async fn empty_message_asks_for_text_or_file() {
        let (app, tg) = e2e("empty");
        handle_message(app.clone(), message_of(&update_text(1, 11, 7, "   "))).await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1);
        assert!(sent[0].contains("только текст и файлы"));
    }

    #[tokio::test]
    async fn chatter_is_dropped_answers_merge_once() {
        let (app, tg) = e2e("merge");
        app.queue(11, Kind::Agent, "Выполняю команду: собрать проект").await;
        app.queue(11, Kind::Answer, "Первая часть ответа").await;
        app.queue(11, Kind::Answer, "Вторая часть ответа").await;
        app.drain_outbox(11).await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1, "несколько кусков уходят одним сообщением");
        assert!(!sent[0].contains("Выполняю команду"), "болтовня в чат не идёт");
        assert!(sent[0].contains("Первая часть ответа") && sent[0].contains("Вторая часть ответа"));
    }

    #[tokio::test]
    async fn expired_queue_items_never_reach_the_chat() {
        let (app, tg) = e2e("ttl");
        let path = outbox::enqueue(&app.cfg.outbox_dir, &Item::new(11, Kind::Answer, "старый ответ из прошлого вечера")).unwrap();
        // помечаем запись как часовую давность: лимит Telegram задержал очередь
        let raw = std::fs::read_to_string(&path).unwrap().replace("\"ts\":", "\"ts\": 1, \"_x\":");
        std::fs::write(path, raw).unwrap();
        app.drain_outbox(11).await;
        assert!(tg.calls("sendMessage").is_empty(), "просроченное в чат не уходит");
        assert!(outbox::pending(&app.cfg.outbox_dir).is_empty(), "просроченное снято с очереди");
        // свежий ответ уходит нормально
        run_turn(&app, 11, "дело".into(), Arc::new(Notify::new())).await;
        assert_eq!(tg.calls("sendMessage").len(), 1);
    }

    #[tokio::test]
    async fn restart_does_not_resend_old_answers() {
        let tg = FakeTg::start();
        let mut cfg = test_config("restart", tg.base());
        cfg.send_ttl_secs = 0; // срок годности отключён — очёредь пережила лимит целиком
        let item = Item::new(11, Kind::Answer, "Тот же вывод, что уже был в чате");
        outbox::append_sent(
            &cfg.sent_log,
            &[SentRecord { chat: 11, message_id: 1, ts: outbox::now_ms(), kind: Kind::Answer, preview: item.text.clone() }],
        )
        .unwrap();
        outbox::enqueue(&cfg.outbox_dir, &item).unwrap();
        // так же, как при реальном старте: память о повторах сеется из журнала
        let app = Arc::new(App {
            recent_out: Mutex::new(outbox::seed_recent(&outbox::read_sent(&cfg.sent_log), RECENT_SENDS)),
            cfg,
            bot: tg.bot(),
            chats: Mutex::new(HashMap::new()),
            running: Mutex::new(HashMap::new()),
            batches: Mutex::new(HashMap::new()),
            last_text: Mutex::new(HashMap::new()),
            status: Mutex::new(HashMap::new()),
        });
        app.drain_outbox(11).await;
        assert!(tg.calls("sendMessage").is_empty(), "перезапуск не гонит то, что уже отправлено");
        assert!(outbox::pending(&app.cfg.outbox_dir).is_empty());
    }

    #[tokio::test]
    async fn commands_answer_without_waking_the_agent() {
        let (app, tg) = e2e("cmds");
        for (cmd, needle) in [
            ("/start", "Пиши задачу"),
            ("/status", "Агент: claude"),
            ("/backend", "Сейчас агент: claude"),
            ("/backend opencode", "Переключил на opencode"),
            ("/new", "начнёт новую сессию"),
            ("/resume", "Автопродолжение не запланировано"),
            ("/cancel", "Сейчас ничего не выполняется"),
        ] {
            handle_command(&app, 11, cmd.split_whitespace().next().unwrap(), cmd.split_once(' ').map(|x| x.1).unwrap_or("")).await;
            assert!(tg.calls("sendMessage").last().unwrap().contains(needle), "{cmd}: {needle}");
        }
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 7, "по одному ответу на команду");
        assert!(sent[0].contains("Пиши задачу"));
        assert!(sent.iter().any(|s| s.contains("Переключил на opencode")));
        assert_eq!(app.backend_of(11).await, Backend::OpenCode);
        assert!(tg.calls("getMe").is_empty(), "команды не дёргают лишнего");
    }

    #[tokio::test]
    async fn unknown_command_goes_to_the_agent() {
        let agent = fake_agent("unknown", &[r#"{"type":"result","result":"Свернул контекст."}"#], 0, false);
        let (app, tg) = e2e_agent("unknown", &agent);
        handle_message(app.clone(), message_of(&update_text(1, 11, 7, "/compact"))).await;
        assert!(agent_prompt(&agent).contains("/compact"), "команда ушла агенту как есть");
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1);
        assert!(sent[0].contains("Свернул контекст"));
    }

    #[tokio::test]
    async fn same_text_twice_runs_the_agent_once() {
        let agent = fake_agent("same", &[r#"{"type":"result","result":"Проверю."}"#], 0, false);
        let (app, tg) = e2e_agent("same", &agent);
        handle_message(app.clone(), message_of(&update_text(1, 11, 7, "одно и то же"))).await;
        handle_message(app.clone(), message_of(&update_text(2, 11, 7, "одно и то же"))).await;
        assert_eq!(tg.calls("sendMessage").len(), 1, "повтор подряд не удваивает ответ");
    }

    #[tokio::test]
    async fn chat_files_download_and_go_to_the_agent_as_paths() {
        let agent = fake_agent("files", &[r#"{"type":"result","result":"Файл прочитал."}"#], 0, false);
        let (app, tg) = e2e_agent("files", &agent);
        handle_message(app.clone(), message_of(&update_photo(42, 11, 7, "разберись с фото"))).await;
        assert!(tg.calls("getFile").iter().any(|r| r.contains("PHOTO1")), "вложение скачано");
        let local = app.cfg.attach_dir.join("42-file-42");
        assert!(local.exists(), "файл лежит в каталоге вложений");
        let prompt = agent_prompt(&agent);
        assert!(prompt.contains(&local.display().to_string()), "агент получил локальный путь");
        assert!(prompt.contains("разберись с фото"), "подпись сообщения дошла до агента");
        assert!(tg.calls("sendMessage")[0].contains("Файл прочитал"));
    }

    #[tokio::test]
    async fn too_large_attachment_is_refused_with_a_hint() {
        let (app, tg) = e2e("bigfile");
        let update = r#"{"ok":true,"result":[{"update_id":1,"message":{"message_id":5,
            "chat":{"id":11},"from":{"id":7},
            "document":{"file_id":"BIG","file_name":"movie.mkv","file_size":31457280}}}]}"#;
        handle_message(app.clone(), message_of(update)).await;
        let sent = tg.calls("sendMessage");
        assert!(sent.iter().any(|s| s.contains("пришли его, пожалуйста, ссылкой")));
        assert!(tg.calls("getFile").is_empty(), "качать не стали");
    }

    #[tokio::test]
    async fn running_agent_can_be_cancelled() {
        let agent = fake_agent("cancel", &[r#"{"type":"result","result":"не успеет"}"#], 20, false);
        let (app, tg) = e2e_agent("cancel", &agent);
        let cancel = Arc::new(Notify::new());
        let running = run_turn(&app, 11, "долгая работа".into(), cancel.clone());
        tokio::time::sleep(Duration::from_millis(300)).await;
        // notify_one оставляет «разрешение»: даже если задача агента ещё не
        // дошла до notified(), отмена не потеряется
        cancel.notify_one();
        running.await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1);
        assert!(sent[0].contains("Выполнение прервано"), "чат видит итог отмены");
        assert!(tg.calls("getFile").is_empty());
    }

    #[tokio::test]
    async fn missing_agent_binary_is_reported_not_fatal() {
        let tg = FakeTg::start();
        let mut cfg = test_config("nobin", tg.base());
        cfg.claude_bin = "/nonexistent/claude-here".into();
        let app = Arc::new(App {
            bot: tg.bot(),
            chats: Mutex::new(HashMap::new()),
            running: Mutex::new(HashMap::new()),
            batches: Mutex::new(HashMap::new()),
            last_text: Mutex::new(HashMap::new()),
            status: Mutex::new(HashMap::new()),
            recent_out: Mutex::new(HashMap::new()),
            cfg,
        });
        run_turn(&app, 11, "дело".into(), Arc::new(Notify::new())).await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1);
        assert!(sent[0].contains("Ошибка запуска claude"));
    }

    #[tokio::test]
    async fn claude_limit_schedules_a_resume() {
        let agent = fake_agent(
            "limit",
            &[r#"{"type":"result","result":"\u26a0\ufe0f You've hit your 5-hour limit \u00b7 resets 9am (Europe/Moscow)"}"#],
            0, false,
        );
        let (app, tg) = e2e_agent("limit", &agent);
        run_turn(&app, 11, "дело".into(), Arc::new(Notify::new())).await;
        let state = app.chat_state(11).await;
        assert!(state.resume_at.is_some() && state.resume_label.is_some(), "продолжение запланировано");
        assert!(state.fallback_from.is_none(), "замещение выключено — ждём свой сброс");
        assert!(tg.calls("sendMessage").iter().any(|s| s.contains("/cancel — отменить")));
    }

    #[tokio::test]
    async fn claude_limit_hands_the_work_over_to_opencode() {
        let agent = fake_agent(
            "fallback",
            &[
                r#"{"type":"assistant","session_id":"sess-77","message":{"content":[{"type":"text","text":"работаю"}]}}"#,
                r#"{"type":"result","result":"\u26a0\ufe0f You've hit your 5-hour limit \u00b7 resets 9am (Europe/Moscow)"}"#,
            ],
            0, false,
        );
        let tg = FakeTg::start();
        let mut cfg = test_config("fallback", tg.base());
        cfg.limit_fallback = true;
        cfg.claude_bin = agent.display().to_string();
        cfg.opencode_bin = agent.display().to_string();
        let app = Arc::new(App {
            bot: tg.bot(),
            chats: Mutex::new(HashMap::new()),
            running: Mutex::new(HashMap::new()),
            batches: Mutex::new(HashMap::new()),
            last_text: Mutex::new(HashMap::new()),
            status: Mutex::new(HashMap::new()),
            recent_out: Mutex::new(HashMap::new()),
            cfg,
        });
        // расшифровка сессии claude — источник контекста для opencode
        std::fs::create_dir_all(&app.cfg.claude_projects_dir).unwrap();
        let transcript = [
            r#"{"type":"user","message":{"content":"сделай отчёт"}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"делаю"}]}}"#,
        ].join("\n");
        std::fs::write(app.cfg.claude_projects_dir.join("sess-77.jsonl"), transcript).unwrap();
        let switched = run_turn(&app, 11, "дело".into(), Arc::new(Notify::new())).await;
        assert!(switched, "чат переключён на замещение: sent={:?} prompt={:?}", tg.calls("sendMessage"), agent_prompt(&agent));
        let state = app.chat_state(11).await;
        assert_eq!(state.backend, Some(Backend::OpenCode));
        assert_eq!(state.fallback_from, Some(Backend::Claude));
        assert!(state.handoff.as_deref().unwrap().contains("сделай отчёт"), "контекст claude собран");
        let sent = tg.calls("sendMessage");
        assert!(sent.iter().any(|s| s.contains("Передаю контекст opencode")));
        assert!(sent.iter().any(|s| s.contains("5-hour limit")), "текст про лимит ушёл в чат");

        // следующий ход ведёт opencode и получает контекст предшественника в промпте
        let oc = fake_agent(
            "fallback-oc",
            &[
                r#"{"type":"text","sessionID":"oc-9","part":{"text":"Смена подхвачена"}}"#,
                r#"{"type":"step_finish","part":{"cost":0.02}}"#,
            ],
            0, false,
        );
        // следующий ход ведёт opencode: пересобираем то же приложение с новым бинарником
        let (app2, tg2) = e2e_reopen("fallback", |c| c.opencode_bin = oc.display().to_string());
        run_turn(&app2, 11, "продолжай".into(), Arc::new(Notify::new())).await;
        assert!(agent_prompt(&oc).contains(handoff::HANDOFF_HEADER), "opencode получил контекст claude");
        assert!(agent_prompt(&oc).contains("продолжай"));
        assert!(tg2.calls("sendMessage").iter().any(|s| s.contains("Смена подхвачена")));
    }

    #[tokio::test]
    async fn limit_reset_returns_to_claude_with_a_note() {
        let agent = fake_agent("back", &[r#"{"type":"result","result":"вернулся к claude"}"#], 0, false);
        let (app, tg) = e2e_agent("back", &agent);
        app.update_chat(11, |s| {
            s.backend = Some(Backend::OpenCode);
            s.fallback_from = Some(Backend::Claude);
            s.resume_at = Some(chrono::Utc::now().timestamp() - 5);
            s.resume_label = Some("09:00 Europe/Moscow".into());
            s.recent.push(Turn { user: "задача".into(), agent: "сделал часть".into(), during_fallback: true });
        })
        .await;
        let mut tasks = resume_chats(app.clone(), vec![11]).await;
        while tasks.join_next().await.is_some() {}
        let state = app.chat_state(11).await;
        assert_eq!(state.backend, Some(Backend::Claude));
        assert!(state.fallback_from.is_none() && state.resume_at.is_none());
        // заметка живёт до первого хода claude: он получает её в промпте и снимает
        assert!(agent_prompt(&agent).contains("СМЕНУ В ЭТОМ ЧАТЕ ВЁЛ ДРУГОЙ АГЕНТ"), "claude получил заметку о смене");
        assert!(agent_prompt(&agent).contains("сделал часть"));
        assert!(state.gap_note.is_none(), "заметка одноразовая");
        let sent = tg.calls("sendMessage");
        assert!(sent.iter().any(|s| s.contains("Лимит claude обновился")));
        assert!(sent.iter().any(|s| s.contains("вернулся к claude")));
    }

    #[tokio::test]
    async fn resume_scheduler_is_skipped_when_nothing_due() {
        let (app, tg) = e2e("sched");
        let mut tasks = resume_due(app.clone()).await;
        while tasks.join_next().await.is_some() {}
        assert!(tg.requests().is_empty());
    }

    #[tokio::test]
    async fn cwd_command_switches_work_dir_and_resets_session() {
        let (app, _tg) = e2e("cwd");
        app.update_chat(11, |s| s.session_id = Some("S-1".into())).await;
        let dir = std::env::temp_dir();
        handle_command(&app, 11, "/cwd", dir.to_str().unwrap()).await;
        let state = app.chat_state(11).await;
        assert!(state.work_dir.is_some() && state.session_id.is_none(), "сессия сброшена, директория новая");
        assert_eq!(work_dir(&app, 11).await, state.work_dir.unwrap());
    }

    #[test]
    fn bubble_shows_line_and_counters() {
        let bubble = Bubble {
            line: "Читаю файл: main.rs".into(),
            actions: 3,
            dropped: 2,
            started: None,
            rendered: None,
            message_id: None,
        };
        let text = bubble.render();
        assert!(text.starts_with("Читаю файл: main.rs"));
        assert!(text.contains("действий: 3"));
        assert!(text.contains("служебных строк скрыто: 2"));
        // пустой пузырь не плодит лишних строк
        assert_eq!(Bubble::default().render(), "");
    }

    #[test]
    fn attachment_names_get_message_id_and_counter() {
        let dir = std::env::temp_dir().join(format!("claudetg-attach-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let first = unique_path(&dir, 7, "отчёт.pdf");
        assert_eq!(first.file_name().unwrap().to_str().unwrap(), "7-отчёт.pdf");
        std::fs::write(&first, "раз").unwrap();
        let second = unique_path(&dir, 7, "отчёт.pdf");
        assert_eq!(second.file_name().unwrap().to_str().unwrap(), "7-отчёт-2.pdf");
        std::fs::write(&second, "два").unwrap();
        assert_eq!(
            unique_path(&dir, 7, "отчёт.pdf").file_name().unwrap().to_str().unwrap(),
            "7-отчёт-3.pdf",
            "счётчик доходит до третьего файла подряд"
        );
        // файл без расширения
        std::fs::write(dir.join("9-лог"), "x").unwrap();
        assert_eq!(
            unique_path(&dir, 9, "лог").file_name().unwrap().to_str().unwrap(),
            "9-лог-2"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sizes_are_readable() {
        assert_eq!(human_size(512), "512 байт");
        assert_eq!(human_size(2048), "2.0 КиБ");
        assert_eq!(human_size(5 * 1024 * 1024), "5.0 МиБ");
    }

    #[test]
    fn brief_folds_spaces_and_truncates() {
        assert_eq!(brief("раз,\n   два\tтри", 20), "раз, два три");
        let long = "с".repeat(30);
        assert_eq!(brief(&long, 5), format!("{}…", "с".repeat(5)));
    }

    #[test]
    fn deadline_respects_quiet_and_cap() {
        let now = tokio::time::Instant::now();
        // обычно: тишина от последней части
        assert_eq!(agg_deadline(now, now, 12, 60), now + Duration::from_secs(12));
        // предельное окно уже истекло — запуск возможен сразу
        let old = now - Duration::from_secs(70);
        assert!(agg_deadline(now, old, 12, 60) <= now);
        // до конца окна осталось меньше тишины — считаем по окну
        let fresh = now - Duration::from_secs(50);
        assert_eq!(agg_deadline(now, fresh, 12, 60), fresh + Duration::from_secs(60));
    }

    #[tokio::test]
    async fn take_batch_matches_generation() {
        let batches: Mutex<HashMap<i64, Batch>> = Mutex::new(HashMap::new());
        batches.lock().await.insert(
            7,
            Batch {
                texts: vec!["первая".into(), "вторая".into()],
                first_at: tokio::time::Instant::now(),
                gen: 3,
            },
        );
        // таймер старого поколения: окно уже дополнено, трогать нельзя
        assert!(take_batch(&batches, 7, 2).await.is_none());
        assert_eq!(batches.lock().await[&7].texts.len(), 2);
        // верное поколение: забираем всю задачу и освобождаем чат
        let got = take_batch(&batches, 7, 3).await.unwrap();
        assert_eq!(got, vec!["первая".to_string(), "вторая".to_string()]);
        assert!(batches.lock().await.is_empty());
        // окно отменено (/cancel снял запись) — тихий выход
        assert!(take_batch(&batches, 7, 3).await.is_none());
    }
    #[tokio::test]
    async fn send_goes_to_the_queue_when_the_bot_is_alive() {
        let (app, tg) = e2e("clisend");
        outbox::write_pid(&app.cfg.pid_file).unwrap();
        cli_send(&app.bot, &app.cfg, Some("11"), "привет из агента")
            .await
            .unwrap();
        assert_eq!(outbox::pending(&app.cfg.outbox_dir).len(), 1, "живой бот не шлёт сам");
        assert!(tg.requests().is_empty());
        app.drain_outbox(11).await;
        assert!(tg.calls("sendMessage")[0].contains("привет из агента"));
    }

    #[tokio::test]
    async fn send_writes_directly_when_the_bot_is_down() {
        let (app, tg) = e2e("clisend2");
        // pid-файла нет — бот не работает, сообщению нельзя остаться в очереди
        cli_send(&app.bot, &app.cfg, Some("11"), "привет").await.unwrap();
        assert_eq!(tg.calls("sendMessage").len(), 1);
        assert!(outbox::pending(&app.cfg.outbox_dir).is_empty());
    }

    #[tokio::test]
    async fn send_without_a_chat_arg_shows_usage() {
        let (app, _tg) = e2e("clisend3");
        let err = format!("{}", cli_send(&app.bot, &app.cfg, None, "текст").await.unwrap_err());
        assert!(err.contains("usage"), "{err}");
    }

    #[tokio::test]
    async fn send_file_uses_env_chat_or_explicit_one() {
        let (app, tg) = e2e("clifile");
        let path = std::env::temp_dir().join(format!("claudetg-cli-{}.apk", std::process::id()));
        std::fs::write(&path, "apk").unwrap();
        cli_send_file(&app.bot, &app.cfg, Some("11"), &[path.display().to_string(), "свежий".into()])
            .await
            .unwrap();
        // чат из CLAUDETG_CHAT_ID, который бот выдаёт процессу агента
        cli_send_file(&app.bot, &app.cfg, Some("12"), &[path.display().to_string(), "второй".into()])
            .await
            .unwrap();
        // явный --chat важнее переменной окружения
        cli_send_file(
            &app.bot,
            &app.cfg,
            Some("13"),
            &["--chat".into(), "14".into(), path.display().to_string(), "явный".into()],
        )
        .await
        .unwrap();
        let _ = std::fs::remove_file(&path);
        let seen = tg.calls("sendDocument");
        assert_eq!(seen.len(), 3);
        assert!(seen[0].contains("свежий"));
        assert!(seen[2].contains("явный"), "{}", seen[2]);
        let err = format!(
            "{}",
            cli_send_file(&app.bot, &app.cfg, None, &[path.display().to_string()]).await.unwrap_err()
        );
        assert!(err.contains("CLAUDETG_CHAT_ID"), "{err}");
    }

    #[tokio::test]
    async fn pieces_of_one_message_are_joined_by_the_window() {
        let agent = fake_agent("agg", &[r#"{"type":"result","result":"одна задача"}"#], 0, false);
        let tg = FakeTg::start();
        let mut cfg = test_config("agg", tg.base());
        cfg.claude_bin = agent.display().to_string();
        cfg.agg_quiet_secs = 1;
        cfg.agg_window_secs = 10;
        let app = Arc::new(App {
            bot: tg.bot(),
            chats: Mutex::new(HashMap::new()),
            running: Mutex::new(HashMap::new()),
            batches: Mutex::new(HashMap::new()),
            last_text: Mutex::new(HashMap::new()),
            status: Mutex::new(HashMap::new()),
            recent_out: Mutex::new(HashMap::new()),
            cfg,
        });
        // человек печатает задачу двумя сообщениями — запуск должен быть один
        handle_message(app.clone(), message_of(&update_text(1, 11, 7, "сделай раз"))).await;
        handle_message(app.clone(), message_of(&update_text(2, 11, 7, "и два"))).await;
        assert!(tg.calls("sendMessage").is_empty(), "пока тишина — агент молчит");
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if !tg.calls("sendMessage").is_empty() {
                break;
            }
        }
        let prompt = agent_prompt(&agent);
        assert!(prompt.contains("сделай раз") && prompt.contains("и два"), "{prompt}");
        assert_eq!(tg.calls("sendMessage").len(), 1);
        assert!(tg.calls("sendMessage")[0].contains("одна задача"));
    }

    #[tokio::test]
    async fn messages_arriving_during_a_turn_run_after_it() {
        let first = fake_agent("q1", &[r#"{"type":"result","result":"сделал первое"}"#], 1, false);
        let (app, tg) = e2e_with("q1", |c| {
            c.claude_bin = first.display().to_string();
            c.status_bubble = true;
        });
        let handle = tokio::spawn(run_agent(app.clone(), 11, "первая задача".into()));
        tokio::time::sleep(Duration::from_millis(400)).await;
        // вторая задача приходит, когда первая ещё выполняется: она попадёт в очередь чата
        run_agent(app.clone(), 11, "вторая задача".into()).await;
        let all = || {
            let mut v = tg.calls("sendMessage");
            v.extend(tg.calls("editMessageText"));
            v
        };
        assert!(all().iter().any(|s| s.contains("очереди")), "{:?}", all());
        // после конца первой смены стартует вторая: подсовываем скрипт для второго прогона
        let second = fake_agent("q2", &[r#"{"type":"result","result":"сделал второе"}"#], 0, false);
        let _ = std::fs::remove_file(&first);
        std::os::unix::fs::symlink(&second, &first).expect("подмена скрипта агента");
        handle.await.unwrap();
        for _ in 0..30 {
            if app.running.lock().await.is_empty() && all().iter().any(|s| s.contains("сделал второе")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let sent = all();
        assert!(sent.iter().any(|s| s.contains("сделал первое")), "{sent:?}");
        assert!(sent.iter().any(|s| s.contains("сделал второе")), "{sent:?}");
        assert!(app.running.lock().await.is_empty(), "очередь чата разобрана");
    }

    #[tokio::test]
    async fn status_bubble_is_created_edited_and_removed() {
        let (app, tg) = e2e_with("bubble", |c| c.status_bubble = true);
        assert!(tg.requests().is_empty());
        app.status(11, "Читаю файл: main.rs").await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1, "пузырь — одно сообщение");
        assert!(sent[0].contains("Читаю файл"));
        // пузырь правят не чаще, чем раз в 1,5 с — выжидаем паузу
        tokio::time::sleep(Duration::from_millis(1600)).await;
        app.status_progress(11, "Ищу по коду").await;
        let edits = tg.calls("editMessageText");
        assert_eq!(edits.len(), 1, "{edits:?}");
        assert!(edits[0].contains("Ищу по коду"));
        app.status_dropped(11).await;
        app.status_finish(11).await;
        let dels = tg.calls("deleteMessage");
        assert_eq!(dels.len(), 1, "пузырь убран из чата");
        assert!(app.status.lock().await.is_empty());
    }

    #[tokio::test]
    async fn cleanup_removes_service_messages_and_forgets_them() {
        let (app, tg) = e2e("clean");
        let records = vec![
            SentRecord { chat: 11, message_id: 1, ts: 1, kind: Kind::Note, preview: "🕐 задача принята".into() },
            SentRecord { chat: 11, message_id: 2, ts: 2, kind: Kind::Agent, preview: "Выполняю команду: собрать".into() },
            SentRecord { chat: 11, message_id: 3, ts: 3, kind: Kind::Answer, preview: "Итог работы".into() },
            SentRecord { chat: 11, message_id: 4, ts: 4, kind: Kind::Answer, preview: "Вопрос".into() },
            SentRecord { chat: 11, message_id: 5, ts: 5, kind: Kind::Answer, preview: "Пятый".into() },
            SentRecord { chat: 11, message_id: 6, ts: 6, kind: Kind::Answer, preview: "Шестой".into() },
        ];
        outbox::append_sent(&app.cfg.sent_log, &records).unwrap();
        clean_messages(&app.bot, &app.cfg, &list(&["--chat", "11"])).await.unwrap();
        // по умолчанию бережем последние пять: из шести записи под чистку
        // попадает только самая старая — служебная заметка
        let seen = tg.calls("deleteMessage");
        assert_eq!(seen.len(), 1, "{seen:?}");
        assert!(seen[0].contains("\"message_id\":1"), "{seen:?}");
        let left = outbox::read_sent(&app.cfg.sent_log);
        assert_eq!(left.len(), 5, "журнал починился");

        // с --keep 2 под чистку попадают и заметка, и болтовня «выполняю команду»
        let (app, tg) = e2e_reopen("clean", |_| {});
        clean_messages(&app.bot, &app.cfg, &list(&["--chat", "11", "--keep", "2"])).await.unwrap();
        let seen = tg.calls("deleteMessage");
        // заметка уже удалена, из старого остаётся болтовня «выполняю команду»
        assert_eq!(seen.len(), 1, "{seen:?}");
        assert!(seen[0].contains("\"message_id\":2"), "{seen:?}");
        assert_eq!(outbox::read_sent(&app.cfg.sent_log).len(), 4);
    }

    #[tokio::test]
    async fn cleanup_dry_run_touches_nothing() {
        let (app, tg) = e2e("clean2");
        outbox::append_sent(
            &app.cfg.sent_log,
            &[
                SentRecord { chat: 11, message_id: 1, ts: 1, kind: Kind::Note, preview: "🕐 принято".into() },
                SentRecord { chat: 11, message_id: 2, ts: 2, kind: Kind::Note, preview: "🔍 ищу по коду".into() },
                SentRecord { chat: 11, message_id: 3, ts: 3, kind: Kind::Answer, preview: "итог".into() },
            ],
        )
        .unwrap();
        // --keep 1: под чистку попадают первые две записи, но в чате никто не бывает
        clean_messages(&app.bot, &app.cfg, &list(&["--chat", "11", "--dry", "--keep", "1"])).await.unwrap();
        assert!(tg.calls("deleteMessage").is_empty());
        assert_eq!(outbox::read_sent(&app.cfg.sent_log).len(), 3, "пробный прогон ничего не меняет");
    }

    #[tokio::test]
    async fn cleanup_says_there_is_nothing_to_remove() {
        let (app, tg) = e2e("cleanempty");
        // журнал пустой: ни одного обращения в чат
        clean_messages(&app.bot, &app.cfg, &list(&["--chat", "11"])).await.unwrap();
        assert!(tg.requests().is_empty(), "пустой журнал — никаких запросов");
        // журнал есть, но все записи помечены как последние — тоже не трогаем
        outbox::append_sent(
            &app.cfg.sent_log,
            &[SentRecord { chat: 11, message_id: 5, ts: 5, kind: Kind::Note, preview: "🕐 принято".into() }],
        )
        .unwrap();
        clean_messages(&app.bot, &app.cfg, &list(&["--chat", "11", "--keep", "3"])).await.unwrap();
        assert!(tg.calls("deleteMessage").is_empty(), "последние сообщения не удаляем");
        assert_eq!(outbox::read_sent(&app.cfg.sent_log).len(), 1, "журнал целён");
    }

    #[tokio::test]
    async fn cleanup_all_flag_forgets_the_keep_limit() {
        let (app, tg) = e2e("clean3");
        let records = (1..=4)
            .map(|i| SentRecord { chat: 11, message_id: i, ts: i as i64, kind: Kind::Answer, preview: format!("ответ {i}") })
            .collect::<Vec<_>>();
        outbox::append_sent(&app.cfg.sent_log, &records).unwrap();
        clean_messages(&app.bot, &app.cfg, &list(&["--chat", "11", "--all", "--keep", "1"])).await.unwrap();
        // --all снимает фильтр по «служебности», --keep 1 бережёт последний ответ
        assert_eq!(tg.calls("deleteMessage").len(), 3);
        assert_eq!(outbox::read_sent(&app.cfg.sent_log).len(), 1);
    }

    #[tokio::test]
    async fn cleanup_without_a_chat_says_how_to_use_it() {
        let (app, tg) = e2e("clean4");
        match std::env::var("CLAUDETG_CHAT_ID") {
            // без явного --chat берётся переменная окружения (её бот даёт агенту)
            Ok(_) => {
                clean_messages(&app.bot, &app.cfg, &list(&[])).await.unwrap();
                assert!(tg.calls("deleteMessage").is_empty(), "в пустом журнале чистить нечего");
            }
            // а если чата нет ни в аргументах, ни в окружении — понятная ошибка
            Err(_) => {
                let err = format!("{}", clean_messages(&app.bot, &app.cfg, &list(&[])).await.unwrap_err());
                assert!(err.contains("usage") || err.contains("чат не задан"), "{err}");
            }
        }
    }

    #[tokio::test]
    async fn whitespace_answers_are_not_sent() {
        let (app, tg) = e2e("blank");
        app.queue(11, Kind::Answer, "   \n ").await;
        app.drain_outbox(11).await;
        assert!(tg.calls("sendMessage").is_empty(), "пустышку в чат не гоним");
        assert!(outbox::pending(&app.cfg.outbox_dir).is_empty(), "но из очереди она ушла");
    }

    #[tokio::test]
    async fn failed_send_keeps_the_queue_for_the_retry() {
        // адрес, где никто не слушает: первая попытка упирается в сетевой сбой
        let dead_addr = crate::testkit::dead_endpoint();
        let dead = Bot::new("TOK", None, Some(&format!("http://{dead_addr}"))).unwrap();
        let tg = FakeTg::start();
        let cfg = test_config("retry", tg.base());
        let app = Arc::new(App {
            bot: dead,
            chats: Mutex::new(HashMap::new()),
            running: Mutex::new(HashMap::new()),
            batches: Mutex::new(HashMap::new()),
            last_text: Mutex::new(HashMap::new()),
            status: Mutex::new(HashMap::new()),
            recent_out: Mutex::new(HashMap::new()),
            cfg,
        });
        app.queue(11, Kind::Answer, "это должно дойти").await;
        app.drain_outbox(11).await;
        assert_eq!(outbox::pending(&app.cfg.outbox_dir).len(), 1, "сбой не съедает сообщение");
        // связь вернулась — то же сообщение уходит без потери
        let (app2, tg2) = e2e_reopen("retry", |_| {});
        app2.drain_outbox(11).await;
        assert_eq!(outbox::pending(&app2.cfg.outbox_dir).len(), 0);
        assert!(tg2.calls("sendMessage")[0].contains("это должно дойти"));
    }

    #[tokio::test]
    async fn flood_pause_keeps_the_whole_queue_intact() {
        let tg = FakeTg::start().script(
            "sendMessage",
            vec![r#"{"ok":false,"description":"Too Many Requests: retry after 2","parameters":{"retry_after":2}}"#.into()],
        );
        let cfg = test_config("pause", tg.base());
        let app = Arc::new(App {
            bot: tg.bot(),
            chats: Mutex::new(HashMap::new()),
            running: Mutex::new(HashMap::new()),
            batches: Mutex::new(HashMap::new()),
            last_text: Mutex::new(HashMap::new()),
            status: Mutex::new(HashMap::new()),
            recent_out: Mutex::new(HashMap::new()),
            cfg,
        });
        app.queue(11, Kind::Answer, "ответ, который не пустили").await;
        app.drain_outbox(11).await;
        assert!(app.bot.send_paused(), "пауза заведена");
        app.drain_outbox(11).await;
        app.drain_outbox(11).await;
        // одна попытка — и до конца запрета ни одной лишней
        assert_eq!(tg.calls("sendMessage").len(), 1, "{:?}", tg.requests());
        assert_eq!(outbox::pending(&app.cfg.outbox_dir).len(), 1, "очередь цела");
    }

    #[tokio::test]
    async fn agent_without_a_session_still_answers() {
        let agent = fake_agent("nocl", &[r#"{"type":"result","result":"ок"}"#], 0, true);
        let (app, tg) = e2e_agent("nocl", &agent);
        run_turn(&app, 11, "дело".into(), Arc::new(Notify::new())).await;
        let args = crate::testkit::agent_args(&agent);
        assert!(!args.contains(&"--resume".to_string()), "новая сессия без --resume: {args:?}");
        assert!(tg.calls("sendMessage")[0].contains("ок"));
    }

    #[tokio::test]
    async fn opencode_turn_resumes_by_session_id() {
        let first = fake_agent(
            "occont",
            &[r#"{"type":"text","sessionID":"oc-5","part":{"text":"раз"}}"#],
            0,
            false,
        );
        let (app, tg) = e2e_agent("occont", &first);
        app.update_chat(11, |s| s.backend = Some(Backend::OpenCode)).await;
        run_turn(&app, 11, "первая".into(), Arc::new(Notify::new())).await;
        assert_eq!(app.chat_state(11).await.opencode_session_id.as_deref(), Some("oc-5"));
        let second = fake_agent(
            "occont2",
            &[r#"{"type":"text","part":{"text":"два"}}"#],
            0,
            true,
        );
        let (app2, tg2) = e2e_reopen("occont", |c| c.opencode_bin = second.display().to_string());
        run_turn(&app2, 11, "вторая".into(), Arc::new(Notify::new())).await;
        let args = crate::testkit::agent_args(&second);
        assert!(args.contains(&"--session".to_string()) && args.contains(&"oc-5".to_string()), "{args:?}");
        assert_eq!(tg.calls("sendMessage").len(), 1);
        assert_eq!(tg2.calls("sendMessage").len(), 1);
    }

    #[tokio::test]
    async fn bubble_is_hidden_when_tools_are_off() {
        let agent = fake_agent(
            "nobubble",
            &[
                r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"description":"собираю"}}]}}"#,
                r#"{"type":"result","result":"собрано"}"#,
            ],
            0,
            false,
        );
        let (app, tg) = e2e_agent("nobubble", &agent);
        run_turn(&app, 11, "собери".into(), Arc::new(Notify::new())).await;
        assert_eq!(tg.calls("sendMessage").len(), 1, "в чате только ответ агента");
        assert!(tg.calls("editMessageText").is_empty());
    }

    #[tokio::test]
    async fn polling_reads_updates_and_advances_offset() {
        let agent = fake_agent(
            "poll",
            &[r#"{"type":"result","result":"сделано поллингом"}"#],
            0,
            false,
        );
        let upd = |id: i64, user: i64, text: &str| {
            format!(
                r#"{{"update_id":{id},"message":{{"message_id":{id},"chat":{{"id":11}},"from":{{"id":{user}}},"text":"{text}"}}}}"#
            )
        };
        let tg = FakeTg::start().script(
            "getUpdates",
            vec![
                // чужое обновление игнорируется, своё доходит до агента
                format!(
                    r#"{{"ok":true,"result":[{},{},{}]}}"#,
                    upd(10, 999, "не от своего"),
                    upd(11, 7, "дело"),
                    // обновление не-сообщение (редактирование, ответ на кнопку):
                    // смещение сдвигаем, но обработчик не заводим
                    r#"{"update_id":12}"#
                ),
                r#"{"ok":true,"result":[]}"#.into(),
            ],
        );
        let mut c = crate::testkit::test_config("poll", tg.base());
        c.claude_bin = agent.display().to_string();
        let app = Arc::new(App {
                cfg: c,
                bot: tg.bot(),
                chats: Mutex::new(HashMap::new()),
                running: Mutex::new(HashMap::new()),
                batches: Mutex::new(HashMap::new()),
                last_text: Mutex::new(HashMap::new()),
                status: Mutex::new(HashMap::new()),
                recent_out: Mutex::new(HashMap::new()),
            });
        let mut offset = 0;
        poll_once(&app, &mut offset).await.unwrap();
        assert_eq!(offset, 13, "смещение сдвинуто за последним обновлением");
        for _ in 0..40 {
            if !tg.calls("sendMessage").is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1, "отвечили только своему: {sent:?}");
        assert!(sent[0].contains("сделано поллингом"));
        // пустой виток — без изменений
        poll_once(&app, &mut offset).await.unwrap();
        assert_eq!(offset, 13);
    }

    #[tokio::test]
    async fn polling_survives_a_broken_updates_call() {
        let (app, tg) = e2e_with("pollerr", |_| {});
        let dead_addr = crate::testkit::dead_endpoint();
        let mut offset = 3;
        let dead = Bot::new("TOK", None, Some(&format!("http://{dead_addr}"))).unwrap();
        let err = format!("{}", dead.get_updates(offset).await.expect_err("связи нет"));
        assert!(err.contains("error sending request") || err.contains("Connection refused") || err.contains("connection closed"), "{err}");
        // пустой ответ подставы — нормальный виток
        poll_once(&app, &mut offset).await.unwrap();
        assert_eq!(offset, 3, "пустая выборка смещение не двигает");
        assert_eq!(tg.calls("getUpdates").len(), 1);
    }

    #[tokio::test]
    async fn unknown_subcommand_never_starts_a_second_poller() {
        let (app, tg) = e2e("dispatch");
        let a = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let err = format!(
            "{}",
            cli_dispatch(&app.bot, &app.cfg, &a(&["claudetg", "deamon"]), "").await.unwrap_err()
        );
        assert!(err.contains("неизвестная подкоманда"), "{err}");
        // run/start/poll и пустой список — это про поллинг, dispatch молчит
        for verb in ["run", "start", "poll"] {
            assert!(cli_dispatch(&app.bot, &app.cfg, &a(&["claudetg", verb]), "").await.unwrap().is_none());
        }
        assert!(cli_dispatch(&app.bot, &app.cfg, &a(&["claudetg"]), "").await.unwrap().is_none());
        // send через dispatch: живой бот — значит в очередь, не в чат напрямую
        outbox::write_pid(&app.cfg.pid_file).unwrap();
        let r = cli_dispatch(&app.bot, &app.cfg, &a(&["claudetg", "send", "11"]), "текст из stdin-теста")
            .await
            .unwrap()
            .expect("send обработан dispatch-ем");
        r.unwrap();
        assert_eq!(outbox::pending(&app.cfg.outbox_dir).len(), 1);
        assert!(tg.requests().is_empty(), "сам dispatch в чат не пишет");
        // clean через dispatch
        outbox::append_sent(
            &app.cfg.sent_log,
            &[SentRecord { chat: 11, message_id: 1, ts: 1, kind: Kind::Note, preview: "🕐 принято".into() }],
        )
        .unwrap();
        // --keep 0 снимает защиту «последних пяти», заметка уходит из чата
        let r = cli_dispatch(
            &app.bot,
            &app.cfg,
            &a(&["claudetg", "clean", "--chat", "11", "--keep", "0"]),
            "",
        )
        .await
        .unwrap()
        .expect("clean обработан dispatch-ем");
        r.unwrap();
        assert_eq!(tg.calls("deleteMessage").len(), 1);
        assert!(outbox::read_sent(&app.cfg.sent_log).is_empty(), "журнал почистился");
    }

    #[tokio::test]
    async fn resume_due_continues_a_chat_past_its_schedule() {
        let agent = fake_agent("rdue", &[r#"{"type":"result","result":"продолжил сам"}"#], 0, false);
        let (app, tg) = e2e_with("rdue", |c| c.claude_bin = agent.display().to_string());
        app.update_chat(11, |s| {
            s.resume_at = Some(chrono::Utc::now().timestamp() - 1);
            s.resume_label = Some("09:00 Europe/Moscow".into());
        })
        .await;
        // чат с будущим сроком не трогаем
        app.update_chat(12, |s| s.resume_at = Some(chrono::Utc::now().timestamp() + 3600)).await;
        let mut tasks = resume_due(app.clone()).await;
        while tasks.join_next().await.is_some() {}
        let sent = tg.calls("sendMessage");
        assert!(sent.iter().any(|s| s.contains("Лимит обновился — продолжаю")), "{sent:?}");
        assert!(sent.iter().any(|s| s.contains("продолжил сам")));
        assert_eq!(app.chat_state(11).await.resume_at, None, "план снят");
        assert!(app.chat_state(12).await.resume_at.is_some(), "будущий план цел");
    }

    #[tokio::test]
    async fn full_bot_start_polls_and_answers() {
        // настоящий путь запуска: getMe, pid-файл, фоновые задачи и цикл поллинга —
        // всё на подставном Telegram и подставном агенте
        let agent = fake_agent(
            "startbot",
            &[r#"{"type":"result","result":"бот поднялся и ответил"}"#],
            0,
            false,
        );
        let upd = r#"{"update_id":1,"message":{"message_id":1,"chat":{"id":11},"from":{"id":7},"text":"дело"}}"#;
        let tg = FakeTg::start().script(
            "getUpdates",
            vec![format!(r#"{{"ok":true,"result":[{upd}]}}"#)],
        );
        let mut cfg = crate::testkit::test_config("startbot", tg.base());
        cfg.claude_bin = agent.display().to_string();
        let handle = tokio::spawn(run_bot(cfg, tg.bot()));
        for _ in 0..60 {
            if !tg.calls("sendMessage").is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        handle.abort();
        assert!(tg.calls("getMe").len() >= 1, "представились Telegram-у");
        let sent = tg.calls("sendMessage");
        assert!(sent.iter().any(|s| s.contains("бот поднялся и ответил")), "{sent:?}");
        // pid-файл записан: `claudetg send` по нему видит живого бота
        let pid = std::env::temp_dir().join(format!("claudetg-test-startbot-{}", std::process::id()));
        assert!(outbox::bot_alive(&pid.join("bot.pid")), "pid-файл живой");
    }

    #[test]
    fn seed_recent_trims_to_the_newest() {
        let many: Vec<SentRecord> = (1..=12)
            .map(|i| SentRecord {
                chat: 11,
                message_id: i,
                ts: i as i64,
                kind: Kind::Answer,
                preview: format!("ответ {i}"),
            })
            .collect();
        let seeded = outbox::seed_recent(&many, RECENT_SENDS);
        assert_eq!(seeded.len(), 1, "один чат");
        let texts = &seeded[&11];
        assert_eq!(texts.len(), RECENT_SENDS, "память ограничена");
        assert_eq!(texts.last().unwrap(), "ответ 12", "свежее — в конце");
        assert!(!texts.contains(&"ответ 1".to_string()), "старое выброшено");
    }
    #[tokio::test]
    async fn queue_files_go_to_the_chat_and_journal() {
        // живой бот: send-file не лезет в сеть сам, а ставит файл в очередь —
        // так отдача пережидает лимит Telegram и попадает в журнал отправленного
        let (app, tg) = e2e("filequeue");
        outbox::write_pid(&app.cfg.pid_file).expect("pid-файл живого бота");
        let dir = std::env::temp_dir().join(format!("claudetg-files-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let ok = dir.join("otchet.txt");
        std::fs::write(&ok, "dannye").unwrap();
        let missing = dir.join("uzhe-net.apk");

        cli_send_file(&app.bot, &app.cfg, Some("11"), &[ok.display().to_string(), "gotovyi otchet".into()])
            .await
            .unwrap();
        assert!(tg.requests().is_empty(), "живой бот в сеть не ходил: {:?}", tg.requests());
        assert_eq!(outbox::pending(&app.cfg.outbox_dir).len(), 1, "файл в очереди");

        // файла нет на диске: запись снимается с очереди, а не висит вечно
        outbox::enqueue(&app.cfg.outbox_dir, &Item::file(11, Kind::Agent, &missing, "net fayla")).unwrap();
        outbox::enqueue(&app.cfg.outbox_dir, &Item::new(11, Kind::Answer, "otchet sobran")).unwrap();
        app.drain_outbox(11).await;
        let docs = tg.calls("sendDocument");
        assert_eq!(docs.len(), 1, "ушёл только тот файл, что есть: {docs:?}");
        assert!(docs[0].contains("gotovyi otchet"), "{}", docs[0]);
        let texts = tg.calls("sendMessage");
        assert_eq!(texts.len(), 1, "текст шага ушёл своим чередом: {texts:?}");
        assert!(texts[0].contains("otchet sobran"), "{}", texts[0]);
        assert!(outbox::pending(&app.cfg.outbox_dir).is_empty(), "очередь разобрана");
        let journal = std::fs::read_to_string(&app.cfg.sent_log).unwrap_or_default();
        assert!(journal.contains("otchet.txt"), "файл записан в журнал: {journal}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn an_unsent_file_stays_in_the_queue_while_texts_go() {
        // Telegram отверг файл: он остаётся в очереди и дойдёт позже, а тексты
        // этого шага не должны из-за этого застрять
        let (app, tg) = e2e_with("filefail", |_| {});
        let tg = tg.script(
            "sendDocument",
            vec![r#"{"ok":false,"description":"Bad Request: wrong file witness"}"#.into()],
        );
        let dir = std::env::temp_dir().join(format!("claudetg-filefail-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let apk = dir.join("vypusk.apk");
        std::fs::write(&apk, "apk").unwrap();
        outbox::enqueue(&app.cfg.outbox_dir, &Item::file(11, Kind::Agent, &apk, "vypusk")).unwrap();
        outbox::enqueue(&app.cfg.outbox_dir, &Item::new(11, Kind::Answer, "shag gotov")).unwrap();
        app.drain_outbox(11).await;
        assert_eq!(tg.calls("sendMessage").len(), 1, "текст ушёл: {:?}", tg.calls("sendMessage"));
        assert_eq!(tg.calls("sendDocument").len(), 1, "файл пробован один раз");
        let left = outbox::pending(&app.cfg.outbox_dir);
        assert_eq!(left.len(), 1, "файл остался в очереди: {left:?}");
        assert!(left[0].1.is_file(), "в очереди именно файл");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn the_same_file_is_not_sent_twice() {
        // память о последних отправленных файлах чата: перезапуск бота или
        // повторная команда агента не должны слать тот же APK дважды
        let (app, tg) = e2e("filetwice");
        let dir = std::env::temp_dir().join(format!("claudetg-filetwice-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let apk = dir.join("vypusk.apk");
        std::fs::write(&apk, "apk").unwrap();
        outbox::enqueue(&app.cfg.outbox_dir, &Item::file(11, Kind::Agent, &apk, "pervyraz")).unwrap();
        app.drain_outbox(11).await;
        outbox::enqueue(&app.cfg.outbox_dir, &Item::file(11, Kind::Agent, &apk, "vtorojraz")).unwrap();
        app.drain_outbox(11).await;
        let docs = tg.calls("sendDocument");
        assert_eq!(docs.len(), 1, "тот же файл один раз: {docs:?}");
        assert!(outbox::pending(&app.cfg.outbox_dir).is_empty(), "повтор снят с очереди");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_state_file_that_cannot_be_written_is_only_logged() {
        // файл состояния оказался каталогом: бот не роняет чат, а пишет в журнал
        let (app, _tg) = e2e_with("statebad", |cfg| {
            std::fs::create_dir_all(&cfg.outbox_dir).expect("каталог очереди");
            cfg.state_file = cfg.outbox_dir.clone();
        });
        app.save_state().await;
        assert!(std::fs::read_dir(&app.cfg.outbox_dir).is_ok(), "каталог на месте");
    }

    #[tokio::test]
    async fn a_state_file_named_without_a_directory_is_written_here() {
        // имя файла без каталога (только «state.json»): каталог создавать нечего,
        // и запись идёт туда, где запущен процесс
        let name = format!("claudetg-state-bare-{}.json", std::process::id());
        let _ = std::fs::remove_file(&name);
        let (app, _tg) = e2e_with("statebare", |cfg| {
            cfg.state_file = std::path::PathBuf::from(&name);
        });
        app.update_chat(11, |s| s.session_id = Some("S-bare".into())).await;
        app.save_state().await;
        let written = std::fs::read_to_string(&name).unwrap_or_default();
        assert!(written.contains("S-bare"), "файл записан рядом: {written}");
        // перечитывание тем же путём: сессия находится
        let reloaded = App::load_state(&app.cfg);
        assert_eq!(
            reloaded.get(&11).and_then(|s| s.session_id.as_deref()),
            Some("S-bare"),
            "состояние читается обратно"
        );
        let _ = std::fs::remove_file(&name);
    }

    #[tokio::test]
    async fn the_same_tool_line_does_not_edit_the_bubble_twice() {
        let (app, tg) = e2e_with("bubblesame", |c| c.status_bubble = true);
        app.status_progress(11, "✏️ Правлю файл: main.rs").await;
        app.status_progress(11, "✏️ Правлю файл: main.rs").await;
        app.status_dropped(11).await;
        app.status_progress(11, "✏️ Правлю файл: main.rs").await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1, "одно сообщение на три одинаковые строки: {sent:?}");
        assert!(tg.calls("editMessageText").is_empty(), "тот же текст не правим: {:?}", tg.requests());
        // пузырь, который так и не ушёл в чат, нечего и удалять
        app.status.lock().await.insert(12, Bubble::default());
        app.status_finish(12).await;
        assert!(tg.calls("deleteMessage").is_empty(), "удалять нечего: {:?}", tg.requests());
        assert!(!app.status.lock().await.contains_key(&12), "счётчик снят с учёта");
    }

    #[tokio::test]
    async fn nothing_is_logged_when_nothing_was_sent() {
        let (app, tg) = e2e("logempty");
        app.log_sent(11, &[], Kind::Answer, "не ушло ни одного сообщения");
        app.drain_outbox(42).await; // чат без очереди: тихий выход
        assert!(tg.requests().is_empty(), "в чат не ушло ничего: {:?}", tg.requests());
        let journal = std::fs::read_to_string(&app.cfg.sent_log).unwrap_or_default();
        assert!(!journal.contains("ни одного"), "{journal}");
    }

    #[tokio::test]
    async fn only_the_expired_part_of_a_queue_is_dropped() {
        // очередь пережила лимит частично: старое снимаем, свежее уходит
        let (app, tg) = e2e_with("ttlpart", |c| c.send_ttl_secs = 60);
        let old = outbox::enqueue(&app.cfg.outbox_dir, &Item::new(11, Kind::Answer, "старый ответ прошлого вечера")).unwrap();
        let raw = std::fs::read_to_string(&old).unwrap().replace("\"ts\":", "\"ts\": 1, \"_x\":");
        std::fs::write(old, raw).unwrap();
        outbox::enqueue(&app.cfg.outbox_dir, &Item::new(11, Kind::Answer, "свежий ответ")).unwrap();
        app.drain_outbox(11).await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1, "уходит только свежее: {sent:?}");
        assert!(sent[0].contains("свежий ответ"), "{}", sent[0]);
        assert!(outbox::pending(&app.cfg.outbox_dir).is_empty(), "очередь разобрана");
    }

    #[tokio::test]
    async fn agent_send_file_finds_its_chat_in_the_environment() {
        let (app, tg) = e2e("clidispatch");
        let path = std::env::temp_dir().join(format!("claudetg-clidispatch-{}.apk", std::process::id()));
        std::fs::write(&path, "apk").unwrap();
        // агент вызывает `claudetg send-file <файл>`; чат он не указывает — его
        // даёт процессу переменная CLAUDETG_CHAT_ID
        std::env::set_var("CLAUDETG_CHAT_ID", "11");
        let args = ["claudetg", "send-file", &path.display().to_string(), "готовый файл"];
        let res = cli_dispatch(&app.bot, &app.cfg, &args.map(String::from), "").await;
        std::env::remove_var("CLAUDETG_CHAT_ID");
        let _ = std::fs::remove_file(&path);
        assert!(res.unwrap().expect("команда распознана").is_ok(), "отправка прошла");
        let seen = tg.calls("sendDocument");
        assert_eq!(seen.len(), 1, "{seen:?}");
        assert!(seen[0].contains("11") && seen[0].contains("готовый"), "{}", seen[0]);
    }

    #[tokio::test]
    async fn a_direct_send_that_never_arrives_is_an_error() {
        // бот не работает — отправляем сами; если Telegram не принял, это ошибка,
        // а не «успех» с потерянным сообщением
        let broken = r#"{"ok":true,"result":"#.to_string();
        let tg = FakeTg::start().script("sendMessage", vec![broken.clone(), broken.clone(), broken]);
        let cfg = crate::testkit::test_config_fresh("clifail", tg.base(), true);
        let bot = tg.bot();
        let err = format!("{}", cli_send(&bot, &cfg, Some("11"), "текст в чат").await.unwrap_err());
        assert!(err.contains("не удалось отправить в чат 11"), "{err}");
        assert_eq!(tg.calls("sendMessage").len(), 3, "отправку повторили трижды");
    }

    #[tokio::test]
    async fn an_emptied_aggregation_window_starts_nothing() {
        let (app, tg) = e2e_with("aggempty", |c| {
            c.agg_quiet_secs = 1;
            c.agg_window_secs = 60;
            c.claude_bin = "/nonexistent/claudetg-агент".into();
        });
        submit_text(&app, 11, "первая часть задачи".into()).await;
        let before = tg.calls("sendMessage").len();
        // окно сняли до срабатывания таймера (так делает /cancel): текстов нет —
        // таймер должен молчать, а не поднимать агента на пустом месте
        if let Some(batch) = app.batches.lock().await.get_mut(&11) {
            batch.texts.clear();
        }
        tokio::time::sleep(Duration::from_millis(1600)).await;
        assert!(app.batches.lock().await.is_empty(), "окно закрыто");
        assert_eq!(tg.calls("sendMessage").len(), before, "агент не запускался: {:?}", tg.requests());
    }

    #[tokio::test]
    async fn backend_command_offers_the_other_agent() {
        let (app, tg) = e2e("backendcmd");
        app.update_chat(11, |s| s.backend = Some(Backend::OpenCode)).await;
        handle_command(&app, 11, "/backend", "").await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert!(sent[0].contains("opencode") && sent[0].contains("/backend claude"), "{}", sent[0]);
    }

    #[tokio::test]
    async fn status_says_the_task_runs_right_now() {
        let (app, tg) = e2e("statusrun");
        app.running.lock().await.insert(11, Running { cancel: Arc::new(Notify::new()), queue: Vec::new() });
        handle_command(&app, 11, "/status", "").await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert!(sent[0].contains(r#"Выполняется: да\nАвтопродолжение"#), "{}", sent[0]);
    }

    #[tokio::test]
    async fn a_stuck_previous_shift_is_not_waited_for_forever() {
        // бот перезапустился, а сессию этого чата всё ещё ведёт процесс от прежнего
        // запуска: ждём его конца, но не вечно — срок задаёт настройка
        let agent = fake_agent("shiftcap", &[r#"{"type":"result","result":"новая смена заработала"}"#], 0, false);
        let (app, tg) = e2e_with("shiftcap", |c| {
            c.claude_bin = agent.display().to_string();
            c.status_bubble = true;
            c.shift_wait_secs = 1;
        });
        let (mut child, _script) = crate::testkit::adopted_agent("shiftcap", "S-stuck");
        app.update_chat(11, |s| s.session_id = Some("S-stuck".into())).await;
        run_agent(app.clone(), 11, "дело".into()).await;
        let _ = child.kill();
        let _ = child.wait();
        let sent = tg.calls("sendMessage");
        assert!(sent.iter().any(|s| s.contains("ведёт процесс с прошлого запуска бота")), "{sent:?}");
        assert!(sent.iter().any(|s| s.contains("новая смена заработала")), "{sent:?}");
        assert!(!tg.calls("sendChatAction").is_empty(), "пока ждали — держали индикатор");
    }

    #[tokio::test]
    async fn limit_handoff_continues_the_shift_on_its_own() {
        // лимит + замещение: смену продолжает сам run_agent, человеку не нужно
        // писать второе сообщение
        let agent = fake_agent(
            "shiftcont",
            &[r#"{"type":"result","result":"\u26a0\ufe0f You've hit your 5-hour limit \u00b7 resets 9am (Europe/Moscow)"}"#],
            0,
            false,
        );
        let (app, tg) = e2e_with("shiftcont", |c| {
            c.claude_bin = agent.display().to_string();
            c.opencode_bin = agent.display().to_string();
            c.limit_fallback = true;
        });
        record_turn(&app, 11, "собери отчёт".into(), "собрал, остались детали".into(), false).await;
        run_agent(app.clone(), 11, "собери отчёт".into()).await;
        let state = app.chat_state(11).await;
        assert_eq!(state.backend, Some(Backend::OpenCode), "чат передан opencode");
        assert_eq!(state.fallback_from, Some(Backend::Claude), "возврат запланирован в claude");
        // последнее, что получил агент — промпт продолжения с контекстом замещения
        let prompt = crate::testkit::agent_prompt(&agent);
        assert!(prompt.contains(handoff::HANDOFF_HEADER), "{prompt}");
        assert!(prompt.contains("Продолжай работу из переданного выше контекста"), "{prompt}");
        assert!(tg.calls("sendMessage").iter().any(|s| s.contains("Передаю контекст opencode")));
    }

    #[tokio::test]
    async fn the_outbox_waits_out_a_telegram_pause_and_then_flushes() {
        let (app, tg) = e2e_with("flushpause", |c| c.send_quiet_secs = 0);
        let tg = tg.script(
            "sendMessage",
            vec![r#"{"ok":false,"description":"Too Many Requests: retry after 1","parameters":{"retry_after":1}}"#.into()],
        );
        outbox::enqueue(&app.cfg.outbox_dir, &Item::new(11, Kind::Answer, "ответ после паузы")).unwrap();
        // первая попытка упирается в лимит: заводим паузу, как это делает живой бот
        app.bot.send_text(11, "первая попытка").await;
        assert!(app.bot.send_paused(), "пауза от Telegram заведена");
        let handle = tokio::spawn(flush_outbox(app.clone()));
        for _ in 0..40 {
            if tg.calls("sendMessage").len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        handle.abort();
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 2, "во время паузы в чат не летело ничего: {sent:?}");
        assert!(sent[1].contains("ответ после паузы"), "{sent:?}");
        assert!(outbox::pending(&app.cfg.outbox_dir).is_empty(), "очередь разобрана");
    }

    #[tokio::test]
    async fn the_journal_of_a_chat_stays_bounded() {
        let (app, _tg) = e2e("recentcap");
        for i in 0..=handoff::RECENT_TURNS {
            record_turn(&app, 11, format!("задача {i}"), format!("ответ {i}"), false).await;
        }
        let state = app.chat_state(11).await;
        assert_eq!(state.recent.len(), handoff::RECENT_TURNS, "журнал не растёт бесконечно");
        assert_eq!(state.recent[0].user, format!("задача {}", 1), "старое_exchange выброшено");
    }

    /// Лог включаем во всех сквозных тестах: ветки с `log::...` в коде бота
    /// тоже должны быть проверены (иначе в бою там оказывается кривой код).
    fn logging_on() {
        let _ = env_logger::Builder::new()
            .filter_level(log::LevelFilter::Info)
            .try_init();
    }

    #[tokio::test]
    async fn bot_start_adopts_a_live_session_and_survives_a_bad_reply() {
        // полный путь запуска: getMe, состояние чатов из файла, поиск смены от
        // прежнего запуска, фоновые задачи и цикл поллинга
        logging_on();
        let tg = FakeTg::start().script(
            "getUpdates",
            vec![
                // кривой ответ: цикл не падает, а ругается, ждёт и пробует снова
                r#"{"ok":true,"result":"это не список"}"#.into(),
                // чат без смены от прежнего запуска — задача уходит агенту сразу
                update_text(1, 12, 7, "дело"),
            ],
        );
        let agent = fake_agent("adoptbot", &[r#"{"type":"result","result":"сделано поллингом"}"#], 0, false);
        let mut cfg = crate::testkit::test_config("adoptbot", tg.base());
        cfg.claude_bin = agent.display().to_string();
        cfg.allowed_users = [123_456_789, 7].into_iter().collect();
        // чат из прошлого запуска: сессия ведётся процессом, который мы не запускали
        let (mut child, script) = crate::testkit::adopted_agent("adopt", "ses-live");
        let mut state: HashMap<i64, ChatState> = HashMap::new();
        let mut st = ChatState::default();
        st.session_id = Some("ses-live".into());
        state.insert(11, st);
        // сессия есть, а процесса уже нет (смена кончилась, пока бот лежал)
        let mut gone = ChatState::default();
        gone.session_id = Some("ses-gone".into());
        state.insert(13, gone);
        // чат без сессии: искать в /proc нечего
        state.insert(14, ChatState::default());
        std::fs::write(&cfg.state_file, serde_json::to_string(&state).unwrap()).unwrap();

        let handle = tokio::spawn(run_bot(cfg, tg.bot()));
        // после сбоя getUpdates цикл спит 5 с — ждём с запасом
        for _ in 0..90 {
            if !tg.calls("sendMessage").is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        handle.abort();
        let _ = child.kill();
        let _ = std::fs::remove_file(&script);
        let sent = tg.calls("sendMessage");
        assert!(sent.iter().any(|s| s.contains("сделано поллингом")), "{sent:?}");
        assert!(tg.calls("getUpdates").len() >= 2, "сбой не обрывает поллинг");
        assert!(tg.calls("getMe").len() >= 1);
    }

    #[tokio::test]
    async fn a_shift_left_from_the_previous_run_is_awaited_then_resumed() {
        // бот перезапустился, а смену ведёт процесс от прежнего запуска: новую
        // задачу поднимаем только после его конца, в чате — одна строка о ожидании
        let (mut child, script) = crate::testkit::adopted_agent("wait", "ses-waiting");
        let agent = fake_agent("wait", &[r#"{"type":"result","result":"смена подхвачена"}"#], 0, false);
        let (app, tg) = e2e_with("wait", |c| {
            c.claude_bin = agent.display().to_string();
            c.status_bubble = true;
        });
        app.update_chat(11, |s| s.session_id = Some("ses-waiting".into())).await;
        let handle = tokio::spawn(run_agent(app.clone(), 11, "продолжай".into()));
        // ожидание длится, пока процесс смены жив
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(app.running.lock().await.contains_key(&11), "смена ждёт свою очередь");
        let _ = child.kill();
        let _ = std::fs::remove_file(&script);
        for _ in 0..60 {
            if tg.calls("sendMessage").iter().any(|s| s.contains("смена подхвачена")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        handle.await.unwrap();
        let all = tg.calls("sendMessage");
        assert!(all.iter().any(|s| s.contains("смена подхвачена")), "{all:?}");
        assert!(app.running.lock().await.is_empty(), "задача разобрана");
    }

    #[tokio::test]
    async fn status_command_describes_every_state() {
        let (app, tg) = e2e_with("status1", |_| {});
        app.update_chat(11, |s| {
            s.backend = Some(Backend::OpenCode);
            s.opencode_session_id = Some("ses-open-12345".into());
            s.resume_at = Some(chrono::Utc::now().timestamp() + 600);
            s.resume_label = Some("01.01 10:41 Europe/Moscow".into());
            s.fallback_from = Some(Backend::Claude);
        })
        .await;
        // задача идёт, ещё две ждут своей очереди, плюс незапущенное окно
        app.running.lock().await.insert(
            11,
            Running { cancel: Arc::new(Notify::new()), queue: vec!["а".into(), "б".into()] },
        );
        app.batches.lock().await.insert(
            11,
            Batch { texts: vec!["часть".into()], first_at: tokio::time::Instant::now(), gen: 1 },
        );
        handle_command(&app, 11, "/status", "").await;
        let text = tg.calls("sendMessage").last().unwrap().clone();
        assert!(text.contains("да, в очереди 2"), "{text}");
        assert!(text.contains("Ожидает окна агрегации: 1 сообщ."), "{text}");
        assert!(text.contains("Замещение: claude дожидается лимита"), "{text}");
        assert!(text.contains("Сессия (opencode): ses-open-12345"), "{text}");

        // задачу другого чата ведёт процесс от прежнего запуска бота
        let (mut child, script) = crate::testkit::adopted_agent("status", "ses-ghost");
        app.update_chat(12, |s| s.session_id = Some("ses-ghost".into())).await;
        handle_command(&app, 12, "/status", "").await;
        let text = tg.calls("sendMessage").last().unwrap().clone();
        let _ = child.kill();
        let _ = std::fs::remove_file(&script);
        assert!(text.contains("смена от прошлого запуска бота"), "{text}");

        // чат без задач и без сессии
        handle_command(&app, 13, "/status", "").await;
        let text = tg.calls("sendMessage").last().unwrap().clone();
        assert!(text.contains("Выполняется: нет"), "{text}");
        assert!(text.contains("Сессия (claude): нет"), "{text}");
        assert!(text.contains("Автопродолжение: нет"), "{text}");
    }

    #[tokio::test]
    async fn help_and_backend_commands_answer_every_variant() {
        // окно агрегации уже предельного срока — справка короче
        let (app, tg) = e2e_with("cmds", |c| {
            c.agg_quiet_secs = 3;
            c.agg_window_secs = 2;
        });
        handle_command(&app, 11, "/start", "").await;
        let text = tg.calls("sendMessage").last().unwrap().clone();
        assert!(text.contains("(2 с максимум)"), "{text}");

        let (app2, tg2) = e2e_with("cmds2", |c| {
            c.agg_quiet_secs = 4;
            c.agg_window_secs = 9;
        });
        handle_command(&app2, 11, "/help", "").await;
        let text = tg2.calls("sendMessage").last().unwrap().clone();
        assert!(text.contains("жду 4 с тишины, максимум 9 с"), "{text}");

        // текущий агент и переключение с отменой автовозврата
        handle_command(&app, 11, "/backend", "").await;
        assert!(tg.calls("sendMessage").last().unwrap().contains("Сейчас агент: claude"));
        app.update_chat(11, |s| {
            s.session_id = Some("ses-claude-1".into());
            s.fallback_from = Some(Backend::Claude);
            s.resume_label = Some("метка".into());
            s.handoff = Some("контекст".into());
        })
        .await;
        handle_command(&app, 11, "/backend", "opencode").await;
        let text = tg.calls("sendMessage").last().unwrap().clone();
        assert!(text.contains("Переключил на opencode"), "{text}");
        assert!(text.contains("Новое сообщение начнёт новую сессию opencode"), "{text}");
        assert!(text.contains("Автовозврат по лимиту отменён"), "{text}");
        let state = app.chat_state(11).await;
        assert_eq!(state.fallback_from, None, "ручной выбор снимает замещение");
        assert_eq!(state.resume_label, None);
        assert_eq!(state.handoff, None);

        // обратно: у обоих агентов есть свои сессии — говорим про продолжение
        app.update_chat(11, |s| s.opencode_session_id = Some("ses-open-12345".into())).await;
        handle_command(&app, 11, "/backend", "claude").await;
        let text = tg.calls("sendMessage").last().unwrap().clone();
        assert!(text.contains("Сессия claude продолжится: ses-claude-1"), "{text}");
        handle_command(&app, 11, "/backend", "opencode").await;
        let text = tg.calls("sendMessage").last().unwrap().clone();
        assert!(text.contains("Сессия opencode продолжится: ses-open-12345"), "{text}");

        // неизвестный агент — подсказка, а не падение
        handle_command(&app, 11, "/backend", "gpt").await;
        let text = tg.calls("sendMessage").last().unwrap().clone();
        assert!(text.contains("opencode"), "{text}");

        // /resume: замещение с основной сменой, просто автовозврат, плана нет
        app.update_chat(12, |s| {
            s.resume_label = Some("01.01 10:41".into());
            s.fallback_from = Some(Backend::Claude);
        })
        .await;
        handle_command(&app, 12, "/resume", "").await;
        assert!(tg.calls("sendMessage").last().unwrap().contains("Лимит claude исчерпан"));
        app.update_chat(13, |s| s.resume_label = Some("01.01 11:00".into())).await;
        handle_command(&app, 13, "/resume", "").await;
        assert!(tg.calls("sendMessage").last().unwrap().contains("Продолжу сам: 01.01 11:00"));
        handle_command(&app, 14, "/resume", "").await;
        assert!(tg.calls("sendMessage").last().unwrap().contains("Автопродолжение не запланировано"));
    }

    #[tokio::test]
    async fn cancel_clears_resume_queue_and_window() {
        let (app, tg) = e2e_with("cancel1", |_| {});
        // отмена автовозврата: ни задачи, ни очереди нет — только снятие плана
        app.update_chat(11, |s| {
            s.resume_at = Some(chrono::Utc::now().timestamp() + 300);
            s.resume_label = Some("метка".into());
        })
        .await;
        handle_command(&app, 11, "/cancel", "").await;
        let seen = tg.calls("sendMessage");
        assert!(seen.iter().any(|s| s.contains("Автопродолжение отменено")), "{seen:?}");
        assert_eq!(app.chat_state(11).await.resume_at, None);
        assert_eq!(app.chat_state(11).await.resume_label, None);

        // очередь текущей задачи и незапущенное окно агрегации
        app.running.lock().await.insert(
            12,
            Running { cancel: Arc::new(Notify::new()), queue: vec!["одна".into(), "вторая".into()] },
        );
        app.batches.lock().await.insert(
            12,
            Batch { texts: vec!["окно".into()], first_at: tokio::time::Instant::now(), gen: 7 },
        );
        handle_command(&app, 12, "/cancel", "").await;
        let seen = tg.calls("sendMessage");
        assert!(seen.iter().any(|s| s.contains("Очередь очищена (2 сообщ.)")), "{seen:?}");
        assert!(seen.iter().any(|s| s.contains("окно агрегации сброшено (1 сообщ.)")), "{seen:?}");

        // задача есть, но очередь пуста — отдельных строк не нужно
        app.running
            .lock()
            .await
            .insert(13, Running { cancel: Arc::new(Notify::new()), queue: Vec::new() });
        let before = tg.calls("sendMessage").len();
        handle_command(&app, 13, "/cancel", "").await;
        assert_eq!(tg.calls("sendMessage").len(), before, "пустую очередь не вспоминаем");

        // чат, где ничего не выполняется
        handle_command(&app, 14, "/cancel", "").await;
        assert!(tg.calls("sendMessage").last().unwrap().contains("Сейчас ничего не выполняется"));
    }

    #[tokio::test]
    async fn cwd_and_new_commands() {
        let (app, tg) = e2e("cwd2");
        handle_command(&app, 11, "/cwd", "").await;
        assert!(tg.calls("sendMessage").last().unwrap().contains("📁"));

        let dir = std::env::temp_dir().join("claudetg-cwd-target");
        std::fs::create_dir_all(&dir).unwrap();
        app.update_chat(11, |s| s.session_id = Some("ses-keep".into())).await;
        handle_command(&app, 11, "/cwd", &dir.display().to_string()).await;
        let text = tg.calls("sendMessage").last().unwrap().clone();
        assert!(text.contains("claudetg-cwd-target") && text.contains("Сессия сброшена"), "{text}");
        assert_eq!(app.chat_state(11).await.session_id, None, "смена сессии обнуляется");
        assert_eq!(app.chat_state(11).await.work_dir, Some(dir.clone()));

        handle_command(&app, 11, "/cwd", "/no/such/dir").await;
        assert!(tg.calls("sendMessage").last().unwrap().contains("Нет такой директории"));

        handle_command(&app, 11, "/new", "").await;
        assert!(tg.calls("sendMessage").last().unwrap().contains("начнёт новую сессию"));
    }

    #[test]
    fn home_shortcut_and_session_prefix() {
        // `~/...` в /cwd раскрывается домашним каталогом
        let home = std::env::var("HOME").expect("HOME");
        assert_eq!(shellexpand_home("~/отчёты"), format!("{home}/отчёты"));
        assert_eq!(shellexpand_home("/обычный/путь"), "/обычный/путь");
        // длинный id сессии в сообщении сокращается
        assert_eq!(short_session(&"x".repeat(30)).chars().count(), 18);
    }

    #[tokio::test]
    async fn attachments_are_named_by_kind_size_and_duplicates() {
        let (app, _tg) = e2e("media");
        // фото: тип по mime, размеры в пояснении
        let photo = message_of(&crate::testkit::update_media(1, 11, 7, "", "photo"));
        let saved = save_attachments(&app, 11, &photo).await;
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].kind, "изображение");
        let listed = list_files(&saved);
        assert!(listed.contains("изображение"), "{listed}");
        assert!(listed.contains("4 байт"), "{listed}");

        // то же вложение повторно: первую копию не затираем
        let again = save_attachments(&app, 11, &photo).await;
        assert_eq!(again.len(), 1);
        assert_ne!(again[0].local, saved[0].local, "имя получило счётчик");
        assert!(again[0].local.to_string_lossy().contains("-2"), "{:?}", again[0].local);

        // видео, аудио и документ — подписываются по-разному
        for (kind, want) in [("video", "видео"), ("audio", "аудио"), ("document", "документ")] {
            let msg = message_of(&crate::testkit::update_media(2, 11, 7, "", kind));
            let saved = save_attachments(&app, 11, &msg).await;
            assert_eq!(saved.len(), 1, "{kind}");
            assert_eq!(saved[0].kind, want, "{kind}");
        }

        // файл, которого на сервере уже нет: внятная строка в чат, ничего не скачано
        let (app, tg) = e2e("media2");
        let tg = tg.script("getFile", vec![r#"{"ok":false,"description":"bad file id"}"#.into()]);
        let msg = message_of(&crate::testkit::update_media(3, 11, 7, "подпись", "photo"));
        let saved = save_attachments(&app, 11, &msg).await;
        assert!(saved.is_empty(), "отказ Telegram не отдаётся как скачанный файл");
        let seen = tg.calls("sendMessage");
        assert!(seen.iter().any(|s| s.contains("Не скачал изображение")), "{seen:?}");
    }

    #[tokio::test]
    async fn a_file_without_a_caption_goes_to_the_agent_with_paths() {
        let agent = fake_agent(
            "fileonly",
            &[r#"{"type":"result","result":"файл разобрал"}"#],
            0,
            false,
        );
        let (app, tg) = e2e_with("fileonly", |c| c.claude_bin = agent.display().to_string());
        let msg = message_of(&crate::testkit::update_media(1, 11, 7, "", "document"));
        handle_message(app.clone(), msg).await;
        for _ in 0..40 {
            if tg.calls("sendMessage").iter().any(|s| s.contains("файл разобрал")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let prompt = crate::testkit::agent_prompt(&agent);
        assert!(prompt.contains("Пользователь прислал файл(ы)"), "{prompt}");
        assert!(prompt.contains("otchet.md"), "{prompt}");
        let sent = tg.calls("sendMessage");
        assert!(sent.iter().any(|s| s.contains("файл разобрал")), "{sent:?}");
    }

    #[tokio::test]
    async fn unwritable_state_outbox_and_log_are_reported_not_fatal() {
        // нет доступа к каталогу состояния и журнала — бот работает и ругается в лог
        let (app, _tg) = e2e_with("brokenpaths", |c| {
            c.state_file = PathBuf::from("/dev/null/nope/state.json");
            c.sent_log = PathBuf::from("/dev/null/nope/sent.jsonl");
        });
        app.update_chat(11, |_| {}).await;
        app.log_sent(11, &[1], Kind::Note, "заметка в несуществуемый журнал");

        // очередь недоступна — сообщение уходит сразу, а не теряется
        let (app2, tg2) = e2e_with("brokenoutbox", |c| {
            c.outbox_dir = PathBuf::from("/dev/null/nope/outbox");
        });
        app2.queue(11, Kind::Answer, "уйдёт напрямую").await;
        let seen = tg2.calls("sendMessage");
        assert!(seen.iter().any(|s| s.contains("уйдёт напрямую")), "{seen:?}");
    }

    #[tokio::test]
    async fn status_bubble_is_recreated_when_the_edit_is_refused() {
        let (app, tg) = e2e_with("bubble2", |c| c.status_bubble = true);
        let tg = tg.script("editMessageText", vec![r#"{"ok":false,"description":"message deleted"}"#.into()]);
        app.status(11, "Читаю файл: main.rs").await;
        app.status(11, "Ищу по коду").await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 2, "правка не вышла — пузырь создаётся заново: {sent:?}");
        assert!(sent[1].contains("Ищу по коду"), "{sent:?}");
    }

    #[tokio::test]
    async fn memory_of_recent_answers_stays_short() {
        let (app, _tg) = e2e("recentmem");
        for i in 1..=12 {
            // с паузой: у сообщений разные метки времени, порядок очереди ясен
            tokio::time::sleep(Duration::from_millis(5)).await;
            app.queue(11, Kind::Answer, &format!("ответ {i}")).await;
        }
        app.drain_outbox(11).await;
        let recent = app.recent_out.lock().await;
        let texts = recent.get(&11).expect("память о чате");
        assert_eq!(texts.len(), RECENT_SENDS, "память короткая: {texts:?}");
        assert!(texts.last().unwrap().contains("ответ 12"), "{texts:?}");
        assert!(!texts.iter().any(|t| t.contains("ответ 1 ") || t == "ответ 1"), "старое выброшено: {texts:?}");
    }

    /// Список аргументов подкоманды — как его собирает main.
    fn list(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

}
