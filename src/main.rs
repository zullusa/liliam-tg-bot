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
        let mine: Vec<(PathBuf, Item)> =
            all.into_iter().filter(|(_, it)| it.chat == chat_id).collect();
        if mine.is_empty() {
            return;
        }
        // Файлы очереди убираем только после отправки: сетевой сбой не должен
        // съедать сообщение — оно дойдёт, когда связь вернётся.
        let mut kept: Vec<String> = Vec::new();
        let mut kind = Kind::Agent;
        for (_, item) in &mine {
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
            outbox::discard(&mine.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>());
            return;
        }
        let merged = outbox::merge(&kept);
        match self.bot.send_markdown_ids(chat_id, &merged).await {
            Ok(ids) => {
                outbox::discard(&mine.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>());
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
    let bot = Bot::new(&cfg.bot_token, cfg.proxy_url.as_deref())?;

    // `claudetg send <chat_id> < file.md` — поставить Markdown в очередь чата и
    // выйти: живой бот отправит её пачкой вместе с остальными сообщениями шага.
    // Если бот не работает, отправляем сами — сообщение не должно пропасть.
    if args.get(1).map(String::as_str) == Some("send") {
        let chat_id: i64 = args.get(2).and_then(|s| s.parse().ok()).ok_or_else(|| {
            anyhow::anyhow!("usage: claudetg send <chat_id> < message.md")
        })?;
        let mut text = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut text)?;
        if outbox::bot_alive(&cfg.pid_file) {
            outbox::enqueue(&cfg.outbox_dir, &Item::new(chat_id, Kind::Agent, text))?;
            log::info!("сообщение поставлено в очередь: {}", cfg.outbox_dir.display());
        } else {
            log::warn!("бот не работает ({}), отправляем напрямую", cfg.pid_file.display());
            bot.send_markdown(chat_id, &text).await;
        }
        return Ok(());
    }
    // `claudetg clean [--chat <id>] [--all] [--keep N] [--dry]` — убрать из чата
    // свои сообщения по журналу отправленного.
    if args.get(1).map(String::as_str) == Some("clean") {
        return clean_messages(&bot, &cfg, &args[2..]).await;
    }
    // `claudetg send-file <path> [подпись]` — прислать файл документом в чат
    // CLAUDETG_CHAT_ID (бот выставляет его процессу claude) или --chat <id>.
    if args.get(1).map(String::as_str) == Some("send-file") {
        let mut rest: Vec<String> = args[2..].to_vec();
        let mut chat = std::env::var("CLAUDETG_CHAT_ID").ok();
        if let Some(i) = rest.iter().position(|a| a == "--chat") {
            chat = rest.get(i + 1).cloned();
            rest.drain(i..(i + 2).min(rest.len()));
        }
        let usage = "usage: claudetg send-file [--chat <id>] <path> [caption]";
        let chat_id: i64 = chat
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| anyhow::anyhow!("{usage}: chat id not set (CLAUDETG_CHAT_ID)"))?;
        let path = rest.first().ok_or_else(|| anyhow::anyhow!("{usage}"))?;
        let caption = rest.get(1).map(String::as_str);
        bot.send_document(chat_id, std::path::Path::new(path), caption).await?;
        return Ok(());
    }
    // Неизвестная подкоманда не должна тихо поднимать второй поллинг-бот:
    // он вступает в конфликт с сервисом и забирает сообщения у него.
    match args.get(1).map(String::as_str) {
        None => {}
        Some("run") | Some("start") | Some("poll") => {}
        Some(other) => {
            anyhow::bail!(
                "неизвестная подкоманда {other:?}; доступно: run (по умолчанию), send, send-file, clean"
            );
        }
    }
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
        recent_out: Mutex::new(HashMap::new()),
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
        let updates = match app.bot.get_updates(offset).await {
            Ok(u) => u,
            Err(e) => {
                log::warn!("getUpdates: {e:#}");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        for update in updates {
            offset = update.update_id + 1;
            if let Some(msg) = update.message {
                let app = app.clone();
                tokio::spawn(async move { handle_message(app, msg).await });
            }
        }
    }
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
        if !wait_adopted_agent(&app, chat_id, Duration::from_secs(900)).await {
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
    loop {
        ticker.tick().await;
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
    let mut chat = std::env::var("CLAUDETG_CHAT_ID").ok();
    if let Some(i) = args.iter().position(|a| a == "--chat") {
        chat = args.get(i + 1).cloned();
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
        Ok(Err(e)) => app
            .bot
            .send_text(chat_id, &format!("⚠️ Ошибка запуска {}: {e:#}", backend.name()))
            .await,
        Err(e) => app.bot.send_text(chat_id, &format!("⚠️ Внутренняя ошибка: {e}")).await,
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
        let now = chrono::Utc::now().timestamp();
        let due: Vec<i64> = app
            .chats
            .lock()
            .await
            .iter()
            .filter(|(_, s)| s.resume_at.is_some_and(|at| at <= now))
            .map(|(id, _)| *id)
            .collect();
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
            tokio::spawn(async move { run_agent(app, chat_id, "Продолжай".to_string()).await });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
