mod backend;
mod claude;
mod config;
mod handoff;
mod limit;
mod markdown;
mod opencode;
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cfg = Config::from_env()?;
    let bot = Bot::new(&cfg.bot_token, cfg.proxy_url.as_deref())?;

    // `claudetg send <chat_id> < file.md` — отправить Markdown из stdin в чат и
    // выйти, без опроса обновлений: проверка разметки и разовые сообщения.
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("send") {
        let chat_id: i64 = args.get(2).and_then(|s| s.parse().ok()).ok_or_else(|| {
            anyhow::anyhow!("usage: claudetg send <chat_id> < message.md")
        })?;
        let mut text = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut text)?;
        bot.send_markdown(chat_id, &text).await;
        return Ok(());
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
        bot,
        cfg,
    });

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
    let Some(text) = msg.text.or(msg.caption) else {
        app.bot.send_text(chat_id, "Пока понимаю только текст.").await;
        return;
    };
    let text = text.trim().to_string();

    if text.starts_with('/') {
        let (cmd, arg) = text.split_once(char::is_whitespace).unwrap_or((&text, ""));
        let cmd = cmd.split('@').next().unwrap_or(cmd);
        handle_command(&app, chat_id, cmd, arg.trim()).await;
        return;
    }

    submit_text(&app, chat_id, text).await;
}

/// Принять текстовое сообщение: одинаковые подряд схлопываем в одно, остальные
/// копим окном агрегации — человек печатает задачу частями, запускаем одной
/// задачей после тишины (но не дольше предельного окна от первой части).
async fn submit_text(app: &Arc<App>, chat_id: i64, text: String) {
    if app.last_text.lock().await.get(&chat_id).map(String::as_str) == Some(text.as_str()) {
        app.bot
            .send_text(chat_id, "🔁 Такой же текст уже принят — вторым сообщением его не повторяю.")
            .await;
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
        app.bot
            .send_text(
                chat_id,
                &format!(
                    "🕐 Задача принята. Можно дополнять ещё {quiet} с после каждого сообщения \
                     (всего не дольше {cap} с) — запущу всё одной задачей.",
                ),
            )
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
            app.bot
                .send_text(
                    chat_id,
                    &format!(
                        "Пиши задачу — я передам её агенту и пришлю ответ.\n\n{agg}\
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
                    None => "нет".to_string(),
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
            app.bot
                .send_text(chat_id, &format!("📥 В очереди ({n}). Отправлю после текущей задачи; /cancel — прервать и очистить."))
                .await;
            return;
        }
        running.insert(chat_id, Running { cancel: cancel.clone(), queue: Vec::new() });
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
            return;
        }
        let queued = std::mem::take(&mut run.queue);
        cancel = Arc::new(Notify::new());
        run.cancel = cancel.clone();
        let lines = queued
            .iter()
            .map(|t| format!("▶️ Следующая задача из очереди: {}", brief(t, 120)))
            .collect::<Vec<_>>()
            .join("\n");
        drop(running);
        app.bot.send_text(chat_id, &lines).await;
        prompt = queued.join("\n\n");
    }
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

    // While the agent works: keep the "typing…" indicator alive and batch tool
    // notifications into one message every few seconds.
    let mut ticker = tokio::time::interval(Duration::from_secs(4));
    let mut progress: Vec<String> = Vec::new();
    loop {
        tokio::select! {
            ev = rx.recv() => match ev {
                Some(Event::Progress(p)) => progress.push(p),
                None => break,
            },
            _ = ticker.tick() => {
                if app.cfg.show_tools && !progress.is_empty() {
                    app.bot.send_text(chat_id, &progress.join("\n")).await;
                    progress.clear();
                }
                app.bot.send_typing(chat_id).await;
            }
        }
    }
    if app.cfg.show_tools && !progress.is_empty() {
        app.bot.send_text(chat_id, &progress.join("\n")).await;
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
            app.bot.send_markdown(chat_id, &text).await;
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
