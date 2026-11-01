mod claude;
mod config;
mod limit;
mod markdown;
mod telegram;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, Notify};

use config::Config;
use telegram::{Bot, Message};

/// Persisted per-chat state.
#[derive(Default, Clone, Serialize, Deserialize)]
struct ChatState {
    session_id: Option<String>,
    work_dir: Option<PathBuf>,
    /// Когда сам отправить «Продолжай» (unix-время, UTC): Claude упёрся в лимит.
    #[serde(default)]
    resume_at: Option<i64>,
    /// То же время для человека: «01.11 10:41 Europe/Moscow».
    #[serde(default)]
    resume_label: Option<String>,
}

struct App {
    cfg: Config,
    bot: Bot,
    chats: Mutex<HashMap<i64, ChatState>>,
    /// Claude run in progress, per chat.
    running: Mutex<HashMap<i64, Running>>,
}

struct Running {
    cancel: Arc<Notify>,
    /// Messages that arrived during the current turn; sent together as the next turn.
    queue: Vec<String>,
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
    let me = bot.get_me().await?;
    log::info!(
        "started as @{} (proxy: {})",
        me["username"].as_str().unwrap_or("?"),
        cfg.proxy_url.as_deref().unwrap_or("none")
    );

    let app = Arc::new(App {
        chats: Mutex::new(App::load_state(&cfg)),
        running: Mutex::new(HashMap::new()),
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

    run_claude(app, chat_id, text).await;
}

async fn handle_command(app: &Arc<App>, chat_id: i64, cmd: &str, arg: &str) {
    match cmd {
        "/start" | "/help" => {
            app.bot
                .send_text(
                    chat_id,
                    "Пиши задачу — я передам её Claude Code и пришлю ответ.\n\n\
                     /new — начать новую сессию\n\
                     /cancel — прервать выполнение, очистить очередь и автопродолжение\n\
                     /resume — когда продолжу сам после лимита\n\
                     /cwd [путь] — показать/сменить рабочую директорию (сбрасывает сессию)\n\
                     /status — текущая сессия и директория",
                )
                .await;
        }
        "/resume" => {
            let state = app.chat_state(chat_id).await;
            match state.resume_label {
                Some(label) => {
                    app.bot
                        .send_text(chat_id, &format!("⏳ Продолжу сам: {label}. /cancel — отменить."))
                        .await
                }
                None => app.bot.send_text(chat_id, "Автопродолжение не запланировано.").await,
            }
        }
        "/new" => {
            app.update_chat(chat_id, |s| s.session_id = None).await;
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
                        s.session_id = None;
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
            let queued = app.running.lock().await.get(&chat_id).map(|r| r.queue.len());
            let text = format!(
                "Сессия: {}\nДиректория: {}\nВыполняется: {}\nАвтопродолжение: {}",
                state.session_id.as_deref().unwrap_or("нет"),
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
            // Unknown commands (e.g. /compact) are passed to Claude as-is.
            let text = if arg.is_empty() { cmd.to_string() } else { format!("{cmd} {arg}") };
            run_claude(app.clone(), chat_id, text).await;
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

/// Starts a claude turn, or queues the prompt if one is already running for
/// this chat. Queued messages are sent together as one turn after the current
/// one finishes.
async fn run_claude(app: Arc<App>, chat_id: i64, prompt: String) {
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
        run_turn(&app, chat_id, prompt, cancel).await;

        let mut running = app.running.lock().await;
        let Some(run) = running.get_mut(&chat_id) else { return };
        if run.queue.is_empty() {
            running.remove(&chat_id);
            return;
        }
        prompt = std::mem::take(&mut run.queue).join("\n\n");
        cancel = Arc::new(Notify::new());
        run.cancel = cancel.clone();
    }
}

async fn run_turn(app: &Arc<App>, chat_id: i64, prompt: String, cancel: Arc<Notify>) {
    let state = app.chat_state(chat_id).await;
    let dir = work_dir(app, chat_id).await;
    let (tx, mut rx) = mpsc::unbounded_channel();

    let run = {
        let app = app.clone();
        tokio::spawn(async move {
            claude::run(&app.cfg, &dir, state.session_id.as_deref(), &prompt, tx, cancel).await
        })
    };

    // While claude works: keep the "typing…" indicator alive and batch tool
    // notifications into one message every few seconds.
    let mut ticker = tokio::time::interval(Duration::from_secs(4));
    let mut progress: Vec<String> = Vec::new();
    loop {
        tokio::select! {
            ev = rx.recv() => match ev {
                Some(claude::Event::Progress(p)) => progress.push(p),
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
                app.update_chat(chat_id, |s| s.session_id = Some(id)).await;
            }
            let mut text = outcome.text;
            if outcome.is_error && !text.starts_with('⛔') {
                text = format!("⚠️ {text}");
            }
            if let Some(cost) = outcome.cost_usd {
                log::info!("chat {chat_id}: turn finished, cost ${cost:.4}");
            }
            app.bot.send_markdown(chat_id, &text).await;
            schedule_resume_if_limited(app, chat_id, &text).await;
        }
        Ok(Err(e)) => app.bot.send_text(chat_id, &format!("⚠️ Ошибка запуска claude: {e:#}")).await,
        Err(e) => app.bot.send_text(chat_id, &format!("⚠️ Внутренняя ошибка: {e}")).await,
    }
}

/// Claude упёрся в лимит — запомнить, когда продолжить, и сказать об этом.
async fn schedule_resume_if_limited(app: &Arc<App>, chat_id: i64, text: &str) {
    let Some((at, label)) = limit::resume_at(text, chrono::Utc::now()) else { return };
    let shown = label.clone();
    app.update_chat(chat_id, |s| {
        s.resume_at = Some(at.timestamp());
        s.resume_label = Some(label);
    })
    .await;
    log::info!("chat {chat_id}: limit hit, resume scheduled at {at}");
    app.bot
        .send_text(
            chat_id,
            &format!("⏳ Лимит исчерпан. Продолжу сам в {shown} — отправлю «Продолжай». /cancel — отменить."),
        )
        .await;
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
            app.update_chat(chat_id, |s| {
                s.resume_at = None;
                s.resume_label = None;
            })
            .await;
            log::info!("chat {chat_id}: limit reset, resuming");
            app.bot.send_text(chat_id, "⏰ Лимит обновился — продолжаю.").await;
            let app = app.clone();
            tokio::spawn(async move { run_claude(app, chat_id, "Продолжай".to_string()).await });
        }
    }
}
