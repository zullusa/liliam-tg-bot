use anyhow::{Context, Result};
use serde_json::Value;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, Notify};

use crate::backend::{file_name, with_detail, Event, Outcome, SYSTEM_PROMPT};
use crate::config::Config;

/// Runs one `opencode run --format json` turn, continuing `session_id` if given.
/// Progress events are sent to `events`; `cancel` aborts the run.
///
/// Unlike `claude -p`, `opencode run` has no system-prompt flag, so the bot
/// instructions are prefixed to the message on every turn.
pub async fn run(
    cfg: &Config,
    work_dir: &std::path::Path,
    chat_id: i64,
    session_id: Option<&str>,
    prompt: &str,
    events: mpsc::UnboundedSender<Event>,
    cancel: Arc<Notify>,
) -> Result<Outcome> {
    let mut cmd = Command::new(&cfg.opencode_bin);
    cmd.arg("run").args(["--format", "json"]);
    if cfg.opencode_auto {
        cmd.arg("--auto");
    }
    if let Some(model) = &cfg.opencode_model {
        cmd.args(["-m", model]);
    }
    if let Some(agent) = &cfg.opencode_agent {
        cmd.args(["--agent", agent]);
    }
    if let Some(id) = session_id {
        cmd.args(["--session", id]);
    }
    cmd.args(&cfg.opencode_extra_args)
        .current_dir(work_dir)
        // Чат, из которого пришёл запрос: `claudetg send-file` шлёт файл туда.
        .env("CLAUDETG_CHAT_ID", chat_id.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", cfg.opencode_bin))?;

    // Prompt goes through stdin so it is never mistaken for a CLI flag.
    let mut stdin = child.stdin.take().context("no stdin")?;
    let full_prompt = format!("{SYSTEM_PROMPT}\n\n---\n{prompt}");
    stdin.write_all(full_prompt.as_bytes()).await?;
    drop(stdin);

    let stderr = child.stderr.take().context("no stderr")?;
    let stderr_task = tokio::spawn(async move {
        let mut buf = String::new();
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            buf.push_str(&line);
            buf.push('\n');
        }
        buf
    });

    let mut lines = BufReader::new(child.stdout.take().context("no stdout")?).lines();
    let mut outcome = Outcome { session_id: session_id.map(String::from), text: String::new(), is_error: false, cost_usd: None };
    let mut last_text = String::new();
    let mut error_text = String::new();
    let mut cost_total = 0.0;
    let mut saw_cost = false;

    loop {
        let line = tokio::select! {
            l = lines.next_line() => l?,
            _ = cancel.notified() => {
                let _ = child.kill().await;
                outcome.text = "⛔ Выполнение прервано.".into();
                outcome.is_error = true;
                return Ok(outcome);
            }
        };
        let Some(line) = line else { break };
        let Ok(v) = serde_json::from_str::<Value>(&line) else {
            log::debug!("non-json line from opencode: {line}");
            continue;
        };
        if let Some(id) = v.get("sessionID").and_then(Value::as_str) {
            outcome.session_id = Some(id.to_string());
        }
        match v.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(t) = v.pointer("/part/text").and_then(Value::as_str) {
                    last_text = t.to_string();
                }
            }
            Some("tool_use") => {
                let _ = events.send(Event::Progress(describe_tool(v.get("part").cloned().unwrap_or(Value::Null))));
            }
            Some("step_finish") => {
                if let Some(c) = v.pointer("/part/cost").and_then(Value::as_f64) {
                    cost_total += c;
                    saw_cost = true;
                }
            }
            Some("error") => {
                let kind = v.pointer("/error/type").and_then(Value::as_str).unwrap_or("error");
                let msg = v.pointer("/error/message").and_then(Value::as_str).unwrap_or("unknown error");
                error_text = if kind == "error" { msg.to_string() } else { format!("{kind}: {msg}") };
            }
            _ => {}
        }
    }

    let status = child.wait().await?;
    let stderr = stderr_task.await.unwrap_or_default();
    if saw_cost {
        outcome.cost_usd = Some(cost_total);
    }
    if !error_text.is_empty() {
        outcome.is_error = true;
        // Сообщение о лимите приходит в event error — для автопродолжения
        // нужен именно оно, даже если до этого был какой-то текст.
        outcome.text = if last_text.is_empty() || error_text.contains("limit") {
            error_text
        } else {
            last_text
        };
    } else if !last_text.is_empty() {
        outcome.text = last_text;
    } else if status.success() {
        outcome.is_error = true;
        outcome.text = "Сессия завершилась без ответа.".into();
    } else {
        outcome.is_error = true;
        outcome.text = format!("opencode завершился с {status}:\n{}", stderr.trim());
    }
    Ok(outcome)
}

/// Человекочитаемая строка о том, что сейчас делает OpenCode.
///
/// Сырой ввод инструмента (текст команды, регулярное выражение) в Telegram
/// не показывается: человеку нужно «что происходит», а не командная строка.
/// Имена инструментов OpenCode — строчные (bash, write, edit…), в отличие от
/// Claude (Bash, Write, Edit…).
fn describe_tool(part: Value) -> String {
    let name = part.get("tool").and_then(Value::as_str).unwrap_or("").to_lowercase();
    let input = part
        .pointer("/state/input")
        .cloned()
        .unwrap_or(Value::Null);
    let field = |keys: &[&str]| -> String {
        keys.iter()
            .find_map(|k| input.get(*k).and_then(Value::as_str))
            .unwrap_or("")
            .lines()
            .next()
            .unwrap_or("")
            .chars()
            .take(150)
            .collect()
    };
    let file = |keys: &[&str]| file_name(&field(keys));
    // Поля путей у opencode — camelCase (filePath), на всякий случай принимаем и snake_case.
    const PATH_KEYS: &[&str] = &["filePath", "file_path", "path"];

    match name.as_str() {
        // У bash есть поле `description` — короткое объяснение словами.
        // Саму команду не показываем никогда.
        "bash" | "shell" => {
            let description = field(&["description"]);
            if description.is_empty() {
                "⚙️ Выполняю команду в терминале".into()
            } else {
                format!("⚙️ {description}")
            }
        }
        "read" => with_detail("📖", "Читаю файл", file(PATH_KEYS)),
        "write" => with_detail("📝", "Создаю файл", file(PATH_KEYS)),
        "edit" | "multiedit" | "patch" | "apply_patch" => {
            with_detail("✏️", "Правлю файл", file(PATH_KEYS))
        }
        "grep" | "glob" => "🔍 Ищу по коду".into(),
        "websearch" => with_detail("🌐", "Ищу в интернете", field(&["query"])),
        "webfetch" => {
            let url = field(&["url"]);
            let host = url.split("://").nth(1).unwrap_or(&url).split('/').next().unwrap_or("");
            with_detail("🌐", "Открываю страницу", host.to_string())
        }
        "task" | "agent" | "subtask" => {
            with_detail("🤖", "Поручаю часть работы помощнику", field(&["description"]))
        }
        "todowrite" | "todo" => "🗒 Обновляю план работы".into(),
        "skill" => "📚 Загружаю инструкцию для задачи".into(),
        _ => {
            let description = field(&["description"]);
            if description.is_empty() {
                "🔧 Работаю над задачей".into()
            } else {
                format!("🔧 {description}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::describe_tool;
    use serde_json::json;
    #[test]
    fn command_text_is_never_shown() {
        let item = json!({"tool": "bash", "state": {"status": "completed", "input": {
            "command": "rm -rf /tmp/x && cargo build", "description": "Собираю проект"}}});
        assert_eq!(describe_tool(item), "⚙️ Собираю проект");
        let bare = json!({"tool": "bash", "state": {"input": {"command": "ls -la"}}});
        assert_eq!(describe_tool(bare), "⚙️ Выполняю команду в терминале");
    }

    #[test]
    fn files_are_named_without_the_path() {
        let item = json!({"tool": "edit", "state": {"input": {"filePath": "/home/u/proj/src/main.rs"}}});
        assert_eq!(describe_tool(item), "✏️ Правлю файл: main.rs");
        let write = json!({"tool": "write", "state": {"input": {"filePath": "/tmp/hello.txt"}}});
        assert_eq!(describe_tool(write), "📝 Создаю файл: hello.txt");
    }

    #[test]
    fn search_and_unknown_tools_stay_plain() {
        let grep = json!({"tool": "grep", "state": {"input": {"pattern": "fn\\s+main"}}});
        assert_eq!(describe_tool(grep), "🔍 Ищу по коду");
        let other = json!({"tool": "some_mcp", "state": {"input": {"query": "select * from t"}}});
        assert_eq!(describe_tool(other), "🔧 Работаю над задачей");
        let page = json!({"tool": "webfetch", "state": {"input": {"url": "https://opencode.ai/docs"}}});
        assert_eq!(describe_tool(page), "🌐 Открываю страницу: opencode.ai");
    }

    /// Живая проверка формата `opencode run --format json`: запуск настоящего
    /// opencode, два хода в одной сессии, продолжение по sessionID.
    /// `cargo test -- --ignored` — нужен установленный opencode с настроенным провайдером.
    #[ignore = "нужен установленный opencode и настроенный провайдер"]
    #[tokio::test]
    async fn live_turn_resumes_session() {
        use super::run;
        use crate::backend::{Backend, Event};
        use crate::config::Config;
        use std::sync::Arc;
        use tokio::sync::{mpsc, Notify};

        let cfg = Config {
            bot_token: String::new(),
            allowed_users: Default::default(),
            proxy_url: None,
            backend: Backend::OpenCode,
            claude_bin: "claude".into(),
            work_dir: std::env::temp_dir(),
            permission_mode: "auto".into(),
            model: None,
            extra_args: Vec::new(),
            opencode_bin: "opencode".into(),
            opencode_model: Some("strata/qwen3.8-flash-next-iq3_s".into()),
            opencode_agent: None,
            opencode_auto: true,
            opencode_extra_args: Vec::new(),
            show_tools: true,
            state_file: "/dev/null".into(),
            limit_fallback: false,
            claude_projects_dir: "/nonexistent".into(),
            agg_quiet_secs: 0,
            agg_window_secs: 0,
        };
        let dir = cfg.work_dir.join("claudetg-smoke");
        std::fs::create_dir_all(&dir).unwrap();

        let (tx, _rx) = mpsc::unbounded_channel::<Event>();
        let first = run(
            &cfg,
            &dir,
            0,
            None,
            "Запомни кодовое слово «ОРАНЖЕВОЕ». Ответь ровно: ОК",
            tx,
            Arc::new(Notify::new()),
        )
        .await
        .unwrap();
        assert!(!first.is_error, "первый ход: {}", first.text);
        assert!(first.text.contains("ОК"), "первый ход: {}", first.text);
        let session = first.session_id.expect("sessionID в потоке json");
        assert!(session.starts_with("ses_"), "sessionID: {session}");

        let (tx2, _rx2) = mpsc::unbounded_channel::<Event>();
        let second = run(
            &cfg,
            &dir,
            0,
            Some(&session),
            "Какое кодовое слово я просил запомнить? Ответь одним словом.",
            tx2,
            Arc::new(Notify::new()),
        )
        .await
        .unwrap();
        assert!(!second.is_error, "второй ход: {}", second.text);
        assert!(
            second.text.to_uppercase().contains("ОРАНЖЕВОЕ") || second.text.contains("Оранжево"),
            "сессия не продолжилась: {}",
            second.text
        );
    }
}
