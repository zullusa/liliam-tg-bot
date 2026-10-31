use anyhow::{Context, Result};
use serde_json::Value;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, Notify};

use crate::config::Config;

const SYSTEM_PROMPT: &str = "You are talking with the user through a Telegram bot. \
The user cannot see your tool calls, only your final text answer, which is shown as plain text \
(markdown is not rendered, so keep formatting simple). \
Interactive tools that wait for user input are not available. If you need a decision or \
clarification from the user, ask the question in your final answer and stop; the user's \
reply will arrive as the next message in this same session. \
Do not put shell commands, command lines, flags or code blocks into your answers: describe in \
plain words what you did, why, and what came of it. Mention a file name only when it is needed \
to understand the answer.";

pub enum Event {
    /// Short progress note, e.g. which tool Claude is running.
    Progress(String),
}

pub struct Outcome {
    pub session_id: Option<String>,
    pub text: String,
    pub is_error: bool,
    pub cost_usd: Option<f64>,
}

/// Runs one `claude -p` turn, resuming `session_id` if given.
/// Progress events are sent to `events`; `cancel` aborts the run.
pub async fn run(
    cfg: &Config,
    work_dir: &Path,
    session_id: Option<&str>,
    prompt: &str,
    events: mpsc::UnboundedSender<Event>,
    cancel: Arc<Notify>,
) -> Result<Outcome> {
    let mut cmd = Command::new(&cfg.claude_bin);
    cmd.arg("-p")
        .args(["--output-format", "stream-json", "--verbose"])
        .args(["--permission-mode", &cfg.permission_mode])
        .args(["--append-system-prompt", SYSTEM_PROMPT]);
    if let Some(model) = &cfg.model {
        cmd.args(["--model", model]);
    }
    if let Some(id) = session_id {
        cmd.args(["--resume", id]);
    }
    cmd.args(&cfg.extra_args)
        .current_dir(work_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", cfg.claude_bin))?;

    // Prompt goes through stdin so it is never mistaken for a CLI flag.
    let mut stdin = child.stdin.take().context("no stdin")?;
    stdin.write_all(prompt.as_bytes()).await?;
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
    let mut last_assistant_text = String::new();
    let mut got_result = false;

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
            log::debug!("non-json line from claude: {line}");
            continue;
        };
        if let Some(id) = v.get("session_id").and_then(Value::as_str) {
            outcome.session_id = Some(id.to_string());
        }
        match v.get("type").and_then(Value::as_str) {
            Some("assistant") => {
                let content = v.pointer("/message/content").and_then(Value::as_array);
                for item in content.into_iter().flatten() {
                    match item.get("type").and_then(Value::as_str) {
                        Some("text") => {
                            if let Some(t) = item.get("text").and_then(Value::as_str) {
                                last_assistant_text = t.to_string();
                            }
                        }
                        Some("tool_use") => {
                            let _ = events.send(Event::Progress(describe_tool(item)));
                        }
                        _ => {}
                    }
                }
            }
            Some("result") => {
                got_result = true;
                outcome.is_error = v.get("is_error").and_then(Value::as_bool).unwrap_or(false);
                outcome.cost_usd = v.get("total_cost_usd").and_then(Value::as_f64);
                outcome.text = v
                    .get("result")
                    .and_then(Value::as_str)
                    .map(String::from)
                    .unwrap_or_else(|| {
                        let sub = v.get("subtype").and_then(Value::as_str).unwrap_or("unknown");
                        format!("Сессия завершилась без ответа ({sub}).")
                    });
            }
            _ => {}
        }
    }

    let status = child.wait().await?;
    let stderr = stderr_task.await.unwrap_or_default();
    if !got_result {
        outcome.is_error = true;
        outcome.text = if !last_assistant_text.is_empty() {
            last_assistant_text
        } else {
            format!("claude завершился с {status}:\n{}", stderr.trim())
        };
    }
    Ok(outcome)
}

/// Человекочитаемая строка о том, что сейчас делает Claude.
///
/// Сырой ввод инструмента (текст команды, регулярное выражение) в Telegram
/// не показывается: человеку нужно «что происходит», а не командная строка.
fn describe_tool(item: &Value) -> String {
    let name = item.get("name").and_then(Value::as_str).unwrap_or("");
    let input = item.get("input").cloned().unwrap_or(Value::Null);
    let field = |key: &str| -> String {
        let raw = input.get(key).and_then(Value::as_str).unwrap_or("");
        raw.lines().next().unwrap_or("").chars().take(150).collect()
    };
    let file = |key: &str| -> String {
        let path = field(key);
        Path::new(&path)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or(path)
    };
    let with = |icon: &str, text: &str, detail: String| -> String {
        if detail.is_empty() {
            format!("{icon} {text}")
        } else {
            format!("{icon} {text}: {detail}")
        }
    };

    match name {
        // У команды есть поле `description` — короткое объяснение словами.
        // Саму команду не показываем никогда.
        "Bash" => {
            let description = field("description");
            if description.is_empty() {
                "⚙️ Выполняю команду в терминале".into()
            } else {
                format!("⚙️ {description}")
            }
        }
        "Read" => with("📖", "Читаю файл", file("file_path")),
        "Write" => with("📝", "Создаю файл", file("file_path")),
        "Edit" | "MultiEdit" => with("✏️", "Правлю файл", file("file_path")),
        "NotebookEdit" => with("✏️", "Правлю блокнот", file("notebook_path")),
        "Grep" | "Glob" => "🔍 Ищу по коду".into(),
        "WebSearch" => with("🌐", "Ищу в интернете", field("query")),
        "WebFetch" => {
            let url = field("url");
            let host = url.split("://").nth(1).unwrap_or(&url).split('/').next().unwrap_or("");
            with("🌐", "Открываю страницу", host.to_string())
        }
        "Task" | "Agent" => with("🤖", "Поручаю часть работы помощнику", field("description")),
        "TodoWrite" => "🗒 Обновляю план работы".into(),
        "Skill" => "📚 Загружаю инструкцию для задачи".into(),
        "ToolSearch" => "🧰 Подключаю нужный инструмент".into(),
        _ => {
            let description = field("description");
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
        let item = json!({"name": "Bash", "input": {
            "command": "rm -rf /tmp/x && cargo build", "description": "Собираю проект"}});
        assert_eq!(describe_tool(&item), "⚙️ Собираю проект");
        let bare = json!({"name": "Bash", "input": {"command": "ls -la"}});
        assert_eq!(describe_tool(&bare), "⚙️ Выполняю команду в терминале");
    }

    #[test]
    fn files_are_named_without_the_path() {
        let item = json!({"name": "Edit", "input": {"file_path": "/home/u/proj/src/main.rs"}});
        assert_eq!(describe_tool(&item), "✏️ Правлю файл: main.rs");
    }

    #[test]
    fn search_patterns_and_unknown_tools_stay_plain() {
        let grep = json!({"name": "Grep", "input": {"pattern": "fn\\s+main"}});
        assert_eq!(describe_tool(&grep), "🔍 Ищу по коду");
        let other = json!({"name": "mcp__x__y", "input": {"query": "select * from t"}});
        assert_eq!(describe_tool(&other), "🔧 Работаю над задачей");
        let page = json!({"name": "WebFetch", "input": {"url": "https://docs.rs/tokio/latest"}});
        assert_eq!(describe_tool(&page), "🌐 Открываю страницу: docs.rs");
    }
}
