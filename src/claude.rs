use anyhow::{Context, Result};
use serde_json::Value;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, Notify};

use crate::backend::{file_name, with_detail, Event, Outcome, SYSTEM_PROMPT};
use crate::config::Config;

/// Runs one `claude -p` turn, resuming `session_id` if given.
/// Progress events are sent to `events`; `cancel` aborts the run.
pub async fn run(
    cfg: &Config,
    work_dir: &Path,
    chat_id: i64,
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
        // Чат, из которого пришёл запрос: `claudetg send-file` шлёт файл туда.
        .env("CLAUDETG_CHAT_ID", chat_id.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", cfg.claude_bin))?;

    // Prompt goes through stdin so it is never mistaken for a CLI flag.
    // Если агент успел завершиться до чтения запроса, письмо вернёт «Broken
    // pipe»: это не повод бросать разбор — stderr и код выхода скажут, что
    // случилось, и человек получит внятную строку вместо тишины.
    let mut stdin = child.stdin.take().context("no stdin")?;
    if let Err(e) = stdin.write_all(prompt.as_bytes()).await {
        log::debug!("chat {chat_id}: запрос не вошёл агенту в stdin ({e:#}) — читаем вывод");
    }
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
    let mut outcome = Outcome { session_id: session_id.map(String::from), text: String::new(), is_error: false, cost_usd: None, api_error_code: None };
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
                outcome.api_error_code = v.get("api_error_code").and_then(Value::as_str).map(String::from);
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
    let file = |key: &str| -> String { file_name(&field(key)) };

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
        "Read" => with_detail("📖", "Читаю файл", file("file_path")),
        "Write" => with_detail("📝", "Создаю файл", file("file_path")),
        "Edit" | "MultiEdit" => with_detail("✏️", "Правлю файл", file("file_path")),
        "NotebookEdit" => with_detail("✏️", "Правлю блокнот", file("notebook_path")),
        "Grep" | "Glob" => "🔍 Ищу по коду".into(),
        "WebSearch" => with_detail("🌐", "Ищу в интернете", field("query")),
        "WebFetch" => {
            let url = field("url");
            let host = url.split("://").nth(1).unwrap_or(&url).split('/').next().unwrap_or("");
            with_detail("🌐", "Открываю страницу", host.to_string())
        }
        "Task" | "Agent" => with_detail("🤖", "Поручаю часть работы помощнику", field("description")),
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
    fn custom_tools_are_named_by_their_description() {
        let tool = json!({"name": "mcp__kb__search", "input": {"description": "ищу по базе знаний"}});
        assert_eq!(describe_tool(&tool), "🔧 ищу по базе знаний");
        let bare = json!({"name": "mcp__kb__search", "input": {}});
        assert_eq!(describe_tool(&bare), "🔧 Работаю над задачей");
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

    // ------- Проверки разбора потока `claude -p --output-format stream-json` -------
    #[tokio::test]
    async fn model_flag_extra_args_and_unparsable_lines() {
        let agent = fake_agent(
            "cl-model",
            &[
                "это не json — просто строка лога агента",
                r#"{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"планы"},{"type":"tool_use","name":"Bash","input":{"description":"собираю проект"}},{"type":"tool_use","name":"mcp__kb","input":{"description":"ищу по базе"}}]}}"#,
                r#"{"type":"result","result":"готово","session_id":"s-11"}"#,
            ],
            0,
            true,
        );
        let mut cfg = cfg_for("cl-model", &agent);
        cfg.model = Some("strata/big-model".into());
        cfg.extra_args = vec!["--extra-flag".into()];
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, "дело", tx.clone(),
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        drop(tx);
        assert_eq!(out.text, "готово");
        assert_eq!(out.session_id.as_deref(), Some("s-11"));
        let mut lines = Vec::new();
        while let Ok(crate::backend::Event::Progress(line)) = rx.try_recv() {
            lines.push(line);
        }
        assert!(lines.iter().any(|l| l.contains("собираю проект")), "{lines:?}");
        assert!(lines.iter().any(|l| l.contains("🔧 ищу по базе")), "{lines:?}");
        // модель и хвост аргументов уходят в командную строку
        let args = crate::testkit::agent_args(&agent);
        assert!(args.contains(&"--model".to_string()) && args.contains(&"strata/big-model".to_string()), "{args:?}");
        assert!(args.contains(&"--extra-flag".to_string()), "{args:?}");
    }



    use crate::testkit::{fake_agent, test_config, FakeTg};

    fn cfg_for(tag: &str, agent: &std::path::Path) -> crate::config::Config {
        let tg = FakeTg::start();
        let mut cfg = test_config(tag, tg.base());
        cfg.claude_bin = agent.display().to_string();
        cfg
    }

    #[tokio::test]
    async fn result_line_is_the_final_answer() {
        let agent = fake_agent(
            "cl-run",
            &[
                r#"{"type":"assistant","session_id":"s-9","message":{"content":[{"type":"text","text":"думаю"}]}}"#,
                r#"{"type":"result","result":"Готово","total_cost_usd":0.05}"#,
            ],
            0,
            true,
        );
        let cfg = cfg_for("cl-run", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, Some("s-old"), "дело", tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        assert_eq!(out.text, "Готово");
        assert_eq!(out.session_id.as_deref(), Some("s-9"));
        assert_eq!(out.cost_usd, Some(0.05));
        assert!(!out.is_error);
        // продолжение прежней сессии и режим разрешений уходят в аргументы
        let args = crate::testkit::agent_args(&agent);
        assert!(args.contains(&"--resume".to_string()) && args.contains(&"s-old".to_string()), "{args:?}");
        assert!(args.contains(&"auto".to_string()), "{args:?}");
        // промпт уходит по stdin, а не флагом
        assert_eq!(crate::testkit::agent_prompt(&agent), "дело");
    }

    #[tokio::test]
    async fn error_result_keeps_its_text() {
        let agent = fake_agent("cl-err", &[r#"{"type":"result","is_error":true,"result":"упало"}"#], 0, false);
        let cfg = cfg_for("cl-err", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, "дело", tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        assert!(out.is_error);
        assert_eq!(out.text, "упало");
    }

    #[tokio::test]
    async fn api_error_code_is_carried_out_of_result() {
        // строка result при блокировке аккаунта Anthropic несёт код ошибки —
        // по нему бот отличает «подожди сброса» от «бесполезно ждать»
        let agent = fake_agent(
            "cl-apierr",
            &[r#"{"type":"result","is_error":true,"api_error_code":"verification_required","result":"API Error: verification"}"#],
            0, false,
        );
        let cfg = cfg_for("cl-apierr", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, "дело", tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        assert!(out.is_error);
        assert_eq!(out.api_error_code.as_deref(), Some("verification_required"));
    }

    #[tokio::test]
    async fn result_without_text_explains_subtype() {
        let agent = fake_agent("cl-sub", &[r#"{"type":"result","subtype":"error_during_execution"}"#], 0, false);
        let cfg = cfg_for("cl-sub", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, "дело", tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        assert!(out.text.contains("error_during_execution"), "{}", out.text);
    }

    #[tokio::test]
    async fn silence_falls_back_to_assistant_text_or_stderr() {
        // был текст ассистента, но нет result — показываем последний текст
        let agent = fake_agent(
            "cl-last",
            &[r#"{"type":"assistant","message":{"content":[{"type":"text","text":"вот частичный ответ"}]}}"#],
            0,
            false,
        );
        let cfg = cfg_for("cl-last", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, "дело", tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        assert!(out.is_error);
        assert_eq!(out.text, "вот частичный ответ");

        // ни текста, ни result — в чат идёт stderr агента
        let agent2 = std::env::temp_dir().join(format!("cl-stderr-{}", std::process::id()));
        std::fs::write(
            &agent2,
            "#!/bin/sh\nshift 0\ncat >/dev/null\necho 'нет доступа к API' >&2\nexit 1\n",
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&agent2, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mut cfg2 = cfg_for("cl-stderr", &agent2);
        cfg2.claude_bin = agent2.display().to_string();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg2, &cfg2.work_dir, 11, None, "дело", tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        assert!(out.is_error);
        assert!(out.text.contains("нет доступа к API"), "{}", out.text);
    }

    #[tokio::test]
    async fn cancel_kills_the_agent() {
        let agent = fake_agent("cl-cancel", &[r#"{"type":"result","result":"не успеет"}"#], 20, false);
        let cfg = cfg_for("cl-cancel", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let cancel = std::sync::Arc::new(tokio::sync::Notify::new());
        cancel.notify_one();
        let out = super::run(&cfg, &cfg.work_dir, 11, None, "дело", tx, cancel)
            .await
            .expect("запускается");
        assert!(out.is_error);
        assert!(out.text.contains("прервано"), "{}", out.text);
    }

    #[tokio::test]
    async fn missing_binary_says_which_one() {
        let tg = FakeTg::start();
        let mut cfg = test_config("cl-nobin", tg.base());
        cfg.claude_bin = "/nonexistent/claude-here".into();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let err = super::run(
            &cfg, &cfg.work_dir, 11, None, "дело", tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("failed to start"), "{err}");
    }

    #[tokio::test]
    async fn a_prompt_the_agent_closed_early_is_not_fatal() {
        // агент закрыл stdin до чтения запроса: «Broken pipe» не повод бросать
        // разбор — stderr и код выхода дают внятную строку для чата
        let agent = crate::testkit::fake_agent_failing("c-pipe", "claude: нет сессии", 5);
        let cfg = cfg_for("c-pipe", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, &"д".repeat(128 * 1024), tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запрос не обязан дойти агенту");
        assert!(out.is_error);
        assert!(out.text.contains("нет сессии"), "{:?}", out.text);
    }

}
