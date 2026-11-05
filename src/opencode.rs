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
    // «Broken pipe» здесь тоже не ошибка: агент мог завершиться, не прочитав
    // запрос, — причины видны из stderr и кода выхода.
    let mut stdin = child.stdin.take().context("no stdin")?;
    let full_prompt = format!("{SYSTEM_PROMPT}\n\n---\n{prompt}");
    if let Err(e) = stdin.write_all(full_prompt.as_bytes()).await {
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
    #[test]
    fn helper_tools_and_unknown_tools_are_named_by_description() {
        let task = json!({"tool": "task", "state": {"input": {"description": "переписать модуль"}}});
        assert_eq!(describe_tool(task), "🤖 Поручаю часть работы помощнику: переписать модуль");
        let other = json!({"tool": "mcp_db", "state": {"input": {"description": "читаю таблицу"}}});
        assert_eq!(describe_tool(other), "🔧 читаю таблицу");
    }

    /// Живая проверка формата `opencode run --format json`: настоящая сессия
    /// должна продолжаться по `--session`, а не начинаться заново.
    #[ignore = "нужен установленный opencode и настроенный провайдер"]
    #[tokio::test]
    async fn live_turn_resumes_session() {
        use super::run;
        use crate::backend::Event;
        use tokio::sync::{mpsc, Notify};

        // настройки как в тестах на подставе, только агент настоящий
        let tg = FakeTg::start();
        let mut cfg = test_config("live-opencode", tg.base());
        cfg.opencode_bin = "opencode".into();
        cfg.work_dir = std::env::temp_dir().join("claudetg-live-opencode");
        std::fs::create_dir_all(&cfg.work_dir).unwrap();
        let dir = cfg.work_dir.clone();

        let (tx, _rx) = mpsc::unbounded_channel::<Event>();
        let first = run(
            &cfg, &dir, 0, None,
            "Запомни кодовое слово «ОРАНЖЕВОЕ». Ответь ровно: ОК",
            tx, std::sync::Arc::new(Notify::new()),
        )
        .await
        .expect("первый ход");
        assert!(!first.is_error, "первый ход: {}", first.text);
        assert!(first.text.contains("ОК"), "первый ход: {}", first.text);
        let session = first.session_id.expect("sessionID в потоке json");
        assert!(session.starts_with("ses_"), "sessionID: {session}");

        let (tx2, _rx2) = mpsc::unbounded_channel::<Event>();
        let second = run(
            &cfg, &dir, 0, Some(&session),
            "Какое кодовое слово я просил запомнить? Ответь одним словом.",
            tx2, std::sync::Arc::new(Notify::new()),
        )
        .await
        .expect("второй ход");
        assert!(!second.is_error, "второй ход: {}", second.text);
        assert!(
            second.text.to_uppercase().contains("ОРАНЖЕВОЕ") || second.text.contains("Оранжево"),
            "сессия не продолжилась: {}",
            second.text
        );
    }

    // ------- Проверки разбора потока `opencode run --format json` -------

    use crate::testkit::{fake_agent, test_config, FakeTg};

    fn cfg_for(tag: &str, agent: &std::path::Path) -> crate::config::Config {
        let tg = FakeTg::start();
        let mut cfg = test_config(tag, tg.base());
        cfg.opencode_bin = agent.display().to_string();
        cfg
    }

    #[tokio::test]
    async fn text_events_give_answer_session_and_cost() {
        let agent = fake_agent(
            "oc-run",
            &[
                r#"{"type":"text","sessionID":"oc-1","part":{"text":"Итог: готово"}}"#,
                r#"{"type":"step_finish","part":{"cost":0.02}}"#,
            ],
            0,
            true,
        );
        let cfg = cfg_for("oc-run", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg,
            &cfg.work_dir,
            11,
            None,
            "дело",
            tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        assert_eq!(out.text, "Итог: готово");
        assert_eq!(out.session_id.as_deref(), Some("oc-1"));
        assert_eq!(out.cost_usd, Some(0.02));
        assert!(!out.is_error);
        let args = crate::testkit::agent_args(&agent);
        assert!(args.contains(&"--auto".to_string()), "авто-режим включён: {args:?}");
        assert!(args.contains(&"--session".to_string()) || !args.iter().any(|a| a == "--session"));
    }

    #[tokio::test]
    async fn tool_events_become_progress_lines() {
        let agent = fake_agent(
            "oc-tool",
            &[
                r#"{"type":"tool_use","part":{"tool":"read","state":{"status":"completed","input":{"filePath":"/tmp/отчёт.md"}}}}"#,
                r#"{"type":"text","part":{"text":"прочитал"}}"#,
            ],
            0,
            false,
        );
        let cfg = cfg_for("oc-tool", &agent);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, Some("oc-7"), "дело", tx.clone(),
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        drop(tx);
        let ev = rx.recv().await.expect("событие прогресса");
        match ev {
            crate::backend::Event::Progress(line) => assert!(line.contains("отчёт.md"), "{line}"),
        }
        assert_eq!(out.text, "прочитал");
        // продолжение сессии попадает в командную строку через --session
        assert_eq!(out.session_id.as_deref(), Some("oc-7"));
    }

    #[tokio::test]
    async fn error_event_wins_over_partial_text() {
        let agent = fake_agent(
            "oc-err",
            &[
                r#"{"type":"text","part":{"text":"начал"}}"#,
                r#"{"type":"error","error":{"type":"error","message":"Provider limit reached"}}"#,
            ],
            0,
            false,
        );
        let cfg = cfg_for("oc-err", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, "дело", tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        assert!(out.is_error);
        // сообщение про лимит важнее частичного ответа: по нему планируют продолжение
        assert!(out.text.contains("limit"), "{}", out.text);
    }

    #[tokio::test]
    async fn cancelled_run_reports_interrupt() {
        let agent = fake_agent("oc-cancel", &[r#"{"type":"text","part":{"text":"не успеет"}}"#], 20, false);
        let cfg = cfg_for("oc-cancel", &agent);
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
    async fn missing_binary_is_an_error_not_a_panic() {
        let tg = FakeTg::start();
        let mut cfg = test_config("oc-nobin", tg.base());
        cfg.opencode_bin = "/nonexistent/opencode-here".into();
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
    async fn model_and_agent_flags_survive_noisy_output() {
        let agent = fake_agent(
            "oc-flags",
            &[
                "обычная строка лога, не json",
                r#"{"type":"step_start","part":{"type":"text"}}"#,
                r#"{"type":"tool_use","part":{"tool":"task","state":{"input":{"description":"поручаю подреву"}}}}"#,
                r#"{"type":"text","sessionID":"oc-9","part":{"text":"готово"}}"#,
                r#"{"type":"step_finish","part":{"cost":0.01}}"#,
            ],
            0,
            true,
        );
        let mut cfg = cfg_for("oc-flags", &agent);
        cfg.opencode_model = Some("strata/small-model".into());
        cfg.opencode_agent = Some("reviewer".into());
        cfg.opencode_extra_args = vec!["--thinking".into()];
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, "дело", tx.clone(),
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        drop(tx);
        assert_eq!(out.text, "готово");
        assert_eq!(out.session_id.as_deref(), Some("oc-9"));
        assert_eq!(out.cost_usd, Some(0.01));
        match rx.recv().await.expect("строка о инструменте") {
            crate::backend::Event::Progress(line) => assert!(line.contains("подреву"), "{line}"),
        }
        let args = crate::testkit::agent_args(&agent);
        assert!(args.contains(&"-m".to_string()) && args.contains(&"strata/small-model".to_string()), "{args:?}");
        assert!(args.contains(&"--agent".to_string()) && args.contains(&"reviewer".to_string()), "{args:?}");
        assert!(args.contains(&"--thinking".to_string()), "{args:?}");
    }

    #[tokio::test]
    async fn a_failing_process_reports_its_stderr() {
        let agent = crate::testkit::fake_agent_failing("oc-fail", "opencode: нет доступа к сессии", 3);
        let cfg = cfg_for("oc-fail", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, "дело", tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        assert!(out.is_error);
        assert!(out.text.contains("нет доступа к сессии"), "{:?}", out.text);
    }

    #[tokio::test]
    async fn silent_exit_is_reported_as_failure() {
        // агент вышел молча: пустой ответ — это ошибка, а не «пустое сообщение» в чат
        let agent = fake_agent("oc-silent", &[], 0, false);
        let cfg = cfg_for("oc-silent", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, "дело", tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        assert!(out.is_error);
        assert!(out.text.contains("без ответа"), "{}", out.text);
    }

    #[tokio::test]
    async fn an_error_that_is_not_a_limit_keeps_the_answer_so_far() {
        let agent = fake_agent(
            "oc-errtext",
            &[
                r#"{"type":"text","sessionID":"oc-8","part":{"text":"Половину работы сделал"}}"#,
                r#"{"type":"error","error":{"type":"provider_error","message":"модель недоступна"}}"#,
            ],
            0,
            false,
        );
        let cfg = cfg_for("oc-errtext", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, "дело", tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запускается");
        assert!(out.is_error, "ошибка есть");
        assert_eq!(out.text, "Половину работы сделал", "не лимит — показываем сделанное");
    }

    #[tokio::test]
    async fn a_prompt_the_agent_closed_early_is_not_fatal() {
        // агент закрыл stdin, не прочитав запрос: письмо вернёт «Broken pipe»,
        // а причины видны из stderr и кода выхода
        let agent = crate::testkit::fake_agent_failing("oc-pipe", "запрос не дошёл", 4);
        let cfg = cfg_for("oc-pipe", &agent);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = super::run(
            &cfg, &cfg.work_dir, 11, None, &"д".repeat(128 * 1024), tx,
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .expect("запрос не обязан дойти агенту");
        assert!(out.is_error);
        assert!(out.text.contains("запрос не дошёл"), "{:?}", out.text);
    }

}
