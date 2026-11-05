//! Единый слой запуска агентов: Claude Code (`claude -p`) и OpenCode (`opencode run`).
//!
//! Оба бэкенда дают потоковый JSON и одинаковую для бота картину: сессия,
//! финальный текст, ошибка и стоимость. Выбор бэкенда — глобальный по умолчанию
//! (`CLAUDETG_BACKEND`) и на каждый чат командой `/backend`.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

use crate::claude;
use crate::config::Config;
use crate::opencode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    Claude,
    OpenCode,
}

impl Backend {
    pub fn name(self) -> &'static str {
        match self {
            Backend::Claude => "claude",
            Backend::OpenCode => "opencode",
        }
    }
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Backend {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "claude" => Ok(Backend::Claude),
            "opencode" => Ok(Backend::OpenCode),
            other => Err(format!("непонятный агент: {other} (бывает claude или opencode)")),
        }
    }
}

pub enum Event {
    /// Короткая строка о том, что сейчас делает агент (какой инструмент запущен).
    Progress(String),
}

#[derive(Debug)]
pub struct Outcome {
    pub session_id: Option<String>,
    pub text: String,
    pub is_error: bool,
    pub cost_usd: Option<f64>,
}

pub const SYSTEM_PROMPT: &str = "You are talking with the user through a Telegram bot. \
The user cannot see your tool calls, only your final text answer. The answer is rendered from \
Markdown: headings, **bold**, *italic*, `inline code`, fenced code blocks, links, bullet lists and \
> quotes are shown formatted; tables are flattened into lines, so prefer lists over tables. \
Interactive tools that wait for user input are not available. If you need a decision or \
clarification from the user, ask the question in your final answer and stop; the user's \
reply will arrive as the next message in this same session. \
Do not put shell commands, command lines, flags or code blocks into your answers: describe in \
plain words what you did, why, and what came of it. Mention a file name only when it is needed \
to understand the answer. \
How to communicate: before noticeable work, send ONE short preview message (2-4 lines: what you \
are going to do and why, without commands and without step-by-step detail). Then work silently: \
the bot already shows one live status line about what you are doing, so do not send 'running a \
command', 'now I will check' and similar notes - such messages are dropped as noise. At the end \
send ONE summary: what was done, what changed, what is next, in a few lines. \
To send the user a file (an APK, a report, an image), run `claudetg send-file <path> [caption]`: \
it delivers the file to this chat as a document (up to 50 MB). \
The user may send files in the chat: the bot downloads them and tells you their local paths, so \
read them with your file tools.";

/// Один ход агента в выбранном бэкенде, с продолжением `session_id` если он есть.
/// События прогресса уходят в `events`; `cancel` прерывает выполнение.
pub async fn run(
    backend: Backend,
    cfg: &Config,
    work_dir: &Path,
    chat_id: i64,
    session_id: Option<&str>,
    prompt: &str,
    events: mpsc::UnboundedSender<Event>,
    cancel: Arc<Notify>,
) -> Result<Outcome> {
    match backend {
        Backend::Claude => {
            claude::run(cfg, work_dir, chat_id, session_id, prompt, events, cancel).await
        }
        Backend::OpenCode => {
            opencode::run(cfg, work_dir, chat_id, session_id, prompt, events, cancel).await
        }
    }
}

/// Базовое имя файла без пути — для строк прогресса.
pub fn file_name(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string())
}

/// Строка «иконка действие: деталь» либо без детали, если она пустая.
pub fn with_detail(icon: &str, text: &str, detail: String) -> String {
    if detail.is_empty() {
        format!("{icon} {text}")
    } else {
        format!("{icon} {text}: {detail}")
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn agent_names_print_short() {
        // печать агенту в чате и в логе: имя совпадает с тем, что человек пишет в /backend
        assert_eq!(Backend::Claude.to_string(), "claude");
        assert_eq!(format!("{}", Backend::OpenCode), "opencode");
        assert_eq!(Backend::OpenCode.name(), "opencode");
        // отладочная печать перечисления — как в коде
        assert_eq!(format!("{:?}", Backend::Claude), "Claude");
    }

    use super::Backend;
    use std::str::FromStr;

    #[test]
    fn backend_names_roundtrip() {
        assert_eq!(Backend::from_str("OpenCode").unwrap(), Backend::OpenCode);
        assert_eq!(Backend::from_str(" claude ").unwrap(), Backend::Claude);
        assert!(Backend::from_str("gpt").is_err());
        assert_eq!(Backend::OpenCode.name(), "opencode");
    }

    #[tokio::test]
    async fn unknown_backend_for_missing_binary_is_an_error() {
        // оба бэкенда идут через один слой: отсутствие бинарника — ошибка, не паника
        use crate::backend::{run, Backend};
        use crate::testkit::{test_config, FakeTg};
        let tg = FakeTg::start();
        let mut cfg = test_config("backend-miss", tg.base());
        cfg.claude_bin = "/nonexistent/claude-x".into();
        cfg.opencode_bin = "/nonexistent/opencode-x".into();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let cancel = std::sync::Arc::new(tokio::sync::Notify::new());
        for backend in [Backend::Claude, Backend::OpenCode] {
            let err = run(backend, &cfg, &cfg.work_dir, 1, None, "дело", tx.clone(), cancel.clone())
                .await
                .unwrap_err();
            assert!(format!("{err}").contains("failed to start"), "{backend}: {err}");
        }
    }

    #[test]
    fn helper_text_helpers() {
        use crate::backend::{file_name, with_detail};
        assert_eq!(file_name("/a/b/c.rs"), "c.rs");
        assert_eq!(file_name("просто строка"), "просто строка");
        assert_eq!(with_detail("📖", "Читаю файл", "x.rs".into()), "📖 Читаю файл: x.rs");
        assert_eq!(with_detail("📖", "Читаю файл", String::new()), "📖 Читаю файл");
    }
}
