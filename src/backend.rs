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
To send the user a file (an APK, a report, an image), run `claudetg send-file <path> [caption]`: \
it delivers the file to this chat as a document (up to 50 MB).";

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
    use super::Backend;
    use std::str::FromStr;

    #[test]
    fn backend_names_roundtrip() {
        assert_eq!(Backend::from_str("OpenCode").unwrap(), Backend::OpenCode);
        assert_eq!(Backend::from_str(" claude ").unwrap(), Backend::Claude);
        assert!(Backend::from_str("gpt").is_err());
        assert_eq!(Backend::OpenCode.name(), "opencode");
    }
}
