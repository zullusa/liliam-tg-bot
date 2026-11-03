use anyhow::{Context, Result};
use std::collections::HashSet;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;

use crate::backend::Backend;

#[derive(Debug, Clone)]
pub struct Config {
    pub bot_token: String,
    pub allowed_users: HashSet<i64>,
    pub proxy_url: Option<String>,
    /// Бэкенд по умолчанию для новых чатов; меняется командой /backend.
    pub backend: Backend,
    pub claude_bin: String,
    pub work_dir: PathBuf,
    pub permission_mode: String,
    pub model: Option<String>,
    pub extra_args: Vec<String>,
    pub opencode_bin: String,
    /// Модель для opencode в формате provider/model (#variant допустим).
    /// По умолчанию — локальная Strata-модель, она используется всегда.
    pub opencode_model: Option<String>,
    /// Опциональный агент из конфигурации opencode (--agent).
    pub opencode_agent: Option<String>,
    /// Автоодобрение разрешений: без него неинтерактивный opencode отклоняет
    /// инструменты, как claude без permission mode.
    pub opencode_auto: bool,
    pub opencode_extra_args: Vec<String>,
    pub show_tools: bool,
    pub state_file: PathBuf,
    /// При лимите Claude передавать контекст opencode и работать им до сброса лимита.
    pub limit_fallback: bool,
    /// Где Claude Code хранит JSONL-сессии (для расшифровки контекста).
    pub claude_projects_dir: PathBuf,
    /// Окно агрегации: сообщения чата копят до N секунд тишины, но не дольше
    /// предельного окна; 0 — начинать задачу сразу (старое поведение).
    pub agg_quiet_secs: u64,
    pub agg_window_secs: u64,
}

fn var(name: &str) -> Option<String> {
    env::var(name).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let bot_token = var("TELEGRAM_BOT_TOKEN").context("TELEGRAM_BOT_TOKEN is not set")?;

        let allowed_users = var("ALLOWED_USER_IDS")
            .context("ALLOWED_USER_IDS is not set (comma-separated Telegram user ids)")?
            .split(',')
            .map(|s| s.trim().parse::<i64>().context("bad user id in ALLOWED_USER_IDS"))
            .collect::<Result<HashSet<_>>>()?;

        // "none" disables the proxy explicitly.
        let proxy_url = match var("PROXY_URL") {
            Some(p) if p.eq_ignore_ascii_case("none") => None,
            Some(p) => Some(p),
            None => Some("http://localhost:3128".to_string()),
        };

        let home = var("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
        let work_dir = var("CLAUDE_WORK_DIR").map(PathBuf::from).unwrap_or_else(|| home.clone());
        let state_file = var("STATE_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".local/state/claudetg/state.json"));

        let backend = var("CLAUDETG_BACKEND")
            .map(|s| Backend::from_str(&s).map_err(anyhow::Error::msg))
            .transpose()?
            .unwrap_or(Backend::Claude);

        Ok(Self {
            bot_token,
            allowed_users,
            proxy_url,
            backend,
            claude_bin: var("CLAUDE_BIN").unwrap_or_else(|| "claude".to_string()),
            work_dir,
            permission_mode: var("CLAUDE_PERMISSION_MODE").unwrap_or_else(|| "auto".to_string()),
            model: var("CLAUDE_MODEL"),
            extra_args: split_args(var("CLAUDE_EXTRA_ARGS")),
            opencode_bin: var("OPENCODE_BIN").unwrap_or_else(|| "opencode".into()),
            // По умолчанию — локальная Qwen3.8-Flash-Next провайдера Strata:
            // передаём её каждому запуску явно, чтобы конфиг opencode не сбивал выбор.
            // OPENCODE_MODEL=none — не передавать -m вовсе.
            opencode_model: match var("OPENCODE_MODEL") {
                Some(v) if v.eq_ignore_ascii_case("none") => None,
                Some(v) => Some(v),
                None => Some("strata/qwen3.8-flash-next-iq3_s".to_string()),
            },
            opencode_agent: var("OPENCODE_AGENT"),
            // По умолчанию включено: в неинтерактивном запуске отказ в разрешении
            // останавливает работу так же, как claude без permission mode.
            opencode_auto: var("OPENCODE_AUTO")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(true),
            opencode_extra_args: split_args(var("OPENCODE_EXTRA_ARGS")),
            show_tools: var("SHOW_TOOLS").map(|v| v == "1" || v == "true").unwrap_or(true),
            state_file,
            limit_fallback: var("CLAUDETG_LIMIT_FALLBACK")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(true),
            claude_projects_dir: var("CLAUDE_PROJECTS_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".claude/projects")),
            agg_quiet_secs: var("CLAUDETG_AGG_QUIET_SECS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(12),
            agg_window_secs: var("CLAUDETG_AGG_WINDOW_SECS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
        })
    }
}

fn split_args(value: Option<String>) -> Vec<String> {
    value
        .map(|s| s.split_whitespace().map(String::from).collect())
        .unwrap_or_default()
}
