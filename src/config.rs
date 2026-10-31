use anyhow::{Context, Result};
use std::collections::HashSet;
use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub bot_token: String,
    pub allowed_users: HashSet<i64>,
    pub proxy_url: Option<String>,
    pub claude_bin: String,
    pub work_dir: PathBuf,
    pub permission_mode: String,
    pub model: Option<String>,
    pub extra_args: Vec<String>,
    pub show_tools: bool,
    pub state_file: PathBuf,
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

        Ok(Self {
            bot_token,
            allowed_users,
            proxy_url,
            claude_bin: var("CLAUDE_BIN").unwrap_or_else(|| "claude".to_string()),
            work_dir,
            permission_mode: var("CLAUDE_PERMISSION_MODE").unwrap_or_else(|| "auto".to_string()),
            model: var("CLAUDE_MODEL"),
            extra_args: var("CLAUDE_EXTRA_ARGS")
                .map(|s| s.split_whitespace().map(String::from).collect())
                .unwrap_or_default(),
            show_tools: var("SHOW_TOOLS").map(|v| v == "1" || v == "true").unwrap_or(true),
            state_file,
        })
    }
}
