use anyhow::{Context, Result};
use std::collections::HashSet;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;

use crate::backend::Backend;

#[derive(Debug, Clone)]
pub struct Config {
    /// Куда слать методы Bot API. По умолчанию — официальный адрес; своя строка
    /// нужна тестам (подставной Telegram) и редким случаям «через свой прокси».
    pub api_base: Option<String>,
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
    /// Пузырь статуса в чате: одно сообщение, которое бот обновляет вместо
    /// потока «выполняю команду». Показывается ли он — `status_bubble`.
    pub status_bubble: bool,
    pub state_file: PathBuf,
    /// Каталог исходящей очереди (`claudetg send` и ответы агента складываются
    /// туда, бот отправляет их пачкой — меньше сообщений в чате).
    pub outbox_dir: PathBuf,
    /// Журнал «что бот отправил в чат» — по нему команда `claudetg clean`
    /// удаляет свои сообщения.
    pub sent_log: PathBuf,
    /// Pid-файл работающего бота: без него `claudetg send` шлёт сразу.
    pub pid_file: PathBuf,
    /// Окно склейки исходящих сообщений: тишина после последнего и предельный срок.
    pub send_quiet_secs: u64,
    pub send_max_secs: u64,
    pub send_max_items: usize,
    /// Срок годности сообщения в очереди: переждя его (лимит Telegram, отсутствие
    /// связи), текст уходит в чат уже бессмысленным. 0 — очередь ждёт вечно.
    pub send_ttl_secs: u64,
    /// Не отправлять в чат служебную болтовню вида «выполняю команду».
    pub drop_chatter: bool,
    /// Куда сохранять вложения из чата и сколько скачивать.
    pub attach_dir: PathBuf,
    pub attach_max_mb: u64,
    /// При лимите Claude передавать контекст opencode и работать им до сброса лимита.
    pub limit_fallback: bool,
    /// Где Claude Code хранит JSONL-сессии (для расшифровки контекста).
    pub claude_projects_dir: PathBuf,
    /// Окно агрегации: сообщения чата копят до N секунд тишины, но не дольше
    /// предельного окна; 0 — начинать задачу сразу (старое поведение).
    pub agg_quiet_secs: u64,
    pub agg_window_secs: u64,
    /// Сколько ждать прежнюю смену (процесс агента от прошлого запуска бота),
    /// прежде чем поднять новую. По умолчанию 15 минут: зависший процесс не
    /// должен останавливать чат навсегда.
    pub shift_wait_secs: u64,
}

fn var(name: &str) -> Option<String> {
    env::var(name).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

impl Config {
    /// Соседний файл рядом с состоянием бота: `state/outbox`, `state/sent.jsonl`.
    fn sibling(state_file: &std::path::Path, name: &str) -> PathBuf {
        state_file
            .parent()
            .map(|dir| dir.join(name))
            .unwrap_or_else(|| PathBuf::from(name))
    }

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
            api_base: var("CLAUDETG_API_BASE"),
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
            // SHOW_TOOLS=0 — совсем без пузыря статуса; по умолчанию одна живая
            // строка о том, чем занят агент, вместо потока сообщений.
            status_bubble: var("SHOW_TOOLS")
                .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
                .unwrap_or(true),
            state_file: state_file.clone(),
            outbox_dir: Self::sibling(&state_file, "outbox"),
            sent_log: Self::sibling(&state_file, "sent.jsonl"),
            pid_file: Self::sibling(&state_file, "bot.pid"),
            send_quiet_secs: var("CLAUDETG_SEND_QUIET_SECS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(7),
            send_max_secs: var("CLAUDETG_SEND_MAX_SECS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(25),
            send_max_items: var("CLAUDETG_SEND_MAX_ITEMS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(4usize),
            send_ttl_secs: var("CLAUDETG_SEND_TTL_SECS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1800),
            drop_chatter: var("CLAUDETG_DROP_CHATTER")
                .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
                .unwrap_or(true),
            attach_dir: var("CLAUDETG_ATTACH_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join("Downloads/claudetg")),
            attach_max_mb: var("CLAUDETG_ATTACH_MAX_MB")
                .and_then(|v| v.parse().ok())
                .unwrap_or(20),
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
            shift_wait_secs: var("CLAUDETG_SHIFT_WAIT_SECS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(900),
        })
    }
}

fn split_args(value: Option<String>) -> Vec<String> {
    value
        .map(|s| s.split_whitespace().map(String::from).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::Config;
    use std::path::{Path, PathBuf};

    #[test]
    fn side_files_live_next_to_the_state() {
        let state = PathBuf::from("/home/u/.local/state/claudetg/state.json");
        assert_eq!(
            Config::sibling(&state, "outbox"),
            PathBuf::from("/home/u/.local/state/claudetg/outbox")
        );
        assert_eq!(
            Config::sibling(&state, "sent.jsonl"),
            PathBuf::from("/home/u/.local/state/claudetg/sent.jsonl")
        );
        // у состояния нет каталога — кладём рядом с именем
        assert_eq!(Config::sibling(Path::new("state.json"), "bot.pid"), PathBuf::from("bot.pid"));
    }

    /// Чистим переменные, которые читаем, чтобы тесты не зависели от порядка.
    fn set_vars(vars: &[(&str, &str)]) {
        for k in [
            "TELEGRAM_BOT_TOKEN", "ALLOWED_USER_IDS", "PROXY_URL", "STATE_FILE",
            "CLAUDETG_BACKEND", "CLAUDE_BIN", "CLAUDE_MODEL", "CLAUDE_EXTRA_ARGS",
            "OPENCODE_BIN", "OPENCODE_MODEL", "OPENCODE_AGENT", "OPENCODE_AUTO",
            "OPENCODE_EXTRA_ARGS", "SHOW_TOOLS", "CLAUDETG_API_BASE",
            "CLAUDETG_SEND_QUIET_SECS", "CLAUDETG_SEND_MAX_SECS", "CLAUDETG_SEND_MAX_ITEMS",
            "CLAUDETG_SEND_TTL_SECS", "CLAUDETG_DROP_CHATTER", "CLAUDETG_ATTACH_DIR",
            "CLAUDETG_ATTACH_MAX_MB", "CLAUDETG_LIMIT_FALLBACK", "CLAUDE_PROJECTS_DIR",
            "CLAUDETG_AGG_QUIET_SECS", "CLAUDETG_AGG_WINDOW_SECS", "CLAUDE_WORK_DIR",
            "CLAUDE_PERMISSION_MODE",
        ] {
            std::env::remove_var(k);
        }
        for (k, v) in vars {
            std::env::set_var(k, v);
        }
    }

    /// Одна проверка сразу для всех вариантов разбора: переменные окружения
    /// общие на процесс, поэтому их нельзя трогать из нескольких тестов разом.
    #[test]
    fn environment_roundtrip() {
        // 1) явные значения
        set_vars(&[
            ("TELEGRAM_BOT_TOKEN", "123:abc"),
            ("ALLOWED_USER_IDS", "7, 8"),
            ("PROXY_URL", "http://proxy:3128"),
            ("OPENCODE_MODEL", "strata/other-model"),
            ("OPENCODE_AGENT", "reviwer"),
            ("CLAUDE_MODEL", "strata/big-model"),
            ("CLAUDETG_SEND_QUIET_SECS", "3"),
            ("CLAUDETG_SEND_MAX_ITEMS", "9"),
            ("CLAUDETG_SEND_TTL_SECS", "0"),
            ("CLAUDETG_DROP_CHATTER", "0"),
            ("SHOW_TOOLS", "0"),
            ("STATE_FILE", "/tmp/claudetg-test/state.json"),
        ]);
        let cfg = super::Config::from_env().unwrap();
        assert_eq!(cfg.allowed_users.len(), 2);
        // прокси и модель заданы словами — берём их, а не дефолт
        assert_eq!(cfg.proxy_url.as_deref(), Some("http://proxy:3128"));
        assert_eq!(cfg.opencode_model.as_deref(), Some("strata/other-model"));
        assert_eq!(cfg.opencode_agent.as_deref(), Some("reviwer"));
        assert_eq!(cfg.model.as_deref(), Some("strata/big-model"));
        assert_eq!(cfg.send_quiet_secs, 3);
        assert_eq!(cfg.send_max_items, 9);
        // ttl=0 — срок годности отключён вручную
        assert_eq!(cfg.send_ttl_secs, 0);
        assert!(!cfg.drop_chatter);
        assert!(!cfg.status_bubble);
        assert_eq!(cfg.outbox_dir, PathBuf::from("/tmp/claudetg-test/outbox"));

        // 2) флаги, списки аргументов и дефолты
        set_vars(&[
            ("TELEGRAM_BOT_TOKEN", "123:abc"),
            ("ALLOWED_USER_IDS", "7"),
            ("PROXY_URL", "none"),
            ("STATE_FILE", "/tmp/claudetg-test2/state.json"),
            ("CLAUDETG_BACKEND", "opencode"),
            ("CLAUDETG_API_BASE", "http://127.0.0.1:8080"),
            ("OPENCODE_MODEL", "none"),
            ("OPENCODE_AUTO", "false"),
            ("SHOW_TOOLS", "false"),
            ("CLAUDE_EXTRA_ARGS", "--dangerously-skip-permissions  --verbose"),
            ("CLAUDETG_ATTACH_MAX_MB", "5"),
            ("CLAUDETG_LIMIT_FALLBACK", "false"),
            ("CLAUDETG_AGG_QUIET_SECS", "не число"),
        ]);
        let cfg = super::Config::from_env().unwrap();
        assert_eq!(cfg.backend, crate::backend::Backend::OpenCode);
        // PROXY_URL=none — прокси отключён явно
        assert!(cfg.proxy_url.is_none());
        assert_eq!(cfg.api_base.as_deref(), Some("http://127.0.0.1:8080"));
        // OPENCODE_MODEL=none — модель не подставляется
        assert!(cfg.opencode_model.is_none());
        assert!(!cfg.opencode_auto);
        assert!(!cfg.status_bubble);
        assert!(!cfg.limit_fallback);
        // список аргументов режется по пробелам
        assert_eq!(cfg.extra_args, vec!["--dangerously-skip-permissions", "--verbose"]);
        assert_eq!(cfg.attach_max_mb, 5);
        // мусор в числах — дефолт, а не падение
        assert_eq!(cfg.agg_quiet_secs, 12);
        // файлы состояния живут рядом с STATE_FILE
        assert_eq!(cfg.sent_log, PathBuf::from("/tmp/claudetg-test2/sent.jsonl"));

        // 3) ошибки настройки
        set_vars(&[]);
        let err = format!("{}", super::Config::from_env().unwrap_err());
        assert!(err.contains("TELEGRAM_BOT_TOKEN"), "{err}");
        set_vars(&[("TELEGRAM_BOT_TOKEN", "123:abc"), ("ALLOWED_USER_IDS", "7, abc")]);
        let err = format!("{}", super::Config::from_env().unwrap_err());
        assert!(err.contains("ALLOWED_USER_IDS"), "{err}");
        set_vars(&[("TELEGRAM_BOT_TOKEN", "123:abc"), ("ALLOWED_USER_IDS", "7"), ("CLAUDETG_BACKEND", "gpt")]);
        let err = format!("{}", super::Config::from_env().unwrap_err());
        assert!(err.contains("непонятный агент"), "{err}");

        // 4) голые обязательные переменные — дефолты остального
        set_vars(&[("TELEGRAM_BOT_TOKEN", "123:abc"), ("ALLOWED_USER_IDS", "7")]);
        let cfg = super::Config::from_env().unwrap();
        assert_eq!(cfg.proxy_url.as_deref(), Some("http://localhost:3128"));
        assert_eq!(cfg.claude_bin, "claude");
        assert_eq!(cfg.opencode_model.as_deref(), Some("strata/qwen3.8-flash-next-iq3_s"));
        assert_eq!(cfg.send_ttl_secs, 1800);
        assert!(cfg.limit_fallback);
        std::env::remove_var("TELEGRAM_BOT_TOKEN");
        std::env::remove_var("ALLOWED_USER_IDS");
    }

    #[test]
    fn args_list_splits_on_whitespace() {
        assert_eq!(super::split_args(Some("a  b\tc".into())), vec!["a", "b", "c"]);
        assert!(super::split_args(None).is_empty());
    }
}
