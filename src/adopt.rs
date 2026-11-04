//! Подхват агентского процесса, который достался от прежнего запуска бота.
//!
//! systemd перезапускает бота, не трогая процесс агента (KillMode=process),
//! поэтому после рестарта новый бот не знает, что сессия уже ведётся: он писал
//! «Выполняется: нет» и поднимал вторую смену поверх работающей. Здесь — чистый
//! разбор командных строк и поиск процесса сессии по /proc.

use std::fs;

/// Ключ флага, которым бэкенд указывает сессию: `--session <id>` у opencode,
/// `--resume <id>` у claude.
pub const SESSION_FLAGS: [&str; 2] = ["--session", "--resume"];

/// Ищет pid процесса, который ведёт сессию `session`. Аргументы вида
/// `--session=<id>` и `--session <id>` равнозначны; наш собственный процесс
/// (`self_pid`) не рассматриваем.
pub fn find_session_pid(cmdlines: &[(u32, Vec<String>)], session: &str, self_pid: u32) -> Option<u32> {
    if session.is_empty() {
        return None;
    }
    cmdlines.iter().find_map(|(pid, args)| {
        if *pid == self_pid {
            return None;
        }
        for (i, arg) in args.iter().enumerate() {
            for flag in SESSION_FLAGS {
                if arg == flag {
                    if args.get(i + 1).map(String::as_str) == Some(session) {
                        return Some(*pid);
                    }
                } else if let Some(rest) = arg.strip_prefix(&format!("{flag}=")) {
                    if rest == session {
                        return Some(*pid);
                    }
                }
            }
        }
        None
    })
}

/// Командные строки всех процессов системы: (pid, аргменты). Непроверяемые
/// или исчезнувшие процессы пропускаем — нас это не должно останавливать.
pub fn proc_cmdlines() -> Vec<(u32, Vec<String>)> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir("/proc") else { return out };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(pid) = name.to_string_lossy().parse::<u32>().ok() else { continue };
        let Ok(bytes) = fs::read(entry.path().join("cmdline")) else { continue };
        if bytes.is_empty() {
            continue;
        }
        let args: Vec<String> = bytes
            .split(|b| *b == 0)
            .filter(|s| !s.is_empty())
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect();
        if !args.is_empty() {
            out.push((pid, args));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{find_session_pid, proc_cmdlines, SESSION_FLAGS};

    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(str::to_string).collect()
    }

    #[test]
    fn finds_agent_by_session_flag() {
        let table = vec![
            (1, args("systemd")),
            (42, args("opencode run --format json --session ses_abc")),
            (7, args("claudetg")),
        ];
        assert_eq!(find_session_pid(&table, "ses_abc", 7), Some(42));
    }

    #[test]
    fn accepts_equal_form_and_resume_flag() {
        let table = vec![(9, args("claude -p --resume=dead-beef"))];
        assert_eq!(find_session_pid(&table, "dead-beef", 1), Some(9));
        let spaced = vec![(10, args("claude -p --resume dead-beef"))];
        assert_eq!(find_session_pid(&spaced, "dead-beef", 1), Some(10));
        assert!(SESSION_FLAGS.contains(&"--resume"));
    }

    #[test]
    fn ignores_self_other_sessions_and_empty_id() {
        let table = vec![
            (5, args("opencode run --session ses_mine")),
            (8, args("opencode run --session ses_other")),
        ];
        // свой процесс не подхватываем
        assert_eq!(find_session_pid(&table, "ses_mine", 5), None);
        // сессии, которую никто не ведёт, — тоже нет
        assert_eq!(find_session_pid(&table, "ses_absent", 1), None);
        assert_eq!(find_session_pid(&table, "ses_mine", 1), Some(5));
        // пустого id в аргументах быть не может — ищем только осмысленный
        assert_eq!(find_session_pid(&table, "", 1), None);
    }

    #[test]
    fn proc_scan_sees_this_process() {
        let me = std::process::id();
        let table = proc_cmdlines();
        // /proc недоступен в песочнице — пустой список тоже корректная работа
        if table.is_empty() {
            return;
        }
        assert!(table.iter().any(|(pid, _)| *pid == me));
    }
}
