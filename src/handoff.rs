//! Передача контекста между агентами при замещении (claude ↔ opencode).
//!
//! При исчерпании лимита Claude бот начинает работать через opencode. Полноценно
//! склеить сессии нельзя — форматы разные, — поэтому контекст переносится текстом:
//! - claude → opencode: бот читает JSONL-расшифровку claude-сессии
//!   (`~/.claude/projects/<проект>/<session_id>.jsonl`) и подклеивает её к первому
//!   сообщению новой opencode-сессии;
//! - opencode → claude: claude-сессия никуда не делась и продолжится через
//!   `--resume`, поэтому обратно передаётся только «что происходило во время
//!   смены opencode» — записи самих обменов, которые вёл бот.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// Максимум символов в передаваемой расшифровке.
pub const MAX_TRANSCRIPT_CHARS: usize = 15_000;
/// Сколько обменов хранит чат для заметок о смене.
pub const RECENT_TURNS: usize = 24;

/// Один обмен «задача → ответ», который помнит бот. Не зависит от сессий агентов.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Turn {
    pub user: String,
    pub agent: String,
    /// Обмен произошёл, когда claude замещался (нужно для заметки о смене).
    #[serde(default)]
    pub during_fallback: bool,
}

/// Расшифровка claude-сессии: найден ли файл, что в нём происходило.
pub fn claude_transcript(
    projects_dir: &Path,
    session_id: &str,
    max_chars: usize,
) -> Option<String> {
    let file = find_session_file(projects_dir, session_id)?;
    let raw = std::fs::read_to_string(&file).ok()?;
    let lines = render_claude_lines(&raw);
    if lines.is_empty() {
        return None;
    }
    Some(cap_tail(lines, max_chars))
}

/// Рекурсивно (неглубоко) ищет `<session_id>.jsonl` под projects-директорией.
fn find_session_file(dir: &Path, session_id: &str) -> Option<PathBuf> {
    let target = format!("{session_id}.jsonl");
    let mut stack = vec![dir.to_path_buf()];
    let mut depth = 0;
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.file_name().and_then(|n| n.to_str()) == Some(target.as_str()) {
                return Some(path);
            }
            if path.is_dir() && depth < 2 {
                stack.push(path);
            }
        }
        depth += 1;
        if depth > 3 {
            break;
        }
    }
    None
}

/// Строки расшифровки по JSONL-логу Claude Code.
///
/// Берутся только реальные реплики: текст пользователя и текста ассистента,
/// плюс краткие строки о вызовах инструментов. tool_result, sidechain-ветки
/// (агенты-помощники) и мета-сообщения пропускаются: они раздувают контекст,
/// не добавляя человеку понимания.
fn render_claude_lines(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in raw.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line) else { continue };
        let v_type = v.get("type").and_then(Value::as_str).unwrap_or("");
        if !matches!(v_type, "user" | "assistant") {
            continue;
        }
        if v.get("isSidechain").and_then(Value::as_bool).unwrap_or(false) {
            continue;
        }
        if v.get("isMeta").and_then(Value::as_bool).unwrap_or(false) {
            continue;
        }
        let content = v.pointer("/message/content");
        let mut parts: Vec<String> = Vec::new();
        match content {
            Some(Value::String(s)) => parts.push(s.clone()),
            Some(Value::Array(items)) => {
                for item in items {
                    match item.get("type").and_then(Value::as_str) {
                        Some("text") => {
                            if let Some(t) = item.get("text").and_then(Value::as_str) {
                                if !t.trim().is_empty() {
                                    parts.push(t.to_string());
                                }
                            }
                        }
                        Some("tool_use") => {
                            let name = item.get("name").and_then(Value::as_str).unwrap_or("?");
                            let target = item
                                .pointer("/input/file_path")
                                .or_else(|| item.pointer("/input/path"))
                                .and_then(Value::as_str)
                                .and_then(|p| Path::new(p).file_name())
                                .map(|n| n.to_string_lossy().into_owned())
                                .or_else(|| {
                                    item.pointer("/input/description")
                                        .and_then(Value::as_str)
                                        .map(String::from)
                                });
                            parts.push(match target {
                                Some(t) => format!("[использовал {name}: {t}]"),
                                None => format!("[использовал {name}]"),
                            });
                        }
                        // tool_result, thinking и прочее — в расшифровку не идёт.
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        let body: Vec<String> = parts
            .iter()
            .map(|p| p.trim())
            .filter(|p| !p.is_empty() && !p.starts_with("<local-command"))
            .map(String::from)
            .collect();
        if body.is_empty() {
            continue;
        }
        let speaker = if v_type == "user" { "Пользователь" } else { "Ассистент" };
        for p in body {
            out.push(format!("{speaker}: {p}"));
        }
    }
    out
}

/// Оставляет хвост не длиннее max_chars; начало сворачивается в счётчик.
fn cap_tail(lines: Vec<String>, max_chars: usize) -> String {
    let mut kept: Vec<&String> = Vec::new();
    let mut size = 0;
    let mut skipped = 0;
    for line in lines.iter().rev() {
        size += line.len() + 1;
        if size > max_chars {
            skipped = lines.len() - kept.len();
            break;
        }
        kept.push(line);
    }
    // самый свежий обмен не выбрасываем, даже если он один больше лимита:
    // без него заметка бессмысленна
    if kept.is_empty() {
        if let Some(last) = lines.last() {
            kept.push(last);
            skipped = lines.len() - 1;
        }
    }
    kept.reverse();
    if skipped > 0 {
        let mut out = format!("…пропущено {skipped} более ранних сообщений…\n");
        out.push_str(&kept.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n"));
        out
    } else {
        kept.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n")
    }
}

/// Хвост обменов чата — резервная расшифровка, когда файл сессии не найден,
/// и заметка о смене opencode для claude.
pub fn render_turns(turns: &[Turn], max_chars: usize) -> String {
    let lines: Vec<String> = turns
        .iter()
        .flat_map(|t| {
            [format!("Пользователь: {}", t.user), format!("Агент: {}", t.agent)]
        })
        .collect();
    cap_tail(lines, max_chars)
}

/// Заголовок, который объясняет opencode, что ему передают историю claude.
pub const HANDOFF_HEADER: &str = "КОНТЕКСТ ОТ CLAUDE CODE (его лимит исчерпан, работу продолжаешь ты, opencode). \
Ниже — расшифровка предыдущей работы в этом проекте; файлы на диске уже изменены тем агентом. \
Это не задача, а история, чтобы ты продолжил в том же курсе. За чертой — текущая задача пользователя.";

/// Заметка для claude: чем закончилась смена opencode, пока лимит был исчерпан.
pub fn gap_note(turns: &[Turn]) -> String {
    let body = render_turns(turns, MAX_TRANSCRIPT_CHARS / 2);
    format!(
        "ПОКА ТВОЙ ЛИМИТ БЫЛ ИСЧЕРПАН, СМЕНУ В ЭТОМ ЧАТЕ ВЁЛ ДРУГОЙ АГЕНТ (opencode), \
без доступа к твоей сессии; файлы проекта менялись им. Вот записи этой смены:\n\n{body}\n\n\
Продолжай нашу сессию с учётом этого: проверь актуальное состояние файлов и не отменяй чужие правки без причин."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;

    fn fixture(dir: &Path) -> String {
        let lines = [
            json!({"type": "mode", "mode": "normal", "sessionId": "abc"}),
            json!({"type": "user", "isSidechain": false, "message": {"role": "user",
                "content": "сделай фикстуру"}}),
            json!({"type": "user", "isSidechain": false, "isMeta": true,
                "message": {"role": "user", "content": "скрытая мета-подсказка"}}),
            json!({"type": "assistant", "message": {"role": "assistant", "content": [
                {"type": "text", "text": "делаю"},
                {"type": "tool_use", "name": "Edit", "input": {"file_path": "/p/src/main.rs"}},
            ]}}),
            json!({"type": "user", "message": {"role": "user", "content": [
                {"type": "tool_result", "content": "секретный вывод инструмента"},
            ]}}),
            json!({"type": "assistant", "isSidechain": true, "message": {"role": "assistant",
                "content": [{"type": "text", "text": "работа помощника"}]}}),
            json!({"type": "user", "message": {"role": "user", "content": [
                {"type": "text", "text": "готово, лимит исчерпан"},
            ]}}),
        ];
        let raw = lines.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("\n");
        fs::create_dir_all(dir).unwrap();
        fs::write(dir.join("abc.jsonl"), &raw).unwrap();
        raw
    }

    #[test]
    fn transcript_keeps_dialog_and_tool_names_only() {
        let dir = std::env::temp_dir().join("claudetg-handoff-test");
        let _ = fs::remove_dir_all(&dir);
        fixture(&dir);
        let t = claude_transcript(&dir, "abc", MAX_TRANSCRIPT_CHARS).unwrap();
        assert!(t.contains("Пользователь: сделай фикстуру"));
        assert!(t.contains("Ассистент: делаю"));
        assert!(t.contains("[использовал Edit: main.rs]"));
        assert!(t.contains("Пользователь: готово, лимит исчерпан"));
        assert!(!t.contains("секретный вывод инструмента"));
        assert!(!t.contains("работа помощника"));
        assert!(!t.contains("скрытая мета-подсказка"));
    }

    #[test]
    fn missing_session_returns_none() {
        let dir = std::env::temp_dir().join("claudetg-handoff-test");
        assert!(claude_transcript(&dir, "нет-такого", MAX_TRANSCRIPT_CHARS).is_none());
    }

    /// Живая проверка на настоящем логе Claude Code: берём самую свежую сессию
    /// из ~/.claude/projects и убедимся, что расшифровка собирается.
    #[test]
    fn real_session_transcripts() {
        let home = std::env::var("HOME").unwrap();
        let projects = Path::new(&home).join(".claude/projects");
        // на машине без Claude Code проверка не нужна — просто пропускаем её
        if !projects.is_dir() {
            return;
        }
        let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
        let mut stack = vec![projects];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(&dir) else { continue };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                    if let Ok(t) = entry.metadata().and_then(|m| m.modified()) {
                        if newest.as_ref().is_none_or(|(prev, _)| t > *prev) {
                            newest = Some((t, path));
                        }
                    }
                }
            }
        }
        let (_t, path) = newest.expect("нет сессий Claude Code");
        let sid = path.file_stem().unwrap().to_string_lossy().into_owned();
        let raw = fs::read_to_string(&path).unwrap();
        let expected_lines = render_claude_lines(&raw).len();
        let transcript = claude_transcript(path.parent().unwrap(), &sid, MAX_TRANSCRIPT_CHARS)
            .expect("расшифровка не собралась");
        log::info!("сессия {sid}: расшифровка {} символов, {expected_lines} реплик", transcript.len());
        assert!(transcript.contains("Пользователь:") || transcript.contains("Ассистент:"));
    }

    #[test]
    fn tool_lines_name_the_file_or_the_description() {
        let lines = [
            json!({"type": "assistant", "message": {"role": "assistant", "content": [
                {"type": "tool_use", "name": "Bash", "input": {"description": "собираю проект"}},
                {"type": "tool_use", "name": "AskUserQuestion"},
                {"type": "thinking", "thinking": "секретные рассуждения"},
                {"type": "tool_use", "name": "Read", "input": {"path": "/srv/app/config.yaml"}},
                {"type": "text", "text": "   "},
            ]}}),
            json!({"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": "<local-command-stdout>служебный вывод"}]}}),
            json!({"type": "assistant", "message": {"role": "assistant", "content": "просто строка ответа"}}),
            json!({"type": "queue-operation", "message": {"role": "user", "content": "не реплика"}}),
            json!({"message": {"role": "user", "content": "нет поля type"}}),
        ];
        let raw = lines
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n")
            + "\nстрока, которая не json вообще";
        let out = render_claude_lines(&raw).join("\n");
        assert!(out.contains("[использовал Bash: собираю проект]"), "{out}");
        assert!(out.contains("[использовал AskUserQuestion]"), "{out}");
        assert!(out.contains("[использовал Read: config.yaml]"), "{out}");
        assert!(out.contains("просто строка ответа"), "{out}");
        assert!(!out.contains("секретные рассуждения"), "{out}");
        assert!(!out.contains("служебный вывод"), "{out}");
        assert!(!out.contains("не реплика"), "{out}");
        assert!(!out.contains("нет поля type"), "{out}");
    }

    #[test]
    fn a_session_without_replicas_gives_no_context() {
        let dir = std::env::temp_dir().join("claudetg-handoff-empty");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // файл есть, но реальных реплик в нём нет: расшифровки не будет
        let raw = json!({"type": "system", "message": {"content": "только служебное"}}).to_string();
        fs::write(dir.join("empty-sess.jsonl"), &raw).unwrap();
        assert!(claude_transcript(&dir, "empty-sess", MAX_TRANSCRIPT_CHARS).is_none());
        // битый json — тоже пустая расшифровка, а не паника
        fs::write(dir.join("broken-sess.jsonl"), "не json\nи ещё раз\n").unwrap();
        assert!(claude_transcript(&dir, "broken-sess", MAX_TRANSCRIPT_CHARS).is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn very_deep_directories_are_not_searched() {
        // каталоги глубже трёх уровней не разбираем: там сессий бота не бывает
        let dir = std::env::temp_dir().join("claudetg-handoff-deep");
        let _ = fs::remove_dir_all(&dir);
        let deep = dir.join("a").join("b").join("c").join("d");
        fs::create_dir_all(&deep).unwrap();
        fs::write(deep.join("deep-sess.jsonl"), "{}").unwrap();
        assert!(claude_transcript(&dir, "deep-sess", MAX_TRANSCRIPT_CHARS).is_none());
        // а на нормальной глубине файл находится
        fs::create_dir_all(dir.join("proj")).unwrap();
        fs::write(dir.join("proj").join("near-sess.jsonl"), "{}").unwrap();
        assert!(find_session_file(&dir, "near-sess").is_some());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn tail_is_capped_and_head_collapsed() {
        let lines: Vec<String> = (0..100).map(|i| format!("строка {i} — много текста для веса")).collect();
        let capped = cap_tail(lines, 600);
        assert!(capped.starts_with("…пропущено "));
        assert!(capped.contains("строка 99"));
        assert!(!capped.contains("строка 0 —"));
        assert!(capped.chars().count() < 800);
    }

    #[test]
    fn recent_turns_render_and_note_mentions_shift() {
        let turns = vec![
            Turn { user: "собери".into(), agent: "собрал".into(), during_fallback: false },
            Turn { user: "поправь".into(), agent: "поправил".into(), during_fallback: true },
        ];
        let r = render_turns(&turns, MAX_TRANSCRIPT_CHARS);
        assert!(r.contains("Пользователь: поправь"));
        assert!(r.contains("Агент: собрал"));
        let note = gap_note(&turns[1..]);
        assert!(note.contains("ДРУГОЙ АГЕНТ"));
        assert!(note.contains("Пользователь: поправь"));
        assert!(!note.contains("Пользователь: собрал") && !note.contains("Пользователь: собери"));
    }

    #[test]
    fn transcript_is_found_in_nested_project_dirs() {
        let dir = std::env::temp_dir().join(format!("claudetg-handoff-nested-{}", std::process::id()));
        let deep = dir.join("proj").join("sub");
        fs::create_dir_all(&deep).unwrap();
        fs::write(
            deep.join("sess-1.jsonl"),
            r#"{"type":"user","message":{"content":"сделай X"}}"#,
        )
        .unwrap();
        let got = claude_transcript(&dir, "sess-1", 4000).expect("нашли расшифровку");
        assert!(got.contains("сделай X"), "{got}");
        // нет файла — молча возвращаем None, бот возьмёт заметки из памяти
        assert!(claude_transcript(&dir, "sess-missing", 4000).is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cap_tail_keeps_the_latest_lines() {
        let lines: Vec<String> = (1..=10).map(|i| format!("строка {i}")).collect();
        let capped = cap_tail(lines, 40);
        assert!(capped.contains("строка 10"), "{capped}");
        assert!(!capped.contains("строка 8"), "{capped}");
        assert!(capped.contains("пропущено"), "{capped}");
        // свежий обмен остаётся, даже если он один не влезает в лимит
        let big = vec!["д".repeat(50)];
        assert!(cap_tail(big, 5).contains('д'));
        // пустота — пустая строка
        assert_eq!(cap_tail(Vec::new(), 100), "");
    }

    #[test]
    fn directory_walk_stops_after_a_few_levels() {
        let root = std::env::temp_dir().join(format!("claudetg-deep-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        // четыре ветки: обход обрывается на пределе глубины, а не идёт до бесконечности
        for branch in ["a", "b", "c", "d"] {
            fs::create_dir_all(root.join(branch)).unwrap();
        }
        fs::write(root.join("a").join("nope.jsonl"), "{}").unwrap();
        assert!(find_session_file(&root, "session").is_none());
        // а на малой глубине файл находится
        fs::write(root.join("session.jsonl"), "{}").unwrap();
        assert_eq!(find_session_file(&root, "session"), Some(root.join("session.jsonl")));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn odd_content_shapes_are_skipped() {
        let raw = [
            // текст без поля text — брать нечего
            json!({"type": "assistant", "message": {"role": "assistant",
                "content": [{"type": "text"}, {"type": "text", "text": "   "}]}}),
            // content не строка и не список — пропускаем
            json!({"type": "assistant", "message": {"role": "assistant", "content": 42}}),
            json!({"type": "assistant", "message": {"role": "assistant", "content": null}}),
            // обычная строка дальше работает как прежде
            json!({"type": "assistant", "message": {"role": "assistant", "content": "обычный ответ"}}),
        ]
        .map(|v| v.to_string())
        .join("\n");
        let lines = render_claude_lines(&raw);
        assert_eq!(lines, vec!["Ассистент: обычный ответ".to_string()], "{lines:?}");
    }

}
