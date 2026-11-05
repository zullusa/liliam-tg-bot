//! Исходящая очередь сообщений в чат.
//!
//! И агент (через `claudetg send`), и сам бот (ответ агента, служебные строки)
//! кладут текст в файлы каталога `outbox/`, а бот отправляет их пачкой: несколько
//! коротких сообщений, накопленных за короткое время, уходят одним сообщением.
//! Так в чате не появляется 9-20 сообщений в минуту и подряд одинаковых строк:
//! повторы отсекаются, а служебная болтовня («выполняю команду…») не уходит вовсе.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Что отправляем: ответ агента, строка от агента человеку, служебная заметка бота.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// Финальный ответ агента.
    Answer,
    /// Сообщение, которое агент сам написал человеку (`claudetg send`).
    Agent,
    /// Служебная строка бота (подтверждение очереди, окна агрегации).
    Note,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    /// Момент, когда сообщение поставили в очередь (мс с эпохи).
    pub ts: i64,
    pub chat: i64,
    pub kind: Kind,
    pub text: String,
    /// Приложенный файл (APK, выгрузка): очередь умеет и в них, чтобы отдача файла
    /// подчинялась тем же правилам — паузе Telegram, сроку годности, журналу
    /// отправленного. Поля нет у записей, поставленных прежней версией бота,
    /// оттого `default`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
}

impl Item {
    pub fn new(chat: i64, kind: Kind, text: impl Into<String>) -> Self {
        Self { ts: now_ms(), chat, kind, text: text.into(), path: None }
    }

    /// Файл документом с подписью: текст записи — эта самая подпись.
    pub fn file(chat: i64, kind: Kind, path: impl Into<PathBuf>, caption: impl Into<String>) -> Self {
        Self {
            ts: now_ms(),
            chat,
            kind,
            text: caption.into(),
            path: Some(path.into()),
        }
    }

    /// Это запись про файл, а не про текст?
    pub fn is_file(&self) -> bool {
        self.path.is_some()
    }
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Ставим сообщение в очередь: пишем во временный файл и переименовываем,
/// чтобы читатель не увидел наполовину записанный JSON.
pub fn enqueue(dir: &Path, item: &Item) -> Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let name = format!("{}-{}-{}.json", item.ts, item.chat, next_seq());
    let tmp = dir.join(format!(".{name}.tmp"));
    std::fs::write(&tmp, serde_json::to_vec(item)?)?;
    let dest = dir.join(&name);
    std::fs::rename(&tmp, &dest)?;
    Ok(dest)
}

/// Очередь растёт быстрее, чем счётчик в миллисекундах — добавим счётчик процесса.
fn next_seq() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static SEQ: AtomicU32 = AtomicU32::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Всё, что сейчас лежит в очереди, по возрастанию времени.
pub fn pending(dir: &Path) -> Vec<(PathBuf, Item)> {
    let mut items = Vec::new();
    let Ok(read_dir) = std::fs::read_dir(dir) else { return items };
    for entry in read_dir.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(raw) = std::fs::read(&path) else { continue };
        if let Ok(item) = serde_json::from_slice::<Item>(&raw) {
            items.push((path, item));
        }
    }
    items.sort_by_key(|(_, it)| (it.ts, it.chat));
    items
}

pub fn discard(paths: &[PathBuf]) {
    for path in paths {
        let _ = std::fs::remove_file(path);
    }
}

/// Политика отправки: ждём `quiet` после последнего сообщения чата, но не дольше
/// `max` от первого; к `max_items` сообщений отправляем независимо от тишины.
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    pub quiet: Duration,
    pub max: Duration,
    pub max_items: usize,
}

/// Чаты, очередь которых пора отправить.
pub fn due_chats(items: &[(PathBuf, Item)], now: i64, policy: &Policy) -> Vec<i64> {
    let mut chats: Vec<i64> = items.iter().map(|(_, it)| it.chat).collect();
    chats.sort_unstable();
    chats.dedup();
    chats
        .into_iter()
        .filter(|chat| {
            let mine: Vec<&Item> = items.iter().map(|(_, it)| it).filter(|it| it.chat == *chat).collect();
            let first = mine.iter().map(|it| it.ts).min().unwrap_or(now);
            let last = mine.iter().map(|it| it.ts).max().unwrap_or(now);
            now - last >= policy.quiet.as_millis() as i64
                || now - first >= policy.max.as_millis() as i64
                || mine.len() >= policy.max_items
        })
        .collect()
}

/// Устаревшие сообщения: протухшие за время простоя (Telegram ограничил частоту,
/// связи не было). Отправлять их через час после смысла уже нет — человек
/// получает «старые ответы», которые он видел в живой переписке. Такие снимаем
/// с очереди и пишем в журнал, сколько и почему убрали. `ttl_ms <= 0` — срок
/// отключён, очередь ждёт сколько угодно.
pub fn stale_paths(items: &[(PathBuf, Item)], now: i64, ttl_ms: i64) -> Vec<PathBuf> {
    if ttl_ms <= 0 {
        return Vec::new();
    }
    items
        .iter()
        .filter(|(_, it)| now - it.ts > ttl_ms)
        .map(|(p, _)| p.clone())
        .collect()
}

/// Память о последних отправленных текстах — из журнала отправленного.
/// Без неё перезапуск бота теряет защиту от повторов, и очередь, стоявшая
/// во время лимита, уходит в чат повторно.
pub fn seed_recent(records: &[SentRecord], keep: usize) -> std::collections::HashMap<i64, Vec<String>> {
    let mut out: std::collections::HashMap<i64, Vec<String>> = std::collections::HashMap::new();
    for r in records {
        out.entry(r.chat).or_default().push(r.preview.clone());
    }
    for texts in out.values_mut() {
        if texts.len() > keep {
            let extra = texts.len() - keep;
            texts.drain(0..extra);
        }
    }
    out
}

/// Сохраняя порядок, склеиваем сообщения чата в одно. Разделитель — отдельная
/// строка: куски от разных шагов агента не сливаются в кашу.
pub fn merge(texts: &[String]) -> String {
    texts
        .iter()
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n···\n\n")
}

/// Нормальный вид текста для сравнения: регистр, знаки и переносы не важны.
pub fn normalize(text: &str) -> String {
    let mut out = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() {
            out.extend(c.to_lowercase());
        } else if !out.ends_with(' ') {
            out.push(' ');
        }
    }
    out.trim().to_string()
}

/// Такой же текст уже отправляли: совпадение после нормализки или когда один
/// текст — почти весь другой (повтор с одной лишней стркой).
pub fn is_repeat(text: &str, recent: &[String]) -> bool {
    let new = normalize(text);
    if new.is_empty() {
        return true;
    }
    recent.iter().any(|old| {
        let old = normalize(old);
        if old.is_empty() {
            return false;
        }
        if old == new {
            return true;
        }
        let (short, long) = if new.len() <= old.len() { (&new, &old) } else { (&old, &new) };
        short.chars().count() * 100 >= long.chars().count() * 60 && long.contains(short.as_str())
    })
}

/// Служебная болтовня вместо содержания: «выполняю команду», «сейчас проверю».
/// Такие сообщения человек просил не слать.
const CHATTER: &[&str] = &[
    "выполняю команд",
    "запускаю команд",
    "запускаю сборк",
    "запускаю тест",
    "сейчас выполн",
    "сейчас запущ",
    "сейчас провер",
    "сейчас собер",
    "сейчас обнов",
    "сейчас скача",
    "сейчас правл",
    "сейчас добав",
    "сейчас удал",
    "сейчас откры",
    "приступаю",
    "готов запуст",
    "пересобира",
    "перезапуска",
    "читаю файл",
    "ищу по коду",
    "смотрю лог",
    "работаю над задачей",
    "обновляю статус",
];

/// Служебное сообщение: фраза-анонс и почти ничего вокруг неё. Длинный ответ или
/// ответ со структурой (список, заголовок, код) болтовнёй не считаем.
pub fn is_chatter(text: &str) -> bool {
    let norm = normalize(text);
    if norm.chars().count() > 160 {
        return false;
    }
    if text.contains("```") || text.contains("\n- ") || text.contains("\n* ") || text.contains('#') {
        return false;
    }
    match CHATTER.iter().find(|p| norm.contains(*p)) {
        Some(phrase) => {
            let rest = norm.replace(phrase, "");
            rest.trim().chars().count() <= 24
        }
        None => false,
    }
}

/// Запись о том, что бот отправил в чат: по ней потом чистим сообщения.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentRecord {
    pub chat: i64,
    pub message_id: i64,
    pub ts: i64,
    pub kind: Kind,
    pub preview: String,
}

pub fn append_sent(path: &Path, records: &[SentRecord]) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    for r in records {
        writeln!(file, "{}", serde_json::to_string(r)?)?;
    }
    Ok(())
}

pub fn read_sent(path: &Path) -> Vec<SentRecord> {
    let Ok(raw) = std::fs::read_to_string(path) else { return Vec::new() };
    raw.lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

pub fn write_sent(path: &Path, records: &[SentRecord]) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let body = records
        .iter()
        .map(|r| serde_json::to_string(r))
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    std::fs::write(path, body)?;
    Ok(())
}

/// Какие записи журнала отправленных сообщений удалять из чата: служебные заметки
/// и болтовню, но не последние `keep_last` сообщений.
pub fn clean_targets(records: &[SentRecord], keep_last: usize, only_chatter: bool) -> Vec<SentRecord> {
    let eligible = records.len().saturating_sub(keep_last);
    records
        .iter()
        .enumerate()
        .filter(|(i, _)| *i < eligible)
        .filter(|(_, r)| !only_chatter || r.kind == Kind::Note || is_chatter(&r.preview))
        .map(|(_, r)| r.clone())
        .collect()
}

/// Pid-файл живого бота: чтобы `claudetg send` понимал, есть кому разобрать
/// очередь, или отправлял сообщение сразу.
pub fn write_pid(path: &Path) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, std::process::id().to_string())?;
    Ok(())
}

pub fn bot_alive(path: &Path) -> bool {
    match std::fs::read_to_string(path) {
        Ok(raw) => match raw.trim().parse::<u32>() {
            // Свой pid — тоже «бот жив»: это процесс send-file внутри бота.
            Ok(pid) if pid == std::process::id() => true,
            Ok(pid) => Path::new(&format!("/proc/{pid}")).exists(),
            Err(_) => false,
        },
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(chat: i64, ts: i64, kind: Kind, text: &str) -> Item {
        Item { ts, chat, kind, text: text.into(), path: None }
    }

    #[test]
    fn queue_roundtrip_sorted_by_time() {
        let dir = std::env::temp_dir().join(format!("claudetg-outbox-{}", now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        // порядок записи намеренно обратный
        enqueue(&dir, &item(1, 200, Kind::Answer, "позже")).unwrap();
        enqueue(&dir, &item(1, 100, Kind::Agent, "раньше")).unwrap();
        let got = pending(&dir);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].1.text, "раньше");
        assert_eq!(got[1].1.text, "позже");
        discard(&got.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>());
        assert!(pending(&dir).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn flush_waits_for_quiet_but_not_past_cap() {
        let policy = Policy {
            quiet: Duration::from_secs(8),
            max: Duration::from_secs(30),
            max_items: 6,
        };
        let fresh = vec![(PathBuf::from("a"), item(1, 10_000, Kind::Agent, "текст"))];
        // прошло мало времени — ждём
        assert!(due_chats(&fresh, 12_000, &policy).is_empty());
        // тишина кончилась — отправляем
        assert_eq!(due_chats(&fresh, 18_500, &policy), vec![1]);
        // предельный срок от первого сообщения — раньше тишины
        let old = vec![
            (PathBuf::from("a"), item(1, 0, Kind::Agent, "первое")),
            (PathBuf::from("b"), item(1, 31_000, Kind::Agent, "второе")),
        ];
        assert_eq!(due_chats(&old, 31_500, &policy), vec![1]);
        // много сообщений — отправляем сразу, не дожидаясь тишины
        let many: Vec<(PathBuf, Item)> = (0..6)
            .map(|_| (PathBuf::from("x"), item(1, 0, Kind::Agent, "строка")))
            .collect();
        assert_eq!(due_chats(&many, 100, &policy), vec![1]);
        // другой чат не зависит от этого
        let two = vec![
            (PathBuf::from("a"), item(1, 0, Kind::Agent, "чат 1")),
            (PathBuf::from("b"), item(7, 9_000, Kind::Agent, "чат 7")),
        ];
        assert_eq!(due_chats(&two, 10_000, &policy), vec![1]);
    }

    #[test]
    fn stale_items_leave_the_queue() {
        let dir = std::env::temp_dir().join(format!("claudetg-stale-{}", now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        // час назад — протухло, две минуты назад — ещё нужно
        enqueue(&dir, &item(1, 0, Kind::Answer, "очень старый ответ")).unwrap();
        enqueue(&dir, &item(1, 58 * 60_000, Kind::Agent, "свежий")).unwrap();
        let all = pending(&dir);
        let stale = stale_paths(&all, 60 * 60_000, 30 * 60_000);
        assert_eq!(stale.len(), 1);
        discard(&stale);
        assert_eq!(pending(&dir).len(), 1);
        // ttl=0 — срок отключён, не протухает ничего
        assert!(stale_paths(&pending(&dir), i64::MAX, 0).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restart_remembers_recent_sends() {
        let records = (1..=10)
            .map(|i| SentRecord {
                chat: if i % 2 == 0 { 7 } else { 1 },
                message_id: i,
                ts: i as i64,
                kind: Kind::Answer,
                preview: format!("ответ {i}"),
            })
            .collect::<Vec<_>>();
        let recent = seed_recent(&records, 3);
        // чаты разделены, хранятся только последние три по каждому
        assert_eq!(recent[&1], vec!["ответ 5", "ответ 7", "ответ 9"]);
        assert_eq!(recent[&7], vec!["ответ 6", "ответ 8", "ответ 10"]);
        // пустой журнал — пустая память, а не паника
        assert!(seed_recent(&[], 3).is_empty());
    }

    #[test]
    fn merges_pieces_in_order_with_separator() {
        let text = merge(&["сделал раз".into(), "  ".into(), "сделал два".into()]);
        assert_eq!(text, "сделал раз\n\n···\n\nсделал два");
    }

    #[test]
    fn repeats_are_caught() {
        let recent = vec!["Готово: покрытие бэкенда 100 %, коммит a1b2c3d".to_string()];
        assert!(is_repeat("готово покрытие бэкенда 100% коммит a1b2c3d", &recent));
        assert!(is_repeat("Готово: покрытие бэкенда 100 %", &recent));
        assert!(!is_repeat("Готово: покрытие фронтенда 100 %", &recent));
        assert!(is_repeat("   ", &recent));
    }

    #[test]
    fn announcements_are_chatter_but_reports_are_not() {
        assert!(is_chatter("Выполняю команду в терминале"));
        assert!(is_chatter("Сейчас проверю сборку."));
        assert!(is_chatter("⚙️ Выполняю команду"));
        // есть содержание — не болтовня
        assert!(!is_chatter("Выполняю команду и правлю файл: добавил три проверки и тесты"));
        assert!(!is_chatter(
            "Пересобираю контейнер, потому что поменялся конфиг, и потом проверю заголовки"
        ));
        assert!(!is_chatter("Готово: коммит a1b2, тесты зелёные"));
        assert!(!is_chatter(&format!("сейчас выполню {}", "с".repeat(200))));
        // структурированный ответ не режем
        assert!(!is_chatter("- пункт\n- ещё пункт"));
    }

    #[test]
    fn clean_picks_service_messages_but_keeps_recent() {
        let records = vec![
            SentRecord { chat: 1, message_id: 1, ts: 1, kind: Kind::Answer, preview: "большой отчёт".into() },
            SentRecord { chat: 1, message_id: 2, ts: 2, kind: Kind::Note, preview: "🕐 задача принята".into() },
            SentRecord { chat: 1, message_id: 3, ts: 3, kind: Kind::Agent, preview: "Выполняю команду".into() },
            SentRecord { chat: 1, message_id: 4, ts: 4, kind: Kind::Agent, preview: "Итог: готово".into() },
        ];
        let picked = clean_targets(&records, 1, true);
        assert_eq!(picked.iter().map(|r| r.message_id).collect::<Vec<_>>(), vec![2, 3]);
        // keep_last=0 и только болтовня — одна запись; только-что отправленное не трогаем
        assert_eq!(clean_targets(&records, 0, true).len(), 2);
        // only_chatter=false — чистим всё, кроме последних двух
        assert_eq!(
            clean_targets(&records, 2, false).iter().map(|r| r.message_id).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn sent_log_roundtrip() {
        let path = std::env::temp_dir().join(format!("claudetg-sent-{}.jsonl", now_ms()));
        let records = vec![
            SentRecord { chat: 5, message_id: 10, ts: 1, kind: Kind::Answer, preview: "раз".into() },
            SentRecord { chat: 5, message_id: 11, ts: 2, kind: Kind::Note, preview: "два".into() },
        ];
        append_sent(&path, &records).unwrap();
        append_sent(&path, &[]).unwrap();
        let got = read_sent(&path);
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].kind, Kind::Note);
        write_sent(&path, &got[..1]).unwrap();
        assert_eq!(read_sent(&path).len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pid_file_reports_liveness() {
        let path = std::env::temp_dir().join(format!("claudetg-pid-{}", now_ms()));
        write_pid(&path).unwrap();
        assert!(bot_alive(&path));
        std::fs::write(&path, "999999999").unwrap();
        assert!(!bot_alive(&path));
        std::fs::write(&path, "не число").unwrap();
        assert!(!bot_alive(&path));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_queue_record_survives_without_a_file_path() {
        // запись без пути: прежняя версия бота файлы в очередь не ставила
        let plain: Item =
            serde_json::from_str(r#"{"ts":1,"chat":7,"kind":"agent","text":"подпись"}"#)
                .expect("старый формат читается");
        assert!(!plain.is_file());
        // запись с путём: файл и есть файл, подпись живёт в тексте
        let with = Item::file(7, Kind::Agent, "/tmp/apk/release.apk", "готовый выпуск");
        let raw = serde_json::to_string(&with).unwrap();
        assert!(raw.contains("\"path\""), "{raw}");
        let back: Item = serde_json::from_str(&raw).unwrap();
        assert!(back.is_file());
        assert_eq!(back.path.unwrap().to_str().unwrap(), "/tmp/apk/release.apk");
        assert_eq!(back.text, "готовый выпуск");
    }

    #[test]
    fn junk_in_the_queue_directory_is_not_a_message() {
        let dir = std::env::temp_dir().join(format!("claudetg-outbox-junk-{}", now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // осколок временной записи и битый json: очередь их просто не замечает
        std::fs::write(dir.join("stray.txt"), "не очередь").unwrap();
        std::fs::write(dir.join("broken.json"), "{ не json").unwrap();
        enqueue(&dir, &item(1, 100, Kind::Answer, "дело")).unwrap();
        let got = pending(&dir);
        assert_eq!(got.len(), 1, "читается только разобранная очередь: {got:?}");
        assert_eq!(got[0].1.text, "дело");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blank_texts_are_never_a_sign_of_a_repeat() {
        // пустая после нормализки строка в памяти не должна совпадать ни с чем,
        // иначе новая фраза ошибочно считается уже отправленной
        assert!(!is_repeat("дело сделано", &["   ".into(), "···".into()]));
        assert!(is_repeat("дело сделано", &["дело сделано".into()]));
    }

    #[test]
    fn journal_paths_without_a_directory_are_reported() {
        // путь без каталога (имя прямо здесь): создавать нечего, а записать всё
        // равно не выходит — это ошибка, а не паника
        let p = std::path::Path::new("");
        let rec = SentRecord { chat: 1, message_id: 2, ts: 3, kind: Kind::Answer, preview: "x".into() };
        assert!(append_sent(p, &[rec.clone()]).is_err());
        assert!(write_sent(p, &[rec.clone()]).is_err());
        assert!(write_pid(p).is_err());
        // пустая пачка журнала не должна ничего создавать вообще
        assert!(append_sent(p, &[]).is_ok());
    }

}
