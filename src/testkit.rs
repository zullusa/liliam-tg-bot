//! Тестовая обвязка: подставной Telegram и подставный агент.
//!
//! Оба нужны, чтобы проверять поведение бота целиком — от письма в чате до
//! ответа, — не трогая настоящий Telegram и не запуская настоящий Claude.
//! Подставной Telegram слушает локальный порт, отвечает по скрипту и записывает
//! всё, что ему прислали: так видно и содержимое ответов, и сколько раз бот
//! сунулся в сеть (важно для лимитов и повторов).

#![cfg(test)]

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};

use serde_json::json;

use crate::config::Config;
use crate::telegram::Bot;
use crate::backend::Backend;

/// Подставной Telegram.
pub struct FakeTg {
    base: String,
    seen: Arc<Mutex<Vec<String>>>,
    replies: Arc<Mutex<HashMap<String, VecDeque<String>>>>,
    _next_id: Arc<AtomicI64>,
    /// Код ответа для скачивания файлов (`/file/...`): 200 — файл отдаётся,
    /// 404 — Telegram говорит, что файла уже нет.
    file_status: Arc<std::sync::atomic::AtomicU16>,
}


impl FakeTg {
    /// Поднимает сервер на свободном порту.
    pub fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("порт для подставы");
        let addr = listener.local_addr().expect("адрес подставы");
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let replies: Arc<Mutex<HashMap<String, VecDeque<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let next_id = Arc::new(AtomicI64::new(1));
        let recorded = seen.clone();
        let scripted = replies.clone();
        let ids = next_id.clone();
        let file_status = Arc::new(std::sync::atomic::AtomicU16::new(200));
        let serve = file_status.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let Some((path, body)) = read_request(&mut stream) else { continue };
                if let Ok(mut seen) = recorded.lock() {
                    seen.push(format!("{path}\t{body}"));
                }
                let method = path.rsplit('/').next().unwrap_or("").to_string();
                let answer = take_reply(&scripted, &method)
                    .unwrap_or_else(|| default_reply(&method, &ids));
                if answer == DROP {
                    // имитация сетевого сбоя: соединение закрыто, ответа нет
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    continue;
                }
                let answer = if path.contains("/file/") {
                    // скачивание вложения: файл, а не JSON
                    let code = serve.load(std::sync::atomic::Ordering::Relaxed);
                    let text = if code == 200 { "200 OK" } else { "404 Not Found" };
                    format!(
                        "HTTP/1.1 {text}\r\nContent-Length: {}\r\nConnection: close\r\n\r\ndata",
                        FILE_BODY.len()
                    )
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                        answer.len()
                    )
                };
                let _ = stream.write_all(answer.as_bytes());
                let _ = stream.flush();
            }
        });
        Self { base: format!("http://{addr}"), seen, replies, _next_id: next_id, file_status }
    }

    /// Заготовленные ответы для метода по порядку; когда список кончается,
    /// метод отвечает обычно.
    pub fn script(self, method: &str, answers: Vec<String>) -> Self {
        if let Ok(mut q) = self.replies.lock() {
            q.insert(method.to_string(), VecDeque::from(answers));
        }
        self
    }

    /// Что пришло на сервер: строки «путь<TAB>тело» по порядку.
    pub fn requests(&self) -> Vec<String> {
        self.seen.lock().map(|v| v.clone()).unwrap_or_default()
    }

    /// Только обращения к методу (например, сколько раз реально отправляли).
    pub fn calls(&self, method: &str) -> Vec<String> {
        self.requests()
            .into_iter()
            .filter(|r| r.split('\t').next().unwrap_or("").ends_with(&format!("/{method}")))
            .collect()
    }

    /// Бот, настроенный на подставу вместо api.telegram.org.
    pub fn bot(&self) -> Bot {
        Bot::new("TOK", None, Some(&self.base)).expect("бот на подставе")
    }

    /// Значение для `CLAUDETG_API_BASE`.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Отдать файл по адресу `/file/...` с этим кодом ответа: 200 — файл есть,
    /// 404 — Telegram говорит, что файла уже нет.
    pub fn file_status(self, code: u16) -> Self {
        self.file_status.store(code, std::sync::atomic::Ordering::Relaxed);
        self
    }
}

/// Тело файла, которое отдаёт подстава при скачивании.
pub const FILE_BODY: &str = "data";

/// Особый ответ подставы: закрыть соединение, ничего не ответив. Так выглядит
/// обрыв посередине запроса — бот должен увидеть сетевую ошибку, а не пустой JSON.
pub const DROP: &str = "@@drop";

/// Адрес, где «нет связи»: порт занят, но соединение сразу закрывается без
/// ответа. Держать просто незакрытый слушатель нельзя — запрос висел бы до
/// таймаута, а освободившийся порт перехватывает подставной сервер другого
/// теста и «мёртвый» адрес становится живым.
pub fn dead_endpoint() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("порт без связи");
    let addr = listener.local_addr().expect("адрес без связи");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            drop(stream); // приняли и сразу закрыли: ответ не приходит никогда
        }
    });
    addr
}

fn take_reply(
    scripted: &Arc<Mutex<HashMap<String, VecDeque<String>>>>,
    method: &str,
) -> Option<String> {
    let mut q = scripted.lock().ok()?;
    q.get_mut(method).and_then(VecDeque::pop_front)
}

/// Ответы, которые Telegram даёт, когда сценарий кончился.
fn default_reply(method: &str, next_id: &Arc<AtomicI64>) -> String {
    match method {
        "getMe" => r#"{"ok":true,"result":{"id":7,"username":"finbri_test"}}"#.into(),
        "getUpdates" => r#"{"ok":true,"result":[]}"#.into(),
        "sendMessage" | "sendDocument" => {
            let id = next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            format!(r#"{{"ok":true,"result":{{"message_id":{id}}}}}"#)
        }
        "editMessageText" | "deleteMessage" | "sendChatAction" => {
            r#"{"ok":true,"result":true}"#.into()
        }
        "getFile" => r#"{"ok":true,"result":{"file_path":"documents/file.bin"}}"#.into(),
        _ => r#"{"ok":true,"result":true}"#.into(),
    }
}

/// Читает запрос: строку, заголовки и тело по Content-Length.
fn read_request(stream: &mut std::net::TcpStream) -> Option<(String, String)> {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let length: usize = head
        .lines()
        .find_map(|l| {
            let l = l.to_lowercase();
            l.strip_prefix("content-length:").map(|v| v.trim().parse().unwrap_or(0))
        })
        .unwrap_or(0);
    while buf.len() < head_end + length {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    let path = head.split_whitespace().nth(1).unwrap_or("/").to_string();
    let body = String::from_utf8_lossy(&buf[head_end..]).to_string();
    Some((path, body))
}

/// Пакет обновлений для подставы: текст от разрешённого пользователя.
pub fn update_text(update_id: i64, chat: i64, user: i64, text: &str) -> String {
    let payload = serde_json::json!({
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "chat": { "id": chat },
            "from": { "id": user },
            "text": text,
        }
    });
    format!(r#"{{"ok":true,"result":[{payload}]}}"#)
}

/// Обновление со вложением (фото) — чтобы проверить скачивание файлов из чата.
pub fn update_photo(update_id: i64, chat: i64, user: i64, caption: &str) -> String {
    let payload = serde_json::json!({
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "chat": { "id": chat },
            "from": { "id": user },
            "caption": caption,
            "photo": [{ "file_id": "PHOTO1", "file_size": 4 }],
        }
    });
    format!(r#"{{"ok":true,"result":[{payload}]}}"#)
}

/// Обновление с вложением нужного рода: `kind` — `document`, `photo`, `video`,
/// `audio`. У фото есть размеры, у документа — имя и mime: по ним бот называет
/// тип файла в чате.
pub fn update_media(update_id: i64, chat: i64, user: i64, caption: &str, kind: &str) -> String {
    // у фото несколько размеров (массив), у остальных вложений — один объект
    let file = match kind {
        "photo" => json!({ "file_id": "PH1", "file_size": 4, "width": 8, "height": 6, "mime_type": "image/png" }),
        "video" => json!({ "file_id": "VI1", "file_size": 4, "mime_type": "video/mp4" }),
        "audio" => json!({ "file_id": "AU1", "file_size": 4, "mime_type": "audio/mpeg" }),
        _ => json!({ "file_id": "DO1", "file_size": 4, "file_name": "otchet.md", "mime_type": "text/markdown" }),
    };
    let field = if kind == "photo" { json!([file]) } else { file };
    let payload = json!({
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "chat": { "id": chat },
            "from": { "id": user },
            "caption": caption,
            kind: field,
        }
    });
    format!(r#"{{"ok":true,"result":[{payload}]}}"#)
}

/// Настройки бота для тестов: всё в отдельном временном каталоге, окно отправки
/// мгновенное, чтобы очередь разбиралась сразу.
pub fn test_config(tag: &str, api_base: &str) -> Config {
    test_config_fresh(tag, api_base, true)
}

/// То же, но с выбором: стирать ли каталог теста. Без стирания тот же тег —
/// те же очередь, журнал и состояние: так проверяется «перезапуск» бота.
pub fn test_config_fresh(tag: &str, api_base: &str, wipe: bool) -> Config {
    let dir = std::env::temp_dir().join(format!("claudetg-test-{tag}-{}", std::process::id()));
    if wipe {
        let _ = std::fs::remove_dir_all(&dir);
    }
    std::fs::create_dir_all(&dir).expect("каталог теста");
    Config {
        api_base: Some(api_base.to_string()),
        bot_token: "TOK".into(),
        allowed_users: [7i64].into_iter().collect(),
        proxy_url: None,
        backend: Backend::Claude,
        claude_bin: "claude".into(),
        work_dir: dir.clone(),
        permission_mode: "auto".into(),
        model: None,
        extra_args: Vec::new(),
        opencode_bin: "opencode".into(),
        opencode_model: None,
        opencode_agent: None,
        opencode_auto: true,
        opencode_extra_args: Vec::new(),
        status_bubble: false,
        state_file: dir.join("state.json"),
        outbox_dir: dir.join("outbox"),
        sent_log: dir.join("sent.jsonl"),
        pid_file: dir.join("bot.pid"),
        send_quiet_secs: 0,
        send_max_secs: 0,
        send_max_items: 1,
        send_ttl_secs: 1800,
        drop_chatter: true,
        attach_dir: dir.join("attach"),
        attach_max_mb: 20,
        limit_fallback: false,
        claude_projects_dir: dir.join("projects"),
        agg_quiet_secs: 0,
        agg_window_secs: 0,
        shift_wait_secs: 900,
    }
}

/// Имя скрипта, которого не будет ни у одного другого теста этого прогона:
/// тег, pid и общий счётчик на процесс. Один счётчик на все помощники — иначе
/// два помощника с одним тегом пишут в один файл.
fn script_path(kind: &str, tag: &str) -> PathBuf {
    static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("claudetg-{kind}-{tag}-{}-{n}", std::process::id()))
}

/// Делает скрипт исполняемым: пишем во временный файл и переносим на место.
///
/// Сразу после записи ядро иногда отвечает «Text file busy» на первую попытку
/// запустить файл (проверено на этом стенде: отказывает ровно первый запуск
/// после записи, повторный — всегда успешен). Поэтому тут же делаем пробный
/// запуск с пустым вводом и сразу снимаем процесс: к тому моменту, как скрипт
/// запускает бот, гонки уже нет.
fn exec_script(path: &Path, body: String) -> PathBuf {
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    std::fs::write(&tmp, body).expect("скрипт агента");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755)).expect("исполняемый");
    }
    std::fs::rename(&tmp, path).expect("скрипт агента на месте");
    let mut probe = loop {
        let spawned = std::process::Command::new(path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
        match spawned {
            Ok(child) => break child,
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(20)),
        }
    };
    let _ = probe.kill();
    let _ = probe.wait();
    path.to_path_buf()
}

/// Подставный агент: скрипт вместо `claude -p` / `opencode run`.
///
/// `lines` — строки потокового JSON, которые агент выдаёт; `sleep` — сколько
/// секунд ждать до них (нужно, чтобы успеть отменить). Промпт скрипт читает из
/// stdin, как настоящий агент, и сохраняет рядом с собой в `.in` — так тест
/// видит, что именно ушло агенту (например, пути скачанных вложений).
pub fn fake_agent(tag: &str, lines: &[&str], sleep: u64, flags: bool) -> PathBuf {
    let path = script_path("agent", tag);
    let prompt_file = format!("{}.in", path.display());
    let _ = std::fs::remove_file(&prompt_file);
    let mut script = format!("#!/bin/sh\ncat > {prompt_file}\n");
    if flags {
        // запись аргументов: тест проверяет, с какими флагами запустили агента
        script.push_str(&format!("printf '%s\\n' \"$@\" > {prompt_file}.args\n"));
    }
    if sleep > 0 {
        script.push_str(&format!("sleep {sleep}\n"));
    }
    // heredoc с кавычками: ни `$`, ни `'`, ни `{}` в тексте строк не трогаются оболочкой
    if !lines.is_empty() {
        script.push_str("cat <<'AGENT_EOF'\n");
        for line in lines {
            script.push_str(line);
            script.push('\n');
        }
        script.push_str("AGENT_EOF\n");
    }
    script.push_str("exit 0\n");
    let path = exec_script(&path, script);
    // пробный запуск успел создать пустые `.in` и `.args` — убираем их, чтобы
    // тест читал только то, что запишет настоящий прогон
    let _ = std::fs::remove_file(&prompt_file);
    let _ = std::fs::remove_file(format!("{prompt_file}.args"));
    path
}

/// Подставный агент, который ругается в stderr и выходит с ненулевым кодом:
/// так проверяется, что сбой процесса агента не теряется.
pub fn fake_agent_failing(tag: &str, stderr_line: &str, code: u32) -> PathBuf {
    let path = script_path("agent", tag);
    // stdin закрываем сразу: так проверка стабильно попадает в ветку «запрос
    // не вошёл агенту» (письмо в закрытый канал даёт «Broken pipe»)
    let script =
        format!("#!/bin/sh\nexec 0<&-\nprintf '%s\\n' '{stderr_line}' >&2\nexit {code}\n");
    exec_script(&path, script)
}

/// Процесс-«смена от прошлого запуска бота»: скрипт с id сессии в имени пути
/// (`/proc/<pid>/cmdline` содержит argv[0]) и спит, пока тест его не снимет.
pub fn adopted_agent(tag: &str, session: &str) -> (std::process::Child, PathBuf) {
    let path = script_path("shift", &format!("{session}-{tag}"));
    exec_script(&path, "#!/bin/sh\nsleep 60\n".to_string());
    // аргументы как у настоящего агента: по ним бот и находит смену в /proc
    let child = std::process::Command::new(&path)
        .args(["run", "--session", session])
        .spawn()
        .expect("процесс смены");
    (child, path)
}

/// Какие аргументы comando получил подставный агент.
pub fn agent_args(agent: &Path) -> Vec<String> {
    std::fs::read_to_string(format!("{}.in.args", agent.display()))
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

/// Что подставный агент получил на stdin (его промпт).
pub fn agent_prompt(agent: &Path) -> String {
    std::fs::read_to_string(format!("{}.in", agent.display())).unwrap_or_default()
}
