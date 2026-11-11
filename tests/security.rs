//! Проверки безопасности на настоящем собранном бинарнике: токен не течёт в
//! вывод и журнал, команды корректно отказывают на кривых аргументах, бот без
//! настроек не поднимает поллинг, а кривые символы в тексте не ломают отправку.
//! Окружение стерильное (env_clear) и каталог — /tmp: тест не способен прочитать
//! настоящий .env или потрогать живое состояние бота.

use std::net::TcpListener;
use std::process::Stdio;

/// Секрет в токене: проверяем, что он не появляется в выводе бинарника.
const SECRET: &str = "SEKRET-NEVIDIM-KLYUCH-9f3a";

fn run(args: &[&str], extra: &[(&str, &str)]) -> std::process::Output {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_claudetg"));
    cmd.env_clear()
        .env("PATH", "/usr/bin:/bin")
        .current_dir(std::env::temp_dir())
        .stdin(Stdio::null())
        .args(args);
    for (k, v) in extra {
        cmd.env(k, v);
    }
    cmd.output().expect("запуск")
}

/// Порт, на котором никто не слушает: соединение отклоняется сразу.
fn refused_addr() -> String {
    let l = TcpListener::bind("127.0.0.1:0").expect("порт");
    let a = l.local_addr().expect("адрес");
    drop(l);
    format!("http://{a}")
}

/// Обязательные настройки для команд, которым нужен конфиг (кроме токена).
fn base_env(tag: &str) -> Vec<(&'static str, String)> {
    vec![
        ("ALLOWED_USER_IDS", "7".into()),
        ("PROXY_URL", "none".into()),
        ("STATE_FILE", format!("/tmp/claudetg-sec-{tag}-{}", std::process::id())),
    ]
}

#[test]
fn token_never_appears_in_error_output() {
    // отправка на мёртвый адрес: reqwest показывает в ошибке полный URL вида
    // /bot<токен>/sendMessage — бот обязан вычистить токен из всех строк
    let api = refused_addr();
    let token = format!("1234567:{SECRET}");
    let env = base_env("token");
    let mut extra: Vec<(&str, &str)> =
        vec![("TELEGRAM_BOT_TOKEN", &token), ("CLAUDETG_API_BASE", &api)];
    for (k, v) in &env {
        extra.push((k, v.as_str()));
    }
    let out = run(&["send", "11"], &extra);
    assert!(!out.status.success(), "мёртвый API — команда обязана ошибаться");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!combined.contains(SECRET), "токен засветился в выводе: {combined}");
    // но понять, что Telegram недоступен, по выводу можно
    assert!(combined.contains("chat") || combined.contains("чат"), "{combined}");
}

#[test]
fn bare_run_without_settings_refuses_to_poll() {
    // запуск без TELEGRAM_BOT_TOKEN завершается ошибкой сразу — поллинг не стартует
    let out = run(&[], &[("ALLOWED_USER_IDS", "7"), ("PROXY_URL", "none")]);
    assert!(!out.status.success());
    let msg = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(msg.contains("TELEGRAM_BOT_TOKEN"), "{msg}");
}

#[test]
fn empty_allow_list_is_impossible_to_misconfigure_quietly() {
    // пустой ALLOWED_USER_IDS — ошибка настройки, а не «бот молчит на всех»
    let out = run(
        &["send", "11"],
        &[
            ("TELEGRAM_BOT_TOKEN", "123:abc"),
            ("ALLOWED_USER_IDS", ""),
            ("PROXY_URL", "none"),
        ],
    );
    assert!(!out.status.success());
    let msg = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(msg.contains("ALLOWED_USER_IDS"), "{msg}");
}

#[test]
fn bad_user_id_in_allow_list_is_rejected() {
    let out = run(
        &["send", "11"],
        &[
            ("TELEGRAM_BOT_TOKEN", "123:abc"),
            ("ALLOWED_USER_IDS", "7, qwerty"),
            ("PROXY_URL", "none"),
        ],
    );
    assert!(!out.status.success());
    let msg = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(msg.contains("ALLOWED_USER_IDS"), "{msg}");
}

#[test]
fn clean_without_chat_is_a_usage_error_and_touches_nothing() {
    let env = base_env("clean");
    let mut extra: Vec<(&str, &str)> = vec![("TELEGRAM_BOT_TOKEN", "123:abc")];
    for (k, v) in &env {
        extra.push((k, v.as_str()));
    }
    let out = run(&["clean"], &extra);
    assert!(!out.status.success());
    let msg = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(msg.contains("claudetg clean"), "{msg}");
}

#[test]
fn send_file_without_path_is_a_usage_error() {
    let env = base_env("sendfile");
    let mut extra: Vec<(&str, &str)> =
        vec![("TELEGRAM_BOT_TOKEN", "123:abc"), ("CLAUDETG_CHAT_ID", "11")];
    for (k, v) in &env {
        extra.push((k, v.as_str()));
    }
    let out = run(&["send-file"], &extra);
    assert!(!out.status.success());
    let msg = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(msg.contains("send-file"), "{msg}");
}

#[test]
fn special_characters_in_text_cannot_break_the_send() {
    // кавычки, обратные слеши, `$()` и перевод строки уходят как текст: отправка
    // на подставной Telegram должна завершиться успехом, без паник и «магии»
    let listener = TcpListener::bind("127.0.0.1:0").expect("порт подставы");
    let addr = listener.local_addr().expect("адрес");
    let seen = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let body_sink = seen.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            use std::io::{Read, Write};
            let mut buf = vec![0u8; 8192];
            let n = stream.read(&mut buf).unwrap_or(0);
            if let Ok(mut s) = body_sink.lock() {
                s.push_str(&String::from_utf8_lossy(&buf[..n]));
            }
            let body = r#"{"ok":true,"result":{"message_id":77}}"#;
            let answer = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(answer.as_bytes());
            let _ = stream.flush();
        }
    });

    let payload = "злой \"кавычки\" и `backtick` $(rm -rf /) \\ слеши\nперевод";
    let api = format!("http://{addr}");
    let state = format!("/tmp/claudetg-sec-quotes-{}", std::process::id());
    let _ = std::fs::remove_file(&state);
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_claudetg"))
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("TELEGRAM_BOT_TOKEN", "123:abc")
        .env("ALLOWED_USER_IDS", "7")
        .env("PROXY_URL", "none")
        .env("STATE_FILE", &state)
        .env("CLAUDETG_API_BASE", &api)
        .current_dir(std::env::temp_dir())
        .args(["send", "11"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("запуск");
    {
        use std::io::Write;
        child.stdin.take().expect("stdin").write_all(payload.as_bytes()).expect("текст передан");
    }
    let out = child.wait_with_output().expect("дождаться");
    let _ = std::fs::remove_file(&state);
    assert!(out.status.success(), "кавычки не должны ломать отправку");
    let sent = seen.lock().unwrap().clone();
    assert!(sent.contains("sendMessage"), "шли именно sendMessage: {sent}");
    // текст ушёл дословно: JSON-экранирование на месте, `$()` не раскрыт
    assert!(sent.contains(r#"злой \"кавычки\""#), "кавычки экранированы: {sent}");
    assert!(sent.contains("$(rm -rf /)"), "оболочка ничего не подставила: {sent}");
}
