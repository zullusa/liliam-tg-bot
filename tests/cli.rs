//! Проверки командной строки на настоящем собранном бинарнике: версия,
//! ошибки настройки, отправка текста и файла, отказ поднимать второй поллинг-бот.
//! Окружение стерильное и каталог — без .env, чтобы тест никогда не смог
//! запустить живого бота с настоящими настройками.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn run(args: &[&str], extra: &[(&str, &str)]) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_claudetg"));
    cmd.env_clear()
        .env("PATH", "/usr/bin:/bin")
        .current_dir(std::env::temp_dir())
        .stdin(Stdio::null())
        .args(args);
    // сборка с покрытием: дочерний процесс пишет свой профиль туда же, откуда
    // его потом сливает `cargo llvm-cov`, иначе `main()` остаётся непокрытым
    if let Ok(prof) = std::env::var("LLVM_PROFILE_FILE") {
        cmd.env("LLVM_PROFILE_FILE", prof);
    }
    for (k, v) in extra {
        cmd.env(k, v);
    }
    cmd.output().expect("запуск")
}

/// Подставной Telegram для настоящей команды командной строки: отвечает один и
/// тот же успех на любой метод, считает обращения и хранит пути запросов.
fn fake_telegram() -> (SocketAddr, Arc<Mutex<Vec<String>>>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("порт для подставы");
    let addr = listener.local_addr().expect("адрес подставы");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let (paths, hits) = (seen.clone(), count.clone());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let mut buf: Vec<u8> = Vec::new();
            let mut chunk = [0u8; 4096];
            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            }
            let head = String::from_utf8_lossy(&buf).to_string();
            let path = head
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_string();
            if let Ok(mut seen) = paths.lock() {
                seen.push(path);
            }
            hits.fetch_add(1, Ordering::Relaxed);
            let body = r#"{"ok":true,"result":{"message_id":77}}"#;
            let answer = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(answer.as_bytes());
            let _ = stream.flush();
        }
    });
    (addr, seen, count)
}

#[test]
fn version_prints_without_settings() {
    let out = run(&["--version"], &[]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("claudetg "), "{text}");

    // короткий вариант тоже работает и не требует настроек
    let short = run(&["-V"], &[]);
    assert!(short.status.success(), "{}", String::from_utf8_lossy(&short.stderr));
    assert_eq!(
        String::from_utf8_lossy(&short.stdout).trim(),
        String::from_utf8_lossy(&out.stdout).trim()
    );
}

#[test]
fn missing_token_is_a_clear_error() {
    let out = run(&["send", "11"], &[]);
    assert!(!out.status.success());
    let msg = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(msg.contains("TELEGRAM_BOT_TOKEN"), "{msg}");
}

#[test]
fn unknown_subcommand_never_starts_a_poller() {
    let state = "/tmp/claudetg-cli-test-state/state.json";
    let out = run(
        &["deamon"],
        &[
            ("TELEGRAM_BOT_TOKEN", "123:abc"),
            ("ALLOWED_USER_IDS", "7"),
            ("PROXY_URL", "none"),
            ("STATE_FILE", state),
        ],
    );
    assert!(!out.status.success());
    let msg = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(msg.contains("неизвестная подкоманда"), "{msg}");
    let _ = std::fs::remove_dir_all("/tmp/claudetg-cli-test-state");
}

#[test]
fn send_from_the_command_line_goes_to_telegram_at_once() {
    // бот не работает (pid-файла нет) — `send` отправляет сам, сразу
    let (addr, seen, hits) = fake_telegram();
    let dir = format!("/tmp/claudetg-cli-send-{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    let state = format!("{dir}/state.json");
    let out = run(
        &["send", "11"],
        &[
            ("TELEGRAM_BOT_TOKEN", "123:abc"),
            ("ALLOWED_USER_IDS", "7"),
            ("PROXY_URL", "none"),
            ("CLAUDETG_API_BASE", &format!("http://{addr}")),
            ("STATE_FILE", &state),
        ],
    );
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(out.status.success(), "текст должен уйти: {err}");
    assert!(hits.load(Ordering::Relaxed) >= 1, "в Telegram ходили: {err}");
    assert!(seen.lock().unwrap().iter().any(|p| p.ends_with("/sendMessage")), "{seen:?}");
    // очередь не создавалась: отправлено напрямую
    assert!(!std::path::Path::new(&format!("{dir}/outbox")).exists(), "{err}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn send_file_from_the_command_line_uses_the_chat_from_the_environment() {
    // так файл отправляет агент: чат он не передаёт, его даёт CLAUDETG_CHAT_ID
    let (addr, seen, hits) = fake_telegram();
    let dir = format!("/tmp/claudetg-cli-file-{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let file = format!("{dir}/отчёт.txt");
    std::fs::write(&file, "данные").unwrap();
    let out = run(
        &["send-file", &file, "готовый отчёт"],
        &[
            ("TELEGRAM_BOT_TOKEN", "123:abc"),
            ("ALLOWED_USER_IDS", "7"),
            ("PROXY_URL", "none"),
            ("CLAUDETG_API_BASE", &format!("http://{addr}")),
            ("CLAUDETG_CHAT_ID", "11"),
            ("STATE_FILE", &format!("{dir}/state.json")),
        ],
    );
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(out.status.success(), "файл должен уйти: {err}");
    assert!(hits.load(Ordering::Relaxed) >= 1, "в Telegram ходили: {err}");
    assert!(seen.lock().unwrap().iter().any(|p| p.ends_with("/sendDocument")), "{seen:?}");

    // живой бот (pid-файл на месте): файл ставится в очередь, а не летит в сеть
    // сам — так отдача пережидает лимит Telegram и попадает в журнал
    std::fs::write(format!("{dir}/bot.pid"), "1").expect("pid-файл");
    let before_alive = hits.load(Ordering::Relaxed);
    let out = run(
        &["send-file", &file, "в очередь"],
        &[
            ("TELEGRAM_BOT_TOKEN", "123:abc"),
            ("ALLOWED_USER_IDS", "7"),
            ("PROXY_URL", "none"),
            ("CLAUDETG_API_BASE", &format!("http://{addr}")),
            ("CLAUDETG_CHAT_ID", "11"),
            ("STATE_FILE", &format!("{dir}/state.json")),
        ],
    );
    assert!(out.status.success(), "файл должен встать в очередь: {}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(hits.load(Ordering::Relaxed), before_alive, "в сеть не ходили");
    let entries: Vec<String> = std::fs::read_dir(format!("{dir}/outbox"))
        .expect("очередь создана ботом")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(entries.len(), 1, "файл в очереди: {entries:?}");
    let body = std::fs::read_to_string(format!("{dir}/outbox/{}", entries[0])).unwrap();
    assert!(body.contains("отчёт.txt") && body.contains("в очередь"), "{body}");

    // относительный путь: бот читает очередь из своей директории, поэтому в
    // очередь должен лечь абсолютный путь
    let rel = run(
        &["send-file", "отчёт.txt", "относительный путь"],
        &[
            ("TELEGRAM_BOT_TOKEN", "123:abc"),
            ("ALLOWED_USER_IDS", "7"),
            ("PROXY_URL", "none"),
            ("CLAUDETG_API_BASE", &format!("http://{addr}")),
            ("CLAUDETG_CHAT_ID", "11"),
            ("STATE_FILE", &format!("{dir}/state.json")),
        ],
    );
    assert!(rel.status.success(), "{}", String::from_utf8_lossy(&rel.stderr));
    let entries: Vec<String> = std::fs::read_dir(format!("{dir}/outbox"))
        .expect("очередь")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(entries.len(), 2, "оба файла в очереди: {entries:?}");
    let bodies: Vec<String> = entries
        .iter()
        .map(|n| std::fs::read_to_string(format!("{dir}/outbox/{n}")).unwrap())
        .collect();
    let joined = bodies.join("\n");
    assert!(joined.contains(&format!("\"path\":\"{file}\"")), "путь абсолютный: {joined}");
    let _ = std::fs::remove_file(format!("{dir}/bot.pid"));

    // а без чата команда объясняет, как её вызывать, и не лезет в сеть
    let before = hits.load(Ordering::Relaxed);
    let out = run(
        &["send-file", &file],
        &[
            ("TELEGRAM_BOT_TOKEN", "123:abc"),
            ("ALLOWED_USER_IDS", "7"),
            ("PROXY_URL", "none"),
            ("CLAUDETG_API_BASE", &format!("http://{addr}")),
            ("STATE_FILE", &format!("{dir}/state.json")),
        ],
    );
    assert!(!out.status.success(), "без чата команда обязана ошибаться");
    let msg = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(msg.contains("CLAUDETG_CHAT_ID"), "{msg}");
    assert_eq!(hits.load(Ordering::Relaxed), before, "в сеть не ходили: {msg}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_refused_send_exits_with_an_error() {
    // Telegram отвечает отказом: команда обязана вернуть ошибку, а не «успех»
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let mut buf = [0u8; 8192];
                let _ = stream.read(&mut buf);
                let body = r#"{"ok":false,"description":"Bad Request: chat not found"}"#;
                let _ = stream.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                );
            }
        });
        addr
    };
    let dir = format!("/tmp/claudetg-cli-refused-{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    let out = run(
        &["send", "11"],
        &[
            ("TELEGRAM_BOT_TOKEN", "123:abc"),
            ("ALLOWED_USER_IDS", "7"),
            ("PROXY_URL", "none"),
            ("CLAUDETG_API_BASE", &format!("http://{addr}")),
            ("STATE_FILE", &format!("{dir}/state.json")),
        ],
    );
    assert!(!out.status.success(), "отказ Telegram — ошибка команды");
    let msg = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(msg.contains("не удалось отправить в чат 11"), "{msg}");
    let _ = std::fs::remove_dir_all(&dir);
}
