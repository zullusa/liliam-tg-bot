use anyhow::{bail, Context, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Telegram limit is 4096 UTF-16 units; stay well below it.
const MAX_MESSAGE_CHARS: usize = 3900;

#[derive(Clone)]
pub struct Bot {
    client: reqwest::Client,
    base: String,
    /// Токен нужен и для вызовов методов, и для загрузки файлов (`/file/bot<token>/…`).
    token: String,
}

#[derive(Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub id: i64,
    pub chat: Chat,
    pub from: Option<User>,
    pub text: Option<String>,
    pub caption: Option<String>,
    /// Вложения, которые умеем скачивать: документ, фото, видео, аудио,
    /// голосовое, анимация, стикер. У фото несколько размеров — берём крупный.
    pub document: Option<FileRef>,
    #[serde(default)]
    pub photo: Vec<FileRef>,
    pub video: Option<FileRef>,
    pub audio: Option<FileRef>,
    pub voice: Option<FileRef>,
    pub animation: Option<FileRef>,
    pub sticker: Option<FileRef>,
}

/// Ссылка на файл в Telegram: `file_id` меняет адрес по `getFile`.
#[derive(Debug, Deserialize)]
pub struct FileRef {
    pub file_id: String,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub file_size: Option<u64>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// Что скачали из чата: локальный путь плюс исходные имя и размер.
#[derive(Debug, Clone)]
pub struct Attachment {
    pub local: PathBuf,
    pub name: String,
    pub kind: &'static str,
    pub size: u64,
}

impl Message {
    /// Вложения сообщения в порядке Telegram: у фото берём последний размер
    /// (самый крупный), имена файлов берём из `file_name`, для фото его нет —
    /// подставляем расширение по mime.
    pub fn attachments(&self) -> Vec<&FileRef> {
        let mut out: Vec<&FileRef> = Vec::new();
        for f in [&self.document, &self.video, &self.audio, &self.voice, &self.animation, &self.sticker]
            .into_iter()
            .flatten()
        {
            out.push(f);
        }
        out.extend(self.photo.last());
        out
    }
}

#[derive(Debug, Deserialize)]
pub struct Chat {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct User {
    pub id: i64,
}

impl Bot {
    pub fn new(token: &str, proxy: Option<&str>) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(20))
            // Must exceed the long-polling timeout used in get_updates.
            .timeout(Duration::from_secs(90));
        if let Some(p) = proxy {
            builder = builder.proxy(reqwest::Proxy::all(p)?);
        } else {
            builder = builder.no_proxy();
        }
        Ok(Self {
            client: builder.build()?,
            base: format!("https://api.telegram.org/bot{token}"),
            token: token.to_string(),
        })
    }

    async fn call<T: DeserializeOwned>(&self, method: &str, body: Value) -> Result<T> {
        let resp: ApiResponse<T> = self
            .client
            .post(format!("{}/{method}", self.base))
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        match (resp.ok, resp.result) {
            (true, Some(r)) => Ok(r),
            _ => bail!(
                "{method} failed: {}",
                resp.description.unwrap_or_else(|| "unknown error".into())
            ),
        }
    }

    pub async fn get_me(&self) -> Result<Value> {
        self.call("getMe", json!({})).await
    }

    pub async fn get_updates(&self, offset: i64) -> Result<Vec<Update>> {
        self.call(
            "getUpdates",
            json!({ "offset": offset, "timeout": 50, "allowed_updates": ["message"] }),
        )
        .await
    }

    pub async fn send_typing(&self, chat_id: i64) {
        let _: Result<bool> = self
            .call("sendChatAction", json!({ "chat_id": chat_id, "action": "typing" }))
            .await;
    }

    /// Sends text as plain text (no parse_mode, so Claude's markdown can't break
    /// the request), split into several messages when needed.
    pub async fn send_text(&self, chat_id: i64, text: &str) {
        let _ = self.send_text_ids(chat_id, text).await;
    }

    /// То же, но возвращает id отправленных сообщений — по ним чистим чат.
    pub async fn send_text_ids(&self, chat_id: i64, text: &str) -> Vec<i64> {
        let text = if text.trim().is_empty() { "(пустой ответ)" } else { text };
        let mut ids = Vec::new();
        for chunk in split_message(text, MAX_MESSAGE_CHARS) {
            let res: Result<Value> = self
                .call("sendMessage", json!({ "chat_id": chat_id, "text": chunk }))
                .await;
            match res {
                Ok(v) => {
                    if let Some(id) = v.get("message_id").and_then(Value::as_i64) {
                        ids.push(id);
                    }
                }
                Err(e) => log::error!("sendMessage to {chat_id}: {e:#}"),
            }
        }
        ids
    }

    /// Правит своё же сообщение (new message_id тот же). Возвращает false, если
    /// правка не удалась — например, сообщение уже удалено: тогда пузырь статуса
    /// создадут заново. «message is not modified» считаем успехом.
    pub async fn edit_text(&self, chat_id: i64, message_id: i64, text: &str) -> bool {
        let res: Result<Value> = self
            .call(
                "editMessageText",
                json!({ "chat_id": chat_id, "message_id": message_id, "text": text }),
            )
            .await;
        match res {
            Ok(_) => true,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("message is not modified") {
                    return true;
                }
                log::debug!("editMessageText {message_id}: {msg}");
                false
            }
        }
    }

    /// Удаляет своё сообщение из чата. Чужие сообщения в личной переписке бот
    /// не удаляет — Telegram ответит ошибкой, и она не страшно: просто false.
    pub async fn delete(&self, chat_id: i64, message_id: i64) -> bool {
        let res: Result<bool> = self
            .call("deleteMessage", json!({ "chat_id": chat_id, "message_id": message_id }))
            .await;
        match res {
            Ok(true) => true,
            Ok(false) => false,
            Err(e) => {
                log::debug!("deleteMessage {message_id}: {e:#}");
                false
            }
        }
    }

    /// Скачивает вложение из чата в `dest`. Ограничение Bot API — 20 МБ на файл;
    /// если файл больше, ругаемся внятной строкой, а не сетевой ошибкой.
    pub async fn download(&self, file: &FileRef, dest: &Path) -> Result<u64> {
        if file.file_size.unwrap_or(0) > 20 * 1024 * 1024 {
            bail!(
                "файл больше 20 МБ — Bot API такие не отдаёт (пришли его, пожалуйста, ссылкой или частями)"
            );
        }
        let file_path: String = self
            .call("getFile", json!({ "file_id": file.file_id }))
            .await
            .context("getFile: бот не получил адрес файла")?;
        // Адрес файла по документации Bot API: https://api.telegram.org/file/bot<token>/<path>.
        let url = format!("https://api.telegram.org/file/bot{}/{}", self.token, file_path);
        let resp = self
            .client
            .get(&url)
            .timeout(Duration::from_secs(600))
            .send()
            .await
            .context("не скачали файл из Telegram")?;
        if !resp.status().is_success() {
            bail!("скачать файл не вышло: HTTP {}", resp.status());
        }
        let bytes = resp.bytes().await.context("обрыв загрузки файла")?;
        if let Some(dir) = dest.parent() {
            tokio::fs::create_dir_all(dir).await.ok();
        }
        tokio::fs::write(dest, &bytes).await.context("не записать файл на диск")?;
        Ok(bytes.len() as u64)
    }
}

/// Имя файла для диска: Telegram может прислать что угодно («../../etc/passwd»,
/// «\», имя с переносом строки). Оставляем только безопасные символы, пустое имя
/// меняем на запасное.
pub fn safe_name(raw: Option<&str>, fallback: &str) -> String {
    let base = raw
        .map(|s| s.rsplit(['/', '\\']).next().unwrap_or(""))
        .unwrap_or("")
        .trim();
    let cleaned: String = base
        .chars()
        .map(|c| match c {
            ' '..='~' if !matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') => c,
            'а'..='я' | 'А'..='Я' | 'ё' | 'Ё' => c,
            _ => '_',
        })
        .collect();
    let cleaned = cleaned.trim_matches(|c: char| c == '.' || c == '_').to_string();
    let cleaned: String = cleaned.chars().take(120).collect();
    if cleaned.is_empty() || cleaned == ".." {
        fallback.to_string()
    } else {
        cleaned
    }
}

impl Bot {
    /// Файл документом (sendDocument, до 50 МБ). Таймаут длиннее обычного:
    /// большой файл через прокси идёт долго.
    pub async fn send_document(
        &self,
        chat_id: i64,
        path: &std::path::Path,
        caption: Option<&str>,
    ) -> Result<()> {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string();
        let bytes = tokio::fs::read(path).await?;
        if bytes.len() > 50 * 1024 * 1024 {
            bail!("{name}: больше 50 МБ — Telegram не примет");
        }
        let mut form = reqwest::multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            .part("document", reqwest::multipart::Part::bytes(bytes).file_name(name));
        if let Some(c) = caption {
            form = form.text("caption", c.to_string());
        }
        let resp: ApiResponse<Value> = self
            .client
            .post(format!("{}/sendDocument", self.base))
            .multipart(form)
            .timeout(Duration::from_secs(600))
            .send()
            .await?
            .json()
            .await?;
        if !resp.ok {
            bail!(
                "sendDocument failed: {}",
                resp.description.unwrap_or_else(|| "unknown error".into())
            );
        }
        Ok(())
    }

    /// Ответ Claude: Markdown → HTML Telegram. Если Telegram отверг разметку
    /// (неожиданная конструкция), этот кусок уходит обычным текстом — ответ не
    /// теряется. Предел меньше обычного: HTML-теги удлиняют текст.
    pub async fn send_markdown(&self, chat_id: i64, text: &str) {
        let _ = self.send_markdown_ids(chat_id, text).await;
    }

    /// Тот же ответ, но возвращает id отправленных сообщений.
    pub async fn send_markdown_ids(&self, chat_id: i64, text: &str) -> Vec<i64> {
        let text = if text.trim().is_empty() { "(пустой ответ)" } else { text };
        let mut ids = Vec::new();
        for (html, plain) in crate::markdown::to_html_chunks(text, 3000) {
            let res: Result<Value> = self
                .call(
                    "sendMessage",
                    json!({
                        "chat_id": chat_id,
                        "text": html,
                        "parse_mode": "HTML",
                        "link_preview_options": { "is_disabled": true },
                    }),
                )
                .await;
            match res {
                Ok(v) => {
                    if let Some(id) = v.get("message_id").and_then(Value::as_i64) {
                        ids.push(id);
                    }
                }
                Err(e) => {
                    log::warn!("sendMessage HTML to {chat_id} rejected, sending plain: {e:#}");
                    ids.extend(self.send_text_ids(chat_id, &plain).await);
                }
            }
        }
        ids
    }
}

/// Splits on line boundaries where possible, never inside a UTF-8 char.
fn split_message(text: &str, max: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_len = 0;
    for line in text.split_inclusive('\n') {
        let line_len = line.chars().count();
        if current_len + line_len > max && !current.is_empty() {
            chunks.push(std::mem::take(&mut current));
            current_len = 0;
        }
        if line_len > max {
            for c in line.chars() {
                if current_len == max {
                    chunks.push(std::mem::take(&mut current));
                    current_len = 0;
                }
                current.push(c);
                current_len += 1;
            }
        } else {
            current.push_str(line);
            current_len += line_len;
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::{safe_name, split_message, Message};

    #[test]
    fn splits_long_text() {
        let text = "абв\n".repeat(10);
        let chunks = split_message(&text, 9);
        assert!(chunks.iter().all(|c| c.chars().count() <= 9));
        assert_eq!(chunks.concat(), text);

        let long = "я".repeat(25);
        let chunks = split_message(&long, 10);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks.concat(), long);
    }

    #[test]
    fn file_names_are_safe_for_disk() {
        // путь наружу превращается в имя файла без каталогов
        assert_eq!(safe_name(Some("../../etc/passwd"), "f"), "passwd");
        assert_eq!(safe_name(Some("c:\\temp\\отчёт.pdf"), "f"), "отчёт.pdf");
        assert_eq!(safe_name(Some("   "), "f"), "f");
        assert_eq!(safe_name(Some("...."), "f"), "f");
        assert_eq!(safe_name(None, "f"), "f");
        assert_eq!(safe_name(Some("a\nb"), "f").chars().count(), 3);
    }

    #[test]
    fn update_message_exposes_attachments() {
        let raw = serde_json::json!({
            "id": 42,
            "chat": { "id": 1 },
            "from": { "id": 2 },
            "caption": "смоти файл",
            "document": { "file_id": "DOC", "file_name": "apk-release.apk", "file_size": 12 },
            "photo": [
                { "file_id": "SMALL", "file_size": 100 },
                { "file_id": "BIG", "file_size": 9000 }
            ]
        });
        let msg: Message = serde_json::from_value(raw).unwrap();
        assert_eq!(msg.id, 42);
        assert_eq!(msg.caption.as_deref(), Some("смоти файл"));
        let files = msg.attachments();
        // порядок: документ, затем самый крупный размер фото
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].file_id, "DOC");
        assert_eq!(files[0].file_name.as_deref(), Some("apk-release.apk"));
        assert_eq!(files[1].file_id, "BIG");
    }

    #[test]
    fn text_only_message_has_no_attachments() {
        let raw = serde_json::json!({
            "id": 7, "chat": { "id": 1 }, "from": { "id": 2 }, "text": "привет"
        });
        let msg: Message = serde_json::from_value(raw).unwrap();
        assert!(msg.attachments().is_empty());
    }
}
