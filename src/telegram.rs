use anyhow::{bail, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

/// Telegram limit is 4096 UTF-16 units; stay well below it.
const MAX_MESSAGE_CHARS: usize = 3900;

#[derive(Clone)]
pub struct Bot {
    client: reqwest::Client,
    base: String,
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
    pub chat: Chat,
    pub from: Option<User>,
    pub text: Option<String>,
    pub caption: Option<String>,
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
        let text = if text.trim().is_empty() { "(пустой ответ)" } else { text };
        for chunk in split_message(text, MAX_MESSAGE_CHARS) {
            let res: Result<Value> = self
                .call("sendMessage", json!({ "chat_id": chat_id, "text": chunk }))
                .await;
            if let Err(e) = res {
                log::error!("sendMessage to {chat_id}: {e:#}");
            }
        }
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
        let text = if text.trim().is_empty() { "(пустой ответ)" } else { text };
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
            if let Err(e) = res {
                log::warn!("sendMessage HTML to {chat_id} rejected, sending plain: {e:#}");
                self.send_text(chat_id, &plain).await;
            }
        }
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
    use super::split_message;

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
}
