use anyhow::{anyhow, bail, Context, Result};
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
    /// Запасной маршрут: если задан прокси, здесь прямой клиент (и наоборот).
    /// Прокси к Telegram иногда «зависает» на CONNECT, а прямой путь работает.
    fallback: Option<reqwest::Client>,
    base: String,
    /// Токен нужен и для вызовов методов, и для загрузки файлов (`/file/bot<token>/…`).
    token: String,
}

/// Telegram принял запрос, но отказал по делу: неверный текст, лимит, права.
/// Такое повторять бесполезно — в отличие от сетевой неполадки.
#[derive(Debug)]
struct ApiRejected(String);

impl std::fmt::Display for ApiRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ApiRejected {}

/// Не удалось соединиться (или запрос оборвался на транспорте): запрос не ушёл,
/// повторять безопасно.
fn is_connect_err(e: &reqwest::Error) -> bool {
    e.is_connect()
}

/// Повторять ли отправку: отказ Telegram по существу повторяют зря, сетевую
/// неполадку — нужно.
fn retryable(e: &anyhow::Error) -> bool {
    e.downcast_ref::<ApiRejected>().is_none()
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
    /// Telegram отдаёт id сообщения в поле `message_id`.
    #[serde(rename = "message_id")]
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

/// Ответ метода getFile: нужен только путь к файлу на сервере Telegram,
/// сам ответ — объект, а не строка.
#[derive(Debug, Deserialize)]
struct FileLocation {
    #[serde(default)]
    file_path: String,
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
        let builder = || {
            reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(20))
                // Must exceed the long-polling timeout used in get_updates.
                .timeout(Duration::from_secs(90))
        };
        // Основной клиент — как настроено, запасной — другой маршрут: прокси к
        // Telegram иногда зависает на CONNECT, а прямой путь в это время работает.
        let (client, fallback) = match proxy {
            Some(p) => (
                builder().proxy(reqwest::Proxy::all(p)?).build()?,
                Some(builder().no_proxy().build()?),
            ),
            None => (builder().no_proxy().build()?, None),
        };
        Ok(Self {
            client,
            fallback,
            base: format!("https://api.telegram.org/bot{token}"),
            token: token.to_string(),
        })
    }

    /// Маршруты обхода: сначала настроенный, потом запасной.
    fn routes(&self) -> Vec<&reqwest::Client> {
        let mut out = vec![&self.client];
        if let Some(f) = &self.fallback {
            out.push(f);
        }
        out
    }

    /// Вырезает токен из текста: reqwest подставляет в ошибку полный URL вида
    /// `…/bot<токен>/метод`, а ошибки мы пишем и в журнал, и в чат — токен
    /// светиться там не должен ни при каких обстоятельствах.
    fn scrub(&self, text: String) -> String {
        text.replace(&self.token, "[токен]")
    }

    /// Сетевая ошибка «как есть», но без токена: цепочка сохраняется целиком.
    fn shown<E: std::error::Error + Send + Sync + 'static>(&self, e: E) -> anyhow::Error {
        anyhow::anyhow!(self.scrub(format!("{:#}", anyhow::Error::new(e))))
    }

    async fn call<T: DeserializeOwned>(&self, method: &str, body: Value) -> Result<T> {
        let url = format!("{}/{method}", self.base);
        let mut last: Option<reqwest::Error> = None;
        for client in self.routes() {
            match client.post(&url).json(&body).send().await {
                Ok(resp) => {
                    let resp: ApiResponse<T> = resp
                        .json()
                        .await
                        .map_err(|e| self.shown(e))
                        .with_context(|| format!("{method}: ответ не разбирается"))?;
                    return match (resp.ok, resp.result) {
                        (true, Some(r)) => Ok(r),
                        _ => Err(ApiRejected(format!(
                            "{method} failed: {}",
                            resp.description.unwrap_or_else(|| "unknown error".into())
                        ))
                        .into()),
                    };
                }
                Err(e) if is_connect_err(&e) => {
                    log::debug!("{method}: маршрут не дошёл, пробуем следующий");
                    last = Some(e);
                }
                Err(e) => return Err(self.shown(e)),
            }
        }
        Err(last
            .map(|e| self.shown(e))
            .unwrap_or_else(|| anyhow::anyhow!("{method}: нет сетевого маршрута")))
    }

    /// Отправка сообщения с повторами: сетевой сбой не должен обрывать
    /// переписку. Отказ по существу (лимит, неверный текст) не повторяем.
    async fn call_send(&self, method: &str, body: Value) -> Result<Value> {
        for attempt in 1..=3u32 {
            match self.call::<Value>(method, body.clone()).await {
                Ok(v) => return Ok(v),
                Err(e) if attempt == 3 || !retryable(&e) => {
                    return Err(e)
                }
                Err(e) => {
                    log::warn!("{method}: не отправлено, попытка {attempt} из 3: {e:#}");
                    tokio::time::sleep(Duration::from_millis(1200 * u64::from(attempt))).await;
                }
            }
        }
        unreachable!()
    }

    pub async fn get_me(&self) -> Result<Value> {
        self.call("getMe", json!({})).await
    }

    /// Updates приходят «пакетом»: одно кривое обновление не должно блокировать
    /// очередь насовсем (offset не двигается). Не декодировалось — ругаемся и
    /// пропускаем, offset по нему всё равно уходит вперёд.
    pub async fn get_updates(&self, offset: i64) -> Result<Vec<Update>> {
        let raw: Vec<Value> = self
            .call(
                "getUpdates",
                json!({ "offset": offset, "timeout": 50, "allowed_updates": ["message"] }),
            )
            .await?;
        Ok(parse_updates(raw))
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
    /// Ошибка приходит только если не ушло ни одной части: тогда очередь
    /// остаётся целой и сообщение дойдёт позже.
    pub async fn send_text_ids(&self, chat_id: i64, text: &str) -> Result<Vec<i64>> {
        let text = if text.trim().is_empty() { "(пустой ответ)" } else { text };
        let mut ids = Vec::new();
        for chunk in split_message(text, MAX_MESSAGE_CHARS) {
            let res = self
                .call_send("sendMessage", json!({ "chat_id": chat_id, "text": chunk }))
                .await;
            match res {
                Ok(v) => {
                    if let Some(id) = v.get("message_id").and_then(Value::as_i64) {
                        ids.push(id);
                    }
                }
                Err(e) if ids.is_empty() => {
                    log::error!("sendMessage to {chat_id}: {e:#}");
                    return Err(e);
                }
                Err(e) => log::error!("sendMessage to {chat_id}: часть сообщения не ушла: {e:#}"),
            }
        }
        Ok(ids)
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
    /// Сетевой сбой повторяем до трёх раз с обоими маршрутами: файлы из чата
    /// не должны тершаться из-за одного таймаута соединения.
    pub async fn download(&self, file: &FileRef, dest: &Path) -> Result<u64> {
        if file.file_size.unwrap_or(0) > 20 * 1024 * 1024 {
            bail!(
                "файл больше 20 МБ — Bot API такие не отдаёт (пришли его, пожалуйста, ссылкой или частями)"
            );
        }
        let mut last: Option<anyhow::Error> = None;
        for attempt in 1..=3u64 {
            match self.download_once(file, dest).await {
                Ok(size) => return Ok(size),
                // Telegram отказал по делу (файл протух) — повторять бессмысленно
                Err(e) if e.downcast_ref::<ApiRejected>().is_some() => return Err(e),
                Err(e) => {
                    log::warn!("скачивание файла: попытка {attempt} из 3: {e:#}");
                    last = Some(e);
                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_millis(1200 * attempt)).await;
                    }
                }
            }
        }
        Err(last.context("не скачали файл из Telegram")?)
    }

    async fn download_once(&self, file: &FileRef, dest: &Path) -> Result<u64> {
        // getFile отвечает объектом File: путь лежит в поле file_path (не строкой!).
        let got: FileLocation = self
            .call("getFile", json!({ "file_id": file.file_id }))
            .await
            .context("getFile: бот не получил адрес файла")?;
        if got.file_path.is_empty() {
            return Err(ApiRejected(
                "файл на сервере Telegram закончился: пришли его ещё раз".into(),
            )
            .into());
        }
        // Адрес файла по документации Bot API: https://api.telegram.org/file/bot<token>/<path>.
        // Токен в URL неизбежен — но в тексты ошибок его подставляет shown().
        let url = format!(
            "https://api.telegram.org/file/bot{}/{}",
            self.token, got.file_path
        );
        let mut last: Option<anyhow::Error> = None;
        for client in self.routes() {
            match client.get(&url).timeout(Duration::from_secs(600)).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let bytes = resp.bytes().await.map_err(|e| self.shown(e))?;
                    if let Some(dir) = dest.parent() {
                        tokio::fs::create_dir_all(dir).await.ok();
                    }
                    tokio::fs::write(dest, &bytes).await.context("не записать файл на диск")?;
                    return Ok(bytes.len() as u64);
                }
                Ok(resp) => last = Some(anyhow!("скачать файл не вышло: HTTP {}", resp.status())),
                Err(e) => {
                    log::debug!("скачивание файла: маршрут не дошёл, пробуем следующий");
                    last = Some(self.shown(e));
                }
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("скачивание файла: нет сетевого маршрута")))
    }
}

/// Разбор пакета обновлений: битое обновление не блокирует очередь — вместо
/// него пустое (без сообщения), но offset по нему всё равно уходит вперёд.
fn parse_updates(raw: Vec<Value>) -> Vec<Update> {
    let mut out = Vec::with_capacity(raw.len());
    for value in raw {
        match serde_json::from_value::<Update>(value.clone()) {
            Ok(update) => out.push(update),
            Err(e) => {
                let id = value.get("update_id").and_then(Value::as_i64).unwrap_or_default();
                log::warn!("update {id} пропущен: {e}");
                out.push(Update { update_id: id, message: None });
            }
        }
    }
    out
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
        // Файл идёт долго, поэтому маршрутом тоже перебираем: зависший прокси
        // не должен стоить доставки APK.
        let mut last: Option<reqwest::Error> = None;
        for client in self.routes() {
            let mut form = reqwest::multipart::Form::new()
                .text("chat_id", chat_id.to_string())
                .part(
                    "document",
                    reqwest::multipart::Part::bytes(bytes.clone()).file_name(name.clone()),
                );
            if let Some(c) = caption {
                form = form.text("caption", c.to_string());
            }
            let send = client
                .post(format!("{}/sendDocument", self.base))
                .multipart(form)
                .timeout(Duration::from_secs(600))
                .send()
                .await;
            match send {
                Ok(resp) => {
                    let resp: ApiResponse<Value> = resp
                        .json()
                        .await
                        .map_err(|e| self.shown(e))
                        .context("sendDocument: ответ не разбирается")?;
                    if !resp.ok {
                        bail!(
                            "sendDocument failed: {}",
                            resp.description.unwrap_or_else(|| "unknown error".into())
                        );
                    }
                    return Ok(());
                }
                Err(e) if is_connect_err(&e) => {
                    log::debug!("sendDocument: маршрут не дошёл, пробуем следующий");
                    last = Some(e);
                }
                Err(e) => return Err(self.shown(e)),
            }
        }
        Err(last
            .map(|e| self.shown(e))
            .unwrap_or_else(|| anyhow::anyhow!("sendDocument: нет сетевого маршрута")))
    }

    /// Ответ Claude: Markdown → HTML Telegram. Если Telegram отверг разметку
    /// (неожиданная конструкция), этот кусок уходит обычным текстом — ответ не
    /// теряется. Предел меньше обычного: HTML-теги удлиняют текст.
    pub async fn send_markdown(&self, chat_id: i64, text: &str) {
        let _ = self.send_markdown_ids(chat_id, text).await;
    }

    /// Тот же ответ, но возвращает id отправленных сообщений.
    pub async fn send_markdown_ids(&self, chat_id: i64, text: &str) -> Result<Vec<i64>> {
        let text = if text.trim().is_empty() { "(пустой ответ)" } else { text };
        let mut ids = Vec::new();
        for (html, plain) in crate::markdown::to_html_chunks(text, 3000) {
            let res = self
                .call_send(
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
                Err(e) if e.downcast_ref::<ApiRejected>().is_some() => {
                    log::warn!("sendMessage HTML to {chat_id} rejected, sending plain: {e:#}");
                    ids.extend(self.send_text_ids(chat_id, &plain).await?);
                }
                Err(e) if ids.is_empty() => {
                    log::error!("sendMessage to {chat_id}: {e:#}");
                    return Err(e);
                }
                Err(e) => log::error!("sendMessage to {chat_id}: часть сообщения не ушла: {e:#}"),
            }
        }
        Ok(ids)
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
    use super::{parse_updates, safe_name, split_message};

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
    fn error_text_never_carries_the_token() {
        // reqwest подставляет в ошибку полный URL с токеном — в журнал и чат
        // должна уходить вычищенная строка
        let bot = super::Bot::new("123456:SEKRET-t0ken", None).expect("бот");
        let net = std::io::Error::other(
            "error sending request for url (https://api.telegram.org/bot123456:SEKRET-t0ken/getFile)",
        );
        let shown = format!("{:#}", bot.shown(net));
        assert!(shown.contains("error sending request"), "цепочка сохранена: {shown}");
        assert!(!shown.contains("SEKRET"), "токен вычищен: {shown}");
        assert!(shown.contains("[токен]"));
    }

    #[test]
    fn network_route_falls_back_when_proxy_is_set() {        // с прокси обязателен запасной маршрут (напрямую), без прокси — не нужен
        let with_proxy = super::Bot::new("t", Some("http://localhost:3128")).expect("бот");
        assert!(with_proxy.fallback.is_some());
        assert_eq!(with_proxy.routes().len(), 2);

        let direct = super::Bot::new("t", None).expect("бот");
        assert!(direct.fallback.is_none());
        assert_eq!(direct.routes().len(), 1);
    }

    #[test]
    fn rejects_are_not_retried_but_network_fails_are() {
        let rejected: anyhow::Error =
            super::ApiRejected("sendMessage failed: bad request".into()).into();
        assert!(!super::retryable(&rejected));
        assert!(format!("{rejected}").contains("bad request"));

        let network = anyhow::anyhow!("error sending request for url");
        assert!(super::retryable(&network));
    }

    #[test]
    fn get_file_answer_is_an_object_with_a_path() {
        // getFile отвечает объектом File; раньше код ждал строку и падал с
        // «invalid type: map, expected a string» — файлы из чата не скачивались.
        let got: super::FileLocation = serde_json::from_value(serde_json::json!({
            "type": "document",
            "file_id": "ABC",
            "file_unique_id": "xyz",
            "file_size": 11780,
            "file_path": "documents/file_3.xlsx",
        }))
        .expect("объект File разбирается");
        assert_eq!(got.file_path, "documents/file_3.xlsx");

        // просроченный file_id: объект без file_path — не паника, а понятный ответ
        let empty: super::FileLocation =
            serde_json::from_value(serde_json::json!({ "type": "document" })).expect("пустой объект");
        assert!(empty.file_path.is_empty());
    }

    #[test]
    fn update_message_exposes_attachments() {
        let raw = serde_json::json!({
            "update_id": 41,
            "message": {
                "message_id": 42,
                "chat": { "id": 1 },
                "from": { "id": 2 },
                "caption": "смоти файл",
                "document": { "file_id": "DOC", "file_name": "apk-release.apk", "file_size": 12 },
                "photo": [
                    { "file_id": "SMALL", "file_size": 100 },
                    { "file_id": "BIG", "file_size": 9000 }
                ]
            }
        });
        let updates = parse_updates(vec![raw]);
        let msg = updates[0].message.as_ref().expect("сообщение разобрано");
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
            "update_id": 6,
            "message": { "message_id": 7, "chat": { "id": 1 }, "from": { "id": 2 }, "text": "привет" }
        });
        let updates = parse_updates(vec![raw]);
        assert!(updates[0].message.as_ref().unwrap().attachments().is_empty());
    }

    #[test]
    fn broken_update_is_skipped_but_offset_moves_on() {
        // первое обновление битое (нет chat), второе — обычное
        let bad = serde_json::json!({ "update_id": 10, "message": { "message_id": 11 } });
        let good = serde_json::json!({
            "update_id": 12,
            "message": { "message_id": 13, "chat": { "id": 1 }, "text": "дальше" }
        });
        let updates = parse_updates(vec![bad, good]);
        assert_eq!(updates.len(), 2);
        assert!(updates[0].message.is_none());
        assert_eq!(updates[1].update_id, 12);
    }
}
