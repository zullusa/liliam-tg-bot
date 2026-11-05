use anyhow::{anyhow, bail, Context, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

/// Telegram limit is 4096 UTF-16 units; stay well below it.
const MAX_MESSAGE_CHARS: usize = 3900;

#[derive(Debug)]
pub struct Bot {
    client: reqwest::Client,
    /// Запасной маршрут: если задан прокси, здесь прямой клиент (и наоборот).
    /// Прокси к Telegram иногда «зависает» на CONNECT, а прямой путь работает.
    fallback: Option<reqwest::Client>,
    base: String,
    /// Значение CLAUDETG_API_BASE: тем же путём, что методы, ходят и скачивание
    /// файлов; в тестах сюда подставляют подставной Telegram.
    api_base: Option<String>,
    /// Токен нужен и для вызовов методов, и для загрузки файлов (`/file/bot<token>/…`).
    token: String,
    /// Когда можно снова слать в Telegram (мс с эпохи): ответ «Too Many Requests»
    /// приносит `retry_after` — пока срок не истёк, любые попытки только
    /// продлевают блокировку, и очередь бота её долбит.
    paused_until_ms: AtomicI64,
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
    // обрыв посередине запроса (прокси принял соединение и молча закрыл его) —
    // тот же сбой маршрута, что и отказ в подключении: нужен запасной маршрут
    e.is_connect() || e.is_request()
}

/// Повторять ли отправку: отказ Telegram по существу повторяют зря, сетевую
/// неполадку — нужно.
fn retryable(e: &anyhow::Error) -> bool {
    e.downcast_ref::<ApiRejected>().is_none()
}

#[derive(Deserialize)]
struct FloodParams {
    /// Секунды, которые Telegram просит выждать (поле `retry_after`).
    #[serde(default)]
    retry_after: u64,
}

#[derive(Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
    #[serde(default)]
    parameters: Option<FloodParams>,
}

/// Наказание от Telegram за частые запросы («Too Many Requests»): сколько
/// секунд нельзя слать. Без паузы каждая повторная попытка только продлевает
/// бан — бот сутками долбит API и не может отправить очередь.
fn pause_ms(description: &str, params: Option<&FloodParams>) -> Option<u64> {
    let wait = params.map(|p| p.retry_after).filter(|s| *s > 0)?;
    if description.contains("Too Many Requests") {
        Some(wait * 1000)
    } else {
        None
    }
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
    pub fn new(token: &str, proxy: Option<&str>, base: Option<&str>) -> Result<Self> {
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
            // адрес методов: по умолчанию официальный Bot API, своя строка — для
            // тестов (подставной Telegram) и самодельных прокси
            api_base: base.map(str::to_string),
            base: base
                .map(|b| format!("{}/bot{token}", b.trim_end_matches('/')))
                .unwrap_or_else(|| format!("https://api.telegram.org/bot{token}")),
            token: token.to_string(),
            paused_until_ms: AtomicI64::new(0),
        })
    }

    /// Можно ли слать: Telegram мог ответить «ждите N секунд», и до конца этого
    /// срока любая попытка лишь удлиняет запрет. Очередь выжидает молча.
    pub fn send_paused(&self) -> bool {
        self.paused_until_ms.load(Ordering::Relaxed) > crate::outbox::now_ms()
    }

    /// Срок паузы в мс с эпохи (для журнала и тестов).
    pub fn paused_until(&self) -> i64 {
        self.paused_until_ms.load(Ordering::Relaxed)
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
                        _ => {
                            let description = resp
                                .description
                                .unwrap_or_else(|| "unknown error".into());
                            // «ждите N секунд» — заводим паузу: попытки внутри этого
                            // срока лишь удлиняют запрет (проверено на живом лимите)
                            if let Some(ms) = pause_ms(&description, resp.parameters.as_ref()) {
                                self.paused_until_ms
                                    .store(crate::outbox::now_ms() + ms as i64, Ordering::Relaxed);
                                log::warn!(
                                    "{method}: Telegram просит паузу {} с, очередь ждёт",
                                    ms / 1000
                                );
                            }
                            Err(ApiRejected(format!("{method} failed: {description}")).into())
                        }
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
        // Во время наказания от Telegram запросы слать нельзя: они и не проходят,
        // и лишь отмеряют новый срок. Отказ отдаём наружу — очередь останется целой.
        if self.send_paused() {
            return Err(ApiRejected(format!(
                "{method}: пауза Telegram ещё {} с",
                (self.paused_until() - crate::outbox::now_ms()) / 1000
            ))
            .into());
        }
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
        if self.send_paused() {
            return;
        }
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
        if self.send_paused() {
            return false;
        }
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
        if self.send_paused() {
            return false;
        }
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

    /// Адрес файла по документации Bot API: тот же хост и токен, что у методов,
    /// только путь начинается с `/file/`. Токен в URL неизбежен — но в тексты
    /// ошибок его подставляет shown().
    fn file_url(&self, path: &str) -> String {
        let root = self
            .api_base
            .clone()
            .unwrap_or_else(|| self.base.replacen(&format!("/bot{}", self.token), "", 1));
        format!("{root}/file/bot{}/{path}", self.token)
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
        let url = self.file_url(&got.file_path);
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
    /// Файл документом. Возвращает id отправленного сообщения — по нему чистка
    /// убирает файл из чата. Отказ «Too Many Requests» заводит паузу, как и
    /// обычные сообщения: иначе длинная загрузка APK лишь отмеряет новый срок.
    pub async fn send_document(
        &self,
        chat_id: i64,
        path: &std::path::Path,
        caption: Option<&str>,
    ) -> Result<i64> {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string();
        if self.send_paused() {
            return Err(ApiRejected(format!(
                "sendDocument: пауза Telegram ещё {} с",
                (self.paused_until() - crate::outbox::now_ms()) / 1000
            ))
            .into());
        }
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
                        let description =
                            resp.description.unwrap_or_else(|| "unknown error".into());
                        // «ждите N секунд» — заводим паузу: новая загрузка файла
                        // внутри этого срока только продлевает запрет
                        if let Some(ms) = pause_ms(&description, resp.parameters.as_ref()) {
                            self.paused_until_ms
                                .store(crate::outbox::now_ms() + ms as i64, Ordering::Relaxed);
                            log::warn!(
                                "sendDocument: Telegram просит паузу {} с, файл ждёт",
                                ms / 1000
                            );
                        }
                        bail!("sendDocument failed: {description}");
                    }
                    let message_id = resp
                        .result
                        .as_ref()
                        .and_then(|v| v.get("message_id"))
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    return Ok(message_id);
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
                // лимит Telegram: запасной вариант «обычным текстом» тоже упрётся
                // в запрет и только удлинит его — отдаём ошибку как есть, очередь
                // останется целой и дойдёт после паузы
                Err(e) if self.send_paused() => {
                    log::error!("sendMessage to {chat_id}: {e:#}");
                    return Err(e);
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
    use super::{pause_ms, Bot, FileRef, FloodParams, parse_updates, safe_name, split_message};
    use crate::testkit::FakeTg;

    #[test]
    fn flood_wait_answers_are_understood() {
        let params = FloodParams { retry_after: 43 };
        // «Too Many Requests: retry after 43» — пауза 43 секунды
        assert_eq!(pause_ms("Too Many Requests: retry after 43", Some(&params)), Some(43_000));
        // другой отказ — никакой паузы, даже если Telegram прислал retry_after
        assert_eq!(pause_ms("Bad Request: message is too long", Some(&params)), None);
        // лимит без параметров — тоже не пауза (чтобы не встать навечно)
        assert_eq!(pause_ms("Too Many Requests", None), None);
        // retry_after=0 — доверия нет, паузу не заводим
        assert_eq!(pause_ms("Too Many Requests", Some(&FloodParams { retry_after: 0 })), None);
    }

    #[tokio::test]
    async fn text_send_goes_through_and_returns_ids() {
        let tg = FakeTg::start().script("sendMessage", vec![r#"{"ok":true,"result":{"message_id":41}}"#.into()]);
        let bot = tg.bot();
        let ids = bot.send_text_ids(7, "привет").await.expect("ушло");
        assert_eq!(ids, vec![41]);
        let seen = tg.requests();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].contains("/sendMessage"));
        assert!(seen[0].contains("\"chat_id\":7"));
        assert!(seen[0].contains("привет"));
    }

    #[tokio::test]
    async fn flood_wait_stops_the_bot_from_hammering_telegram() {
        // живой случай: лимит на чат, Telegram просит ждать 13907 с. Раньше бот
        // долбил API каждые 700 мс — тысячи повторов за вечер и новый бан каждый раз.
        let tg = FakeTg::start().script(
            "sendMessage",
            vec![r#"{"ok":false,"description":"Too Many Requests: retry after 3","parameters":{"retry_after":3}}"#.into()],
        );
        let bot = tg.bot();
        assert!(!bot.send_paused());
        let err = bot.send_markdown_ids(7, "ответ").await.unwrap_err();
        assert!(format!("{err}").contains("Too Many Requests"));
        assert!(bot.send_paused(), "пауза заведена");
        assert!(bot.paused_until() > crate::outbox::now_ms());
        // любая попытка внутри паузы не идёт в сеть: Telegram не получает
        // запрос, который лишь продлил бы запрет
        let again = bot.send_markdown_ids(7, "ответ").await.unwrap_err();
        assert!(format!("{again}").contains("пауза"));
        assert_eq!(tg.requests().len(), 1, "вторая попытка не дошла до сервера");
        // и пузырь статуса, и «печатает», и правки — тоже ждут
        bot.send_typing(7).await;
        assert!(!bot.edit_text(7, 1, "строка").await);
        assert!(!bot.delete(7, 1).await);
        assert_eq!(tg.requests().len(), 1);
    }

    #[tokio::test]
    async fn broken_html_falls_back_to_plain_text() {
        let tg = FakeTg::start().script(
            "sendMessage",
            vec![
                r#"{"ok":false,"description":"Bad Request: can't parse entities"}"#.into(),
                r#"{"ok":true,"result":{"message_id":9}}"#.into(),
            ],
        );
        let bot = tg.bot();
        let ids = bot.send_markdown_ids(7, "**жирный**").await.expect("доставлено");
        assert_eq!(ids, vec![9]);
        let seen = tg.requests();
        assert_eq!(seen.len(), 2, "сначала HTML, затем обычный текст");
        assert!(seen[0].contains("parse_mode"));
        assert!(!seen[1].contains("parse_mode"), "повтор уходит без разметки");
    }

    #[tokio::test]
    async fn network_failure_is_retried_then_given_up() {
        // порт, который никто не слушает: соединение не поднимается
        let free_addr = crate::testkit::dead_endpoint();
        let bot = super::Bot::new("TOK", None, Some(&format!("http://{free_addr}"))).expect("бот");
        let err = format!("{}", bot.send_text_ids(7, "нет связи").await.unwrap_err());
        let transport_fail = err.contains("error sending request")
            || err.contains("Connection refused")
            || err.contains("connection closed");
        assert!(transport_fail, "{err}");
        assert!(!bot.send_paused(), "сетевой сбой — не наказание Telegram");
    }

    #[tokio::test]
    async fn attachment_is_downloaded_to_disk() {
        let tg = FakeTg::start().script(
            "getFile",
            vec![r#"{"ok":true,"result":{"file_path":"documents/note.txt"}}"#.into()],
        );
        let bot = tg.bot();
        let file = super::FileRef {
            file_id: "ABC".into(),
            file_name: Some("note.txt".into()),
            mime_type: None,
            file_size: Some(4),
            width: None,
            height: None,
        };
        let dest = std::env::temp_dir().join(format!("claudetg-dl-{}.txt", std::process::id()));

        let size = bot.download(&file, &dest).await.expect("скачали");
        assert_eq!(size, 4);
        assert_eq!(std::fs::read_to_string(&dest).expect("файл на диске"), "data");
        let _ = std::fs::remove_file(&dest);
        assert!(tg.requests().iter().any(|r| r.contains("/getFile")));
    }

    #[tokio::test]
    async fn stale_file_id_says_so_instead_of_retrying() {
                let tg = FakeTg::start().script("getFile", vec![r#"{"ok":true,"result":{}}"#.into()]);
        let bot = tg.bot();
        let file = super::FileRef {
            file_id: "OLD".into(),
            file_name: None,
            mime_type: None,
            file_size: None,
            width: None,
            height: None,
        };
        let err = bot.download(&file, &std::env::temp_dir().join("nope")).await.unwrap_err();
        assert!(format!("{err}").contains("пришли его ещё раз"));
        assert_eq!(tg.requests().len(), 1, "отказ по делу не повторяем");
    }

    #[tokio::test]
    async fn huge_attachments_are_refused_before_download() {
        let tg = FakeTg::start();
        let bot = tg.bot();
        let file = super::FileRef {
            file_id: "BIG".into(),
            file_name: Some("movie.mp4".into()),
            mime_type: None,
            file_size: Some(21 * 1024 * 1024),
            width: None,
            height: None,
        };
        let err = bot.download(&file, &std::env::temp_dir().join("nope")).await.unwrap_err();
        assert!(format!("{err}").contains("20 МБ"));
        assert!(tg.requests().is_empty(), "запрос даже не уходил");
    }

    #[tokio::test]
    async fn document_uploads_with_caption() {
        let tg = FakeTg::start();
        let bot = tg.bot();
        let path = std::env::temp_dir().join(format!("claudetg-doc-{}.apk", std::process::id()));
        std::fs::write(&path, "apk-bytes").expect("временный файл");
        bot.send_document(7, &path, Some("свежий выпуск")).await.expect("отправлен");
        let _ = std::fs::remove_file(&path);
        let seen = tg.requests();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].contains("/sendDocument"));
        assert!(seen[0].contains("apk-bytes"));
        assert!(seen[0].contains("свежий выпуск"));
    }

    #[tokio::test]
    async fn oversized_document_is_refused_locally() {
        let tg = FakeTg::start();
        let bot = tg.bot();
        let path = std::env::temp_dir().join(format!("claudetg-big-{}.bin", std::process::id()));
        // 51 МБ «дырявым» файлом: на диске почти ноль, в памяти — ровно проверка лимита
        let f = std::fs::File::create(&path).expect("файл");
        f.set_len(51 * 1024 * 1024).expect("размер");
        let err = bot.send_document(7, &path, None).await.unwrap_err();
        let _ = std::fs::remove_file(&path);
        assert!(format!("{err}").contains("50 МБ"));
        assert!(tg.requests().is_empty());
    }

    #[tokio::test]
    async fn status_bubble_edits_are_tolerant() {
        let tg = FakeTg::start()
            .script("editMessageText", vec![
                r#"{"ok":true,"result":{"message_id":5}}"#.into(),
                r#"{"ok":false,"description":"Bad Request: message is not modified"}"#.into(),
            ])
            .script("deleteMessage", vec![
                r#"{"ok":false,"description":"Bad Request: message to delete not found"}"#.into(),
            ]);
        let bot = tg.bot();
        assert!(bot.edit_text(7, 5, "строка").await, "правка прошла");
        assert!(bot.edit_text(7, 5, "строка").await, "«не изменено» — тоже успех");
        assert!(!bot.delete(7, 5).await, "чужое сообщение не удалить");
    }

    #[tokio::test]
    async fn markdown_answers_use_html_and_the_short_wrapper() {
        let tg = crate::testkit::FakeTg::start();
        let bot = tg.bot();
        // wrapper без возврата id: id не нужны, но сообщение уходит с разметкой
        bot.send_markdown(11, "**важно**").await;
        let sent = tg.calls("sendMessage");
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert!(sent[0].contains("parse_mode"), "{sent:?}");
        assert!(sent[0].contains("<b>важно</b>"), "{sent:?}");
    }

    #[tokio::test]
    async fn only_the_first_part_of_a_long_answer_can_fail() {
        // ответ из двух частей: вторая не ушла — первая всё равно на месте,
        // ошибки нет, потому что чат получил начало
        let tg = crate::testkit::FakeTg::start().script(
            "sendMessage",
            vec![
                r#"{"ok":true,"result":{"message_id":1}}"#.into(),
                r#"{"ok":false,"description":"message is too long"}"#.into(),
            ],
        );
        let bot = tg.bot();
        let long = "строка ответа\n".repeat(300);
        let ids = bot.send_text_ids(11, &long).await.expect("первая часть ушла");
        assert_eq!(ids.len(), 1, "вторая часть осталась неотправленной");
    }

    #[tokio::test]
    async fn edits_and_deletes_answer_quietly_when_telegram_refuses() {
        let tg = crate::testkit::FakeTg::start().script(
            "editMessageText",
            vec![
                // сообщение уже удалено — правка не вышла, пузырь создадут заново
                r#"{"ok":false,"description":"message to edit not found"}"#.into(),
                // ничего не изменилось — это считаем успехом
                r#"{"ok":false,"description":"error: message is not modified"}"#.into(),
                // обычная успешная правка
                r#"{"ok":true,"result":true}"#.into(),
            ],
        );
        let tg = tg.script(
            "deleteMessage",
            vec![
                // Telegram принял запрос, но удалять нечего
                r#"{"ok":true,"result":false}"#.into(),
                // чужое сообщение бот не удалит — отказ
                r#"{"ok":false,"description":"message can't be deleted"}"#.into(),
            ],
        );
        let bot = tg.bot();
        assert!(!bot.edit_text(11, 1, "текст").await, "правка не вышла");
        assert!(bot.edit_text(11, 1, "текст").await, "«не изменено» — это успех");
        assert!(bot.edit_text(11, 1, "текст").await, "обычная правка");
        assert!(!bot.delete(11, 1).await, "удалять нечего");
        assert!(!bot.delete(11, 2).await, "чужое сообщение недоступно");
    }

    #[tokio::test]
    async fn file_download_reports_http_errors_of_the_file_server() {
        let tg = crate::testkit::FakeTg::start().file_status(404);
        let bot = tg.bot();
        let file = FileRef {
            file_id: "F1".into(),
            file_name: Some("отчёт.md".into()),
            mime_type: None,
            file_size: Some(4),
            width: None,
            height: None,
        };
        let dest = std::env::temp_dir().join("claudetg-dl-404.md");
        let err = format!("{:#}", bot.download(&file, &dest).await.unwrap_err());
        assert!(err.contains("HTTP 404"), "{err}");
        let _ = std::fs::remove_file(&dest);
    }

    #[tokio::test]
    async fn file_download_retries_a_route_that_does_not_reach() {
        // адрес, где никто не слушает: сетевой сбой повторяем до трёх раз
        let dead_addr = crate::testkit::dead_endpoint();
        let bot = Bot::new("TOK", None, Some(&format!("http://{dead_addr}"))).unwrap();
        let file = FileRef {
            file_id: "F1".into(),
            file_name: None,
            mime_type: None,
            file_size: None,
            width: None,
            height: None,
        };
        let dest = std::env::temp_dir().join("claudetg-dl-dead.bin");
        let err = format!("{:#}", bot.download(&file, &dest).await.unwrap_err());
        let transport_fail = err.contains("Connection refused") || err.contains("connection closed");
        assert!(err.contains("getFile") && transport_fail, "{err}");
        let _ = std::fs::remove_file(&dest);
    }

    #[tokio::test]
    async fn document_upload_reports_a_refusal_from_telegram() {
        let tg = crate::testkit::FakeTg::start().script(
            "sendDocument",
            vec![r#"{"ok":false,"description":"file too big"}"#.into()],
        );
        let path = std::env::temp_dir().join("claudetg-upload-refused.txt");
        std::fs::write(&path, "тело").unwrap();
        let err = format!("{:#}", tg.bot().send_document(11, &path, Some("подпись")).await.unwrap_err());
        assert!(err.contains("sendDocument failed: file too big"), "{err}");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn document_upload_gives_up_when_no_route_works() {
        let dead_addr = crate::testkit::dead_endpoint();
        let bot = Bot::new("TOK", None, Some(&format!("http://{dead_addr}"))).unwrap();
        let path = std::env::temp_dir().join("claudetg-upload-dead.txt");
        std::fs::write(&path, "тело").unwrap();
        let err = format!("{:#}", bot.send_document(11, &path, None).await.unwrap_err());
        assert!(err.contains("sendDocument"), "{err}");
        let _ = std::fs::remove_file(&path);
    }

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
        let bot = super::Bot::new("123456:SEKRET-t0ken", None, None).expect("бот");
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
        let with_proxy = super::Bot::new("t", Some("http://localhost:3128"), None).expect("бот");
        assert!(with_proxy.fallback.is_some());
        assert_eq!(with_proxy.routes().len(), 2);

        let direct = super::Bot::new("t", None, None).expect("бот");
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

    #[tokio::test]
    async fn long_polling_requests_pass_offset() {
        let update = r#"{"update_id":10,"message":{"message_id":10,"chat":{"id":7},"from":{"id":7},"text":"дело"}}"#;
        let tg = crate::testkit::FakeTg::start().script(
            "getUpdates",
            vec![format!(r#"{{"ok":true,"result":[{update}]}}"#)],
        );
        let bot = tg.bot();
        let updates = bot.get_updates(9).await.expect("поллинг");
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].message.as_ref().unwrap().text.as_deref(), Some("дело"));
        let seen = tg.calls("getUpdates");
        assert_eq!(seen.len(), 1);
        assert!(seen[0].contains("\"offset\":9"), "{}", seen[0]);
        assert!(seen[0].contains("timeout"), "долгий поллинг: {}", seen[0]);
    }

    #[tokio::test]
    async fn token_never_appears_in_errors_or_urls_of_log() {
        // ответ с ошибкой, в котором Telegram не прислал описания: токен всё равно
        // не должен попасть в текст ошибки
        let tg = crate::testkit::FakeTg::start().script(
            "sendMessage",
            vec![r#"{"ok":false}"#.into()],
        );
        let bot = tg.bot();
        let err = format!("{:#}", bot.send_text_ids(7, "текст").await.unwrap_err());
        assert!(!err.contains("TOK"), "{err}");
        assert!(err.contains("unknown error"), "{err}");
    }

    #[tokio::test]
    async fn upload_goes_as_multipart_with_caption() {
        let tg = crate::testkit::FakeTg::start();
        let bot = tg.bot();
        let path = std::env::temp_dir().join(format!("claudetg-up-{}.bin", std::process::id()));
        std::fs::write(&path, b"file-bytes").unwrap();
        bot.send_document(7, &path, Some("подпись")).await.expect("ушло");
        let _ = std::fs::remove_file(&path);
        let seen = tg.calls("sendDocument");
        assert_eq!(seen.len(), 1);
        // multipart: файл и подпись уехали в теле запроса
        assert!(seen[0].contains("file-bytes"), "тело файла: {}", seen[0]);
        assert!(seen[0].contains("подпись"), "подпись: {}", seen[0]);
    }

    #[tokio::test]
    async fn upload_says_so_when_server_answers_garbage() {
        let tg = crate::testkit::FakeTg::start().script(
            "sendDocument",
            vec![r#"не json"#.into()],
        );
        let bot = tg.bot();
        let path = std::env::temp_dir().join(format!("claudetg-upbad-{}.bin", std::process::id()));
        std::fs::write(&path, "bytes").unwrap();
        let err = bot.send_document(7, &path, None).await.unwrap_err();
        let _ = std::fs::remove_file(&path);
        assert!(format!("{err}").contains("ответ не разбирается"), "{err}");
    }

    #[tokio::test]
    async fn download_url_follows_configured_base() {
        let tg = crate::testkit::FakeTg::start().script(
            "getFile",
            vec![r#"{"ok":true,"result":{"file_path":"documents/x.bin"}}"#.into()],
        );
        let bot = tg.bot();
        let file = super::FileRef {
            file_id: "A".into(),
            file_name: None,
            mime_type: None,
            file_size: None,
            width: None,
            height: None,
        };
        let dest = std::env::temp_dir().join(format!("claudetg-base-{}.bin", std::process::id()));
        bot.download(&file, &dest).await.expect("скачали");
        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "data");
        let _ = std::fs::remove_file(&dest);
        let urls = tg.requests();
        assert!(urls.iter().any(|r| r.contains("/file/botTOK/documents/x.bin")), "{urls:?}");
    }

    #[tokio::test]
    async fn direct_bot_has_the_official_file_host() {
        let bot = super::Bot::new("TOK", None, None).unwrap();
        assert_eq!(
            bot.file_url("documents/x.bin"),
            "https://api.telegram.org/file/botTOK/documents/x.bin"
        );
    }

    #[tokio::test]
    async fn a_dropped_connection_is_an_error_without_a_token() {
        // Telegram оборвал соединение, ничего не ответив: наружу — сетевая ошибка,
        // и в ней не должно быть токена (ошибки уходят и в журнал, и в чат)
        let tg = FakeTg::start().script("getMe", vec![crate::testkit::DROP.to_string()]);
        let bot = tg.bot();
        let err = format!("{}", bot.get_me().await.unwrap_err());
        assert!(!err.is_empty(), "ошибка есть: {err}");
        assert!(!err.contains("botTOK"), "токен скрыт: {err}");
    }

    #[tokio::test]
    async fn send_document_reports_a_dropped_connection() {
        let tg = FakeTg::start().script("sendDocument", vec![crate::testkit::DROP.to_string()]);
        let bot = tg.bot();
        let path = std::env::temp_dir().join(format!("claudetg-drop-doc-{}.txt", std::process::id()));
        std::fs::write(&path, "x").unwrap();
        let err = format!("{}", bot.send_document(7, &path, Some("файл")).await.unwrap_err());
        assert!(!err.contains("botTOK"), "токен скрыт: {err}");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn a_file_download_survives_a_proxy_that_does_not_answer() {
        // прокси висит: getFile и скачивание идут напрямую, а не падают
        let tg = FakeTg::start();
        let dead_addr = crate::testkit::dead_endpoint();
        let bot = Bot::new("TOK", Some(&format!("http://{dead_addr}")), Some(tg.base())).unwrap();
        let dest = std::env::temp_dir().join(format!("claudetg-dl-proxy-{}.bin", std::process::id()));
        let file = FileRef {
            file_id: "F1".into(),
            file_name: None,
            mime_type: None,
            file_size: Some(4),
            width: None,
            height: None,
        };
        let got = bot.download(&file, &dest).await.expect("прямой маршрут качает файл");
        assert_eq!(got as usize, crate::testkit::FILE_BODY.len());
        // прокси до Telegram не дошёл: подстава видит только прямой заход
        assert_eq!(tg.calls("getFile").len(), 1, "{:?}", tg.calls("getFile"));
        let _ = std::fs::remove_file(&dest);
    }

    #[tokio::test]
    async fn a_lost_second_part_does_not_cancel_the_first_one() {
        // длинный ответ идёт двумя кусками: первый ушёл, второй оборвался.
        // Первое сообщение остаётся в чате, о потере куска пишет журнал
        let tg = FakeTg::start().script(
            "sendMessage",
            vec![
                r#"{"ok":true,"result":{"message_id":61}}"#.into(),
                crate::testkit::DROP.to_string(),
                crate::testkit::DROP.to_string(),
                crate::testkit::DROP.to_string(),
            ],
        );
        let bot = tg.bot();
        let long = format!("{}\n\n{}", "а".repeat(2500), "б".repeat(2500));
        let ids = bot.send_markdown_ids(7, &long).await.expect("первая часть ушла");
        assert_eq!(ids, vec![61], "второй кусок не ушёл: {ids:?}");
        assert_eq!(tg.calls("sendMessage").len(), 4, "второй кусок пробован трижды");
    }


    #[tokio::test]
    async fn a_downloaded_file_that_cannot_be_written_is_reported() {
        // адрес файла получен, запись на диск невозможна (путь — корень файловой
        // системы): бот сообщает о проблеме и не падает
        let tg = FakeTg::start();
        let bot = tg.bot();
        let file = FileRef {
            file_id: "F1".into(),
            file_name: None,
            mime_type: None,
            file_size: Some(4),
            width: None,
            height: None,
        };
        let err = format!("{}", bot.download(&file, std::path::Path::new("/")).await.unwrap_err());
        assert!(err.contains("не записать файл на диск"), "{err}");
    }


    #[tokio::test]
    async fn a_file_send_honours_the_telegram_pause() {
        // лимит на отдаче файлов: первый ответ заводит паузу, вторая попытка
        // не должна снова гнать 5 МБ в сеть
        let tg = FakeTg::start().script(
            "sendDocument",
            vec![
                r#"{"ok":false,"description":"Too Many Requests: retry after 30","parameters":{"retry_after":30}}"#.into(),
            ],
        );
        let bot = tg.bot();
        let path = std::env::temp_dir().join(format!("claudetg-doc-flood-{}.txt", std::process::id()));
        std::fs::write(&path, "d").unwrap();
        let err = format!("{}", bot.send_document(7, &path, Some("vypusk")).await.unwrap_err());
        assert!(err.contains("Too Many Requests"), "{err}");
        assert!(bot.send_paused(), "пауза заведена");
        let err2 = format!("{}", bot.send_document(7, &path, None).await.unwrap_err());
        assert!(err2.contains("пауза Telegram ещё"), "{err2}");
        assert_eq!(tg.calls("sendDocument").len(), 1, "файл не грузился повторно");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn a_sent_file_reports_its_message_id() {
        // id нужен журналу отправленного: по нему чистка убирает файл из чата
        let tg = FakeTg::start().script(
            "sendDocument",
            vec![r#"{"ok":true,"result":{"message_id":4242}}"#.into()],
        );
        let path = std::env::temp_dir().join(format!("claudetg-doc-id-{}.txt", std::process::id()));
        std::fs::write(&path, "d").unwrap();
        let id = tg.bot().send_document(7, &path, None).await.expect("ушло");
        assert_eq!(id, 4242);
        let _ = std::fs::remove_file(&path);
    }

}
