//! Markdown ответа Claude → HTML-подмножество Telegram (`parse_mode=HTML`).
//!
//! Telegram понимает только `b i u s code pre a blockquote` и не знает ни
//! заголовков, ни списков, ни таблиц. Поэтому: заголовки → жирный, списки →
//! «•», таблицы → строки «ячейка · ячейка», правило → черта. Всё, что не
//! разметка, экранируется: `<`, `>`, `&` в тексте иначе ломали бы запрос.

/// Длинный ответ режется на сообщения **до** конвертации, по строкам; блок
/// кода, попавший на границу, закрывается и открывается заново.
/// Пары (HTML, исходный Markdown): исходник нужен, чтобы отправить кусок
/// обычным текстом, если Telegram отверг HTML.
pub fn to_html_chunks(md: &str, max_chars: usize) -> Vec<(String, String)> {
    split_markdown(md, max_chars).into_iter().map(|chunk| (to_html(&chunk), chunk)).collect()
}

fn split_markdown(md: &str, max: usize) -> Vec<String> {
    let max = max.max(1);
    let mut chunks: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut len = 0usize;
    let mut fence: Option<String> = None; // открывающая строка текущего блока кода
    for line in md.split_inclusive('\n') {
        let line_len = line.chars().count();
        // граница между кусками: закрываем блок кода и открываем его заново
        // в следующем куске, чтобы Telegram не увидел незакрытый ```
        if len + line_len > max && !current.is_empty() {
            if let Some(open) = fence.clone() {
                current.push_str("```\n");
                chunks.push(std::mem::take(&mut current));
                current.push_str(&open);
                len = open.chars().count();
                if line_len > max {
                    // длинная строка внутри блока: её тоже режем ровно по max
                    for c in line.chars() {
                        if len == max {
                            current.push_str("```\n");
                            chunks.push(std::mem::take(&mut current));
                            current.push_str(&open);
                            len = open.chars().count();
                        }
                        current.push(c);
                        len += 1;
                    }
                } else {
                    current.push_str(line);
                    len += line_len;
                }
                if line.trim_start().starts_with("```") {
                    // сюда попадаем только с уже открытым блоком (см. `if let
                    // Some(open) = fence.clone()` выше), значит строка ``` его закрывает
                    fence = None;
                }
                continue;
            }
            chunks.push(std::mem::take(&mut current));
            len = 0;
        }
        // Одна строка длиннее предела — режется по символам.
        if line_len > max {
            for c in line.chars() {
                if len == max {
                    chunks.push(std::mem::take(&mut current));
                    len = 0;
                }
                current.push(c);
                len += 1;
            }
        } else {
            current.push_str(line);
            len += line_len;
        }
        if line.trim_start().starts_with("```") {
            fence = match fence {
                Some(_) => None,
                None => Some(format!("{}\n", line.trim_end())),
            };
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

pub fn to_html(md: &str) -> String {
    let mut out = String::new();
    let mut lines = md.lines().peekable();
    while let Some(line) = lines.next() {
        let trimmed = line.trim_start();

        // Блок кода.
        if let Some(lang) = trimmed.strip_prefix("```") {
            let mut code = String::new();
            for inner in lines.by_ref() {
                if inner.trim_start().starts_with("```") {
                    break;
                }
                code.push_str(inner);
                code.push('\n');
            }
            let lang = lang.trim();
            if lang.is_empty() || !lang.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '+') {
                out.push_str(&format!("<pre>{}</pre>\n", escape(code.trim_end_matches('\n'))));
            } else {
                out.push_str(&format!(
                    "<pre><code class=\"language-{lang}\">{}</code></pre>\n",
                    escape(code.trim_end_matches('\n'))
                ));
            }
            continue;
        }

        // Таблица: строки, начинающиеся с `|`.
        if trimmed.starts_with('|') {
            let mut rows = vec![trimmed.to_string()];
            while let Some(next) = lines.peek() {
                if next.trim_start().starts_with('|') {
                    rows.push(lines.next().unwrap_or_default().trim_start().to_string());
                } else {
                    break;
                }
            }
            out.push_str(&table(&rows));
            continue;
        }

        // Заголовок.
        let hashes = trimmed.chars().take_while(|c| *c == '#').count();
        if (1..=6).contains(&hashes) && trimmed[hashes..].starts_with(' ') {
            out.push_str(&format!("<b>{}</b>\n", inline(trimmed[hashes..].trim())));
            continue;
        }

        // Горизонтальная черта.
        let compact: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();
        if compact.len() >= 3
            && (compact.chars().all(|c| c == '-') || compact.chars().all(|c| c == '*'))
        {
            out.push_str("———\n");
            continue;
        }

        // Цитата.
        if let Some(quote) = trimmed.strip_prefix('>') {
            let mut text = vec![inline(quote.trim_start())];
            while let Some(next) = lines.peek() {
                match next.trim_start().strip_prefix('>') {
                    Some(q) => {
                        text.push(inline(q.trim_start()));
                        lines.next();
                    }
                    None => break,
                }
            }
            out.push_str(&format!("<blockquote>{}</blockquote>\n", text.join("\n")));
            continue;
        }

        // Маркированный список (с сохранением отступа вложенности).
        let indent = line.len() - trimmed.len();
        if let Some(item) = trimmed.strip_prefix("- ").or_else(|| trimmed.strip_prefix("* ")) {
            let pad = "  ".repeat(indent / 2);
            out.push_str(&format!("{pad}• {}\n", inline(item)));
            continue;
        }

        out.push_str(&inline(line));
        out.push('\n');
    }
    out.trim_end().to_string()
}

/// Таблица → строки: заголовок жирным, разделитель `|---|` пропускается.
fn table(rows: &[String]) -> String {
    let cells = |row: &str| -> Vec<String> {
        row.trim().trim_matches('|').split('|').map(|c| c.trim().to_string()).collect()
    };
    let is_separator = |row: &str| {
        let r: String = row.chars().filter(|c| !matches!(c, '|' | ' ' | ':' | '-')).collect();
        r.is_empty() && row.contains('-')
    };
    let mut out = String::new();
    let mut header = true;
    for row in rows {
        if is_separator(row) {
            continue;
        }
        let line = cells(row).iter().map(|c| inline(c)).collect::<Vec<_>>().join(" · ");
        if header {
            out.push_str(&format!("<b>{line}</b>\n"));
            header = false;
        } else {
            out.push_str(&format!("{line}\n"));
        }
    }
    out
}

/// Строчная разметка: `код`, **жирный**, *курсив* / _курсив_, ~~зачёркнутый~~, [ссылка](url).
fn inline(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        if let Some((html, next)) = markup_at(&chars, i) {
            out.push_str(&html);
            i = next;
        } else {
            out.push_str(&escape(&chars[i].to_string()));
            i += 1;
        }
    }
    out
}

/// Разметка, начинающаяся в позиции `i`: готовый HTML и позиция после неё.
fn markup_at(chars: &[char], i: usize) -> Option<(String, usize)> {
    let c = chars[i];
    let slice = |a: usize, b: usize| -> String { chars[a..b].iter().collect() };

    // `код` — внутри никакой разметки.
    if c == '`' {
        let end = find(chars, i + 1, "`")?;
        return Some((format!("<code>{}</code>", escape(&slice(i + 1, end))), end + 1));
    }
    // [текст](https://…)
    if c == '[' {
        let close = find(chars, i + 1, "](")?;
        let end = find(chars, close + 2, ")")?;
        let url = slice(close + 2, end);
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return None;
        }
        let href = escape(&url).replace('"', "&quot;");
        return Some((format!("<a href=\"{href}\">{}</a>", inline(&slice(i + 1, close))), end + 1));
    }
    // Парные двойные маркеры.
    for (mark, tag) in [("**", "b"), ("__", "b"), ("~~", "s")] {
        let m: Vec<char> = mark.chars().collect();
        if chars[i..].starts_with(&m) {
            let end = find(chars, i + 2, mark)?;
            if end == i + 2 {
                return None;
            }
            return Some((format!("<{tag}>{}</{tag}>", inline(&slice(i + 2, end))), end + 2));
        }
    }
    // *курсив* / _курсив_: после открывающего — не пробел, перед закрывающим —
    // не пробел; `_` внутри слова (snake_case) курсивом не считается.
    if (c == '*' || c == '_') && i + 1 < chars.len() && !chars[i + 1].is_whitespace() {
        if c == '_' && i > 0 && chars[i - 1].is_alphanumeric() {
            return None;
        }
        let end = find(chars, i + 1, &c.to_string())?;
        let after_ok = end + 1 >= chars.len() || !chars[end + 1].is_alphanumeric();
        if chars[end - 1].is_whitespace() || (c == '_' && !after_ok) {
            return None;
        }
        return Some((format!("<i>{}</i>", inline(&slice(i + 1, end))), end + 1));
    }
    None
}

fn find(chars: &[char], from: usize, needle: &str) -> Option<usize> {
    let needle: Vec<char> = needle.chars().collect();
    if needle.is_empty() || from >= chars.len() {
        return None;
    }
    (from..=chars.len().saturating_sub(needle.len())).find(|&k| chars[k..k + needle.len()] == needle[..])
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_markup() {
        assert_eq!(
            to_html("**Жирный**, *курсив*, `a<b`, ~~нет~~ и [сайт](https://finbri.ru)"),
            "<b>Жирный</b>, <i>курсив</i>, <code>a&lt;b</code>, <s>нет</s> и \
             <a href=\"https://finbri.ru\">сайт</a>"
        );
    }

    #[test]
    fn plain_text_is_escaped_and_snake_case_survives() {
        assert_eq!(to_html("x < y & a_b_c"), "x &lt; y &amp; a_b_c");
        assert_eq!(to_html("2 * 3 * 4"), "2 * 3 * 4");
    }

    #[test]
    fn headings_lists_quotes_rules() {
        let md = "# Заголовок\n- один\n  - вложенный\n> цитата\n---\nтекст";
        assert_eq!(
            to_html(md),
            "<b>Заголовок</b>\n• один\n  • вложенный\n<blockquote>цитата</blockquote>\n———\nтекст"
        );
    }

    #[test]
    fn code_block_is_escaped_and_untouched() {
        let md = "```rust\nlet a = **b** < c;\n```";
        assert_eq!(
            to_html(md),
            "<pre><code class=\"language-rust\">let a = **b** &lt; c;</code></pre>"
        );
    }

    #[test]
    fn table_becomes_lines() {
        let md = "| Код | Цвет |\n|---|---|\n| `--coral` | #E18A7A |";
        assert_eq!(
            to_html(md),
            "<b>Код · Цвет</b>\n<code>--coral</code> · #E18A7A"
        );
    }

    #[test]
    fn long_text_splits_without_breaking_code_blocks() {
        let md = format!("начало\n```\n{}```\nконец", "строка кода\n".repeat(20));
        let chunks = split_markdown(&md, 60);
        assert!(chunks.len() > 2);
        for chunk in &chunks {
            assert_eq!(chunk.matches("```").count() % 2, 0, "блок кода не закрыт: {chunk}");
        }
    }

    #[test]
    fn html_specials_are_escaped() {
        let out = to_html("a <b> & c > d");
        assert!(out.contains("&lt;b&gt;") && out.contains("&amp;"), "{out}");
    }

    #[test]
    fn lists_headers_and_tables_become_plain_lines() {
        let out = to_html("# Заголовок\n- пункт один\n* пункт два\n\n| а | б |\n|---|---|\n| 1 | 2 |");
        assert!(out.contains("<b>Заголовок</b>"), "{out}");
        assert!(out.contains("•") && out.contains("пункт один"), "{out}");
        // таблица: не ломается, строки остаются читаемыми
        assert!(out.contains("а") && out.contains("2"), "{out}");
    }

    #[test]
    fn inline_marks_map_to_html() {
        let out = to_html("**жирный** и *курсив* и `код`");
        assert!(out.contains("<b>жирный</b>"), "{out}");
        assert!(out.contains("<i>курсив</i>"), "{out}");
        assert!(out.contains("<code>код</code>"), "{out}");
    }

    #[test]
    fn links_survive_as_anchors() {
        let out = to_html("см. [документацию](https://example.com/docs)");
        assert!(out.contains("<a href=\"https://example.com/docs\">документацию</a>"), "{out}");
    }

    #[test]
    fn one_long_line_is_cut_to_size_and_keeps_the_fence() {
        // строка длиннее предела режется по символам, а не вылезает за лимит
        let md = format!("```rust\nlet s = \"{}\";\n```", "x".repeat(40));
        let chunks = to_html_chunks(&md, 14);
        assert!(chunks.len() > 2, "{chunks:?}");
        for (html, src) in &chunks {
            assert!(src.chars().count() <= 14 + 8, "кусок длиннее предела: {src:?}");
            let _ = html;
        }
        let joined: String = chunks.iter().map(|(_, s)| s.clone()).collect::<String>();
        assert!(joined.contains("xxxxx"), "содержимое сохранено");
    }

    #[test]
    fn quotes_and_nested_marks_survive() {
        let out = to_html("> важная цитата с **акцентом**\n> продолжение");
        assert!(out.contains("важная цитата"), "{out}");
        assert!(out.contains("акцентом"), "{out}");
        // одиночная звёздочка внутри слова не превращается в курсив
        let plain = to_html("a_b_c без разметки");
        assert!(plain.contains("a_b_c"), "{plain}");
    }

    #[test]
    fn a_long_line_without_a_fence_is_cut_by_characters() {
        // строка длиннее предела режется по символам и без блока кода
        let md = format!("первая строка\n{}\nзаключительная", "д".repeat(100));
        let chunks = split_markdown(&md, 20);
        assert!(chunks.len() >= 6, "{chunks:?}");
        for chunk in &chunks {
            assert!(chunk.chars().count() <= 21, "кусок длиннее предела: {chunk:?}");
        }
        let joined: String = chunks.concat();
        assert_eq!(joined.matches('д').count(), 100, "ни один символ не потерян");
        assert!(joined.starts_with("первая строка"), "{joined:?}");
    }

    #[test]
    fn a_fence_opened_on_the_boundary_is_reopened_in_the_next_chunk() {
        let md = "первая строка\nвторая строка\n```rust\nlet a = 1;\n```\nпосле блока\n";
        // предел такой, что граница приходится ровно на открывающую строку блока
        let chunks = split_markdown(md, 30);
        assert!(chunks.len() >= 3, "{chunks:?}");
        for (i, chunk) in chunks.iter().enumerate() {
            let fences = chunk.lines().filter(|l| l.trim_start().starts_with("```")).count();
            assert!(fences % 2 == 0, "кусок {i} с незакрытым блоком кода: {chunk:?}");
        }
        let joined: String = chunks.concat();
        assert!(joined.contains("let a = 1;"), "{joined:?}");
    }

    #[test]
    fn code_without_language_and_table_followed_by_text() {
        let md = "```\nключ <tag> & значение\n```\n| колонка | ещё |\n| --- | --- |\nобычный текст после таблицы\n";
        let html = to_html(md);
        assert!(html.contains("<pre>ключ &lt;tag&gt; &amp; значение</pre>"), "{html}");
        assert!(!html.contains("language-"), "языка не было — класса не добавляем: {html}");
        assert!(html.contains("колонка"), "{html}");
        assert!(html.contains("обычный текст после таблицы"), "{html}");
    }

    #[test]
    fn half_broken_markup_stays_text() {
        // ссылка не http, пустые пары маркеров и курсив с пробелом перед закрытием
        let html = to_html("[текст](ftp://example) и **** и *курсив * и ещё _внутри_слова_");
        assert!(!html.contains("<a href"), "{html}");
        assert!(!html.contains("<b></b>"), "{html}");
        assert!(!html.contains("<i>курсив </i>"), "{html}");
        assert!(html.contains("ftp://example"), "{html}");
    }

    #[test]
    fn long_code_fence_reopens_after_split() {
        let md = "до\n```rust\nlet a = 1;\n```\nпосле";
        let chunks = to_html_chunks(md, 10);
        assert!(chunks.len() > 1, "должно разрезать: {chunks:?}");
        for (html, src) in &chunks {
            assert_eq!(src.matches("```").count() % 2, 0, "кусок с незакрытым блоком: {src}");
            let _ = html;
        }
    }

    #[test]
    fn char_search_stops_at_the_edges() {
        use super::find;
        let chars: Vec<char> = "слово".chars().collect();
        assert_eq!(find(&chars, 1, "ло"), Some(1));
        // пустая подстрока и поиск за концом — ничего не ищем
        assert!(find(&chars, 0, "").is_none());
        assert!(find(&chars, 9, "о").is_none());
        assert!(find(&[], 0, "о").is_none());
    }

}
