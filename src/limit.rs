//! Сообщение Claude о закончившемся лимите → момент, когда можно продолжить.
//!
//! Claude Code сообщает о лимите строкой вида
//! `You've hit your 5-hour limit · resets 10:40am (Europe/Moscow)`, а для
//! недельного — с датой: `… resets Nov 3, 9am (Europe/Moscow)`. Бот вычитывает
//! время сброса и через минуту после него сам пишет в сессию «Продолжай».

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;

/// Запас после сброса: ровно в момент сброса лимит может ещё не обновиться.
const GRACE: Duration = Duration::minutes(1);

/// Если в тексте есть сообщение о лимите — момент (UTC), когда продолжить,
/// и подпись этого момента в часовом поясе из сообщения («10:41, Europe/Moscow»).
pub fn resume_at(text: &str, now: DateTime<Utc>) -> Option<(DateTime<Utc>, String)> {
    let lower = text.to_lowercase();
    if !lower.contains("limit") {
        return None;
    }
    let after = &text[lower.find("resets")? + "resets".len()..];
    let open = after.find('(')?;
    let close = open + after[open..].find(')')?;
    let tz: Tz = after[open + 1..close].trim().parse().ok()?;
    let when = after[..open].trim();

    // Необязательная дата: «Nov 3, 9am».
    let (date_part, time_part) = match when.rsplit_once(',') {
        Some((d, t)) => (Some(d.trim()), t.trim()),
        None => (None, when),
    };
    let time = parse_time(time_part)?;
    let local_now = now.with_timezone(&tz);

    let target_date = match date_part {
        Some(d) => {
            let (month, day) = parse_month_day(d)?;
            let this_year = NaiveDate::from_ymd_opt(local_now.year(), month, day)?;
            if this_year < local_now.date_naive() {
                NaiveDate::from_ymd_opt(local_now.year() + 1, month, day)?
            } else {
                this_year
            }
        }
        None => {
            // Без даты — ближайшее наступление этого времени: сегодня или завтра.
            let today = local_now.date_naive();
            if today.and_time(time) > local_now.naive_local() {
                today
            } else {
                today.succ_opt()?
            }
        }
    };

    let local = tz.from_local_datetime(&target_date.and_time(time)).earliest()?;
    let resume = local.with_timezone(&Utc) + GRACE;
    let label = format!(
        "{} {}",
        (local + GRACE).format("%d.%m %H:%M"),
        tz.name()
    );
    Some((resume, label))
}

/// «10:40am», «9am», «12:05pm».
fn parse_time(s: &str) -> Option<NaiveTime> {
    let s = s.trim().to_lowercase();
    let (digits, pm) = if let Some(d) = s.strip_suffix("am") {
        (d, false)
    } else if let Some(d) = s.strip_suffix("pm") {
        (d, true)
    } else {
        return None;
    };
    let (h, m) = match digits.trim().split_once(':') {
        Some((h, m)) => (h.parse::<u32>().ok()?, m.parse::<u32>().ok()?),
        None => (digits.trim().parse::<u32>().ok()?, 0),
    };
    if !(1..=12).contains(&h) {
        return None;
    }
    let hour = match (h, pm) {
        (12, false) => 0,
        (12, true) => 12,
        (h, false) => h,
        (h, true) => h + 12,
    };
    NaiveTime::from_hms_opt(hour, m, 0)
}

/// «Nov 3» → (11, 3).
fn parse_month_day(s: &str) -> Option<(u32, u32)> {
    let (mon, day) = s.split_once(char::is_whitespace)?;
    let months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
    let key = mon.trim().to_lowercase();
    let month = months.iter().position(|m| key.starts_with(m))? as u32 + 1;
    Some((month, day.trim().parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn five_hour_limit_resumes_a_minute_after_reset() {
        // 07:00 UTC = 10:00 в Москве; сброс в 10:40 → продолжить в 10:41 МСК = 07:41 UTC.
        let (at, label) = resume_at(
            "⚠️ You've hit your 5-hour limit · resets 10:40am (Europe/Moscow)",
            utc("2026-11-01T07:00:00Z"),
        )
        .unwrap();
        assert_eq!(at, utc("2026-11-01T07:41:00Z"));
        assert_eq!(label, "01.11 10:41 Europe/Moscow");
    }

    #[test]
    fn reset_time_already_passed_today_means_tomorrow() {
        let (at, _) = resume_at(
            "You've hit your 5-hour limit · resets 9am (Europe/Moscow)",
            utc("2026-11-01T08:00:00Z"), // 11:00 МСК
        )
        .unwrap();
        assert_eq!(at, utc("2026-11-02T06:01:00Z"));
    }

    #[test]
    fn weekly_limit_with_a_date_and_pm() {
        let (at, _) = resume_at(
            "You've hit your weekly limit · resets Nov 3, 12:30pm (Europe/Moscow)",
            utc("2026-11-01T07:00:00Z"),
        )
        .unwrap();
        assert_eq!(at, utc("2026-11-03T09:31:00Z"));
    }

    #[test]
    fn dated_reset_of_the_past_year_rolls_over() {
        // «Nov 3» уже прошло в этом году — ждём сброса в следующем
        let now = utc("2026-12-20T08:00:00Z");
        let (at, label) = resume_at(
            "You've hit your weekly limit · resets Nov 3, 9am (Europe/Moscow)",
            now,
        )
        .expect("недельный лимит");
        assert!(at > utc("2027-11-03T00:00:00Z") && at < utc("2027-11-04T00:00:00Z"), "{at}");
        assert!(label.contains("2027") || label.contains("03.11"), "{label}");
    }

    #[test]
    fn times_are_read_in_twelve_hour_clock() {
        assert_eq!(parse_time("12:05pm"), NaiveTime::from_hms_opt(12, 5, 0));
        assert_eq!(parse_time("1pm"), NaiveTime::from_hms_opt(13, 0, 0));
        assert_eq!(parse_time("12am"), NaiveTime::from_hms_opt(0, 0, 0));
        assert_eq!(parse_time("11:40am"), NaiveTime::from_hms_opt(11, 40, 0));
        // время без am/pm не разбираем — иначе нельзя понять, когда продолжать
        assert_eq!(parse_time("10:40"), None);
        assert_eq!(parse_time("0am"), None, "часа 0 в двенадцатичасовом формате нет");
    }

    #[test]
    fn a_reset_without_am_or_pm_is_not_a_schedule() {
        let now = utc("2026-11-05T08:00:00Z");
        assert!(resume_at("limit reached · resets 10:40 (Europe/Moscow)", now).is_none());
    }

    #[test]
    fn ordinary_text_and_garbage_are_ignored() {
        let now = utc("2026-11-01T07:00:00Z");
        assert!(resume_at("Готово: всё выложено", now).is_none());
        assert!(resume_at("limit · resets soon (Nowhere/City)", now).is_none());
        assert!(resume_at("limit · resets 25:00pm (Europe/Moscow)", now).is_none());
    }
}
