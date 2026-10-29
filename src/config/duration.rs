//! Human-readable durations: `100ms`, `2s`, `5m`, `1h`, `1m30s`.
//!
//! A unit is always required, so `30` is rejected (seconds or milliseconds would be a guess).

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serializer};

/// Parses a duration such as `250ms`, `30s`, `5m`, `1h` or `1m30s`.
pub fn parse(text: &str) -> Result<Duration, String> {
    let s = text.trim();
    if s.is_empty() {
        return Err("empty duration; write e.g. `30s`".into());
    }
    let mut total = Duration::ZERO;
    let mut rest = s;
    while !rest.is_empty() {
        let digits = rest.chars().take_while(char::is_ascii_digit).count();
        if digits == 0 {
            return Err(format!(
                "`{text}` is not a duration; write e.g. `100ms`, `30s`, `5m`"
            ));
        }
        let (number, tail) = rest.split_at(digits);
        let value: u64 = number
            .parse()
            .map_err(|_| format!("`{text}`: number too large"))?;
        let unit_len = tail.chars().take_while(char::is_ascii_alphabetic).count();
        let (unit, tail) = tail.split_at(unit_len);
        let part = match unit {
            "ms" => Duration::from_millis(value),
            "s" => Duration::from_secs(value),
            "m" => Duration::from_secs(value.saturating_mul(60)),
            "h" => Duration::from_secs(value.saturating_mul(3600)),
            "" => {
                return Err(format!(
                    "`{text}` has no unit; write e.g. `{value}s` or `{value}ms`"
                ));
            }
            other => {
                return Err(format!(
                    "`{text}`: unknown unit `{other}` (use ms, s, m or h)"
                ));
            }
        };
        total = total.saturating_add(part);
        rest = tail;
    }
    Ok(total)
}

/// Formats a duration the way [`parse`] reads it, e.g. `1500ms`, `30s`.
pub fn format(d: Duration) -> String {
    if d.subsec_millis() == 0 {
        format!("{}s", d.as_secs())
    } else {
        format!("{}ms", d.as_millis())
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Raw {
    Text(String),
    Number(serde_json::Number),
}

fn from_raw<E: serde::de::Error>(raw: Raw) -> Result<Duration, E> {
    match raw {
        Raw::Text(t) => parse(&t).map_err(E::custom),
        Raw::Number(n) => Err(E::custom(format!(
            "`{n}` has no unit; write e.g. `{n}s` or `{n}ms`"
        ))),
    }
}

/// `#[serde(with = "duration")]` for `Duration` fields.
pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    from_raw(Raw::deserialize(d)?)
}

/// Serialises a `Duration` as text, e.g. `30s`.
pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format(*d))
}

/// `#[serde(with = "duration::option")]` for `Option<Duration>` fields.
pub mod option {
    use super::*;

    /// Reads an optional duration.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        Option::<Raw>::deserialize(d)?.map(from_raw).transpose()
    }

    /// Writes an optional duration.
    pub fn serialize<S: Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match d {
            Some(d) => s.serialize_str(&format(*d)),
            None => s.serialize_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_units_and_combinations() {
        assert_eq!(parse("0s").unwrap(), Duration::ZERO);
        assert_eq!(parse("100ms").unwrap(), Duration::from_millis(100));
        assert_eq!(parse("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse("1m30s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse(" 2s ").unwrap(), Duration::from_secs(2));
    }

    #[test]
    fn rejects_missing_or_unknown_units() {
        assert!(parse("30").unwrap_err().contains("no unit"));
        assert!(parse("5 sec").is_err());
        assert!(parse("5d").unwrap_err().contains("unknown unit"));
        assert!(parse("").is_err());
        assert!(parse("s").is_err());
    }

    #[test]
    fn formats_back() {
        assert_eq!(format(Duration::from_secs(30)), "30s");
        assert_eq!(format(Duration::from_millis(1500)), "1500ms");
    }
}
