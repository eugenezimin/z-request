//! Human-readable byte sizes: `512B`, `64KiB`, `10MiB`, `1GiB`, or decimal `10KB`, `5MB`, `1GB`.
//!
//! A unit is always required, so `1024` is rejected (bytes or kibibytes would be a guess).

use serde::{Deserialize, Deserializer, Serializer};

const UNITS: [(&str, u64); 7] = [
    ("B", 1),
    ("KB", 1_000),
    ("MB", 1_000_000),
    ("GB", 1_000_000_000),
    ("KiB", 1 << 10),
    ("MiB", 1 << 20),
    ("GiB", 1 << 30),
];

/// Parses a size such as `512B`, `64KiB`, `10MiB` or `5MB`.
pub fn parse(text: &str) -> Result<u64, String> {
    let s = text.trim();
    let digits = s.chars().take_while(char::is_ascii_digit).count();
    if digits == 0 {
        return Err(format!(
            "`{text}` is not a size; write e.g. `512KiB` or `10MiB`"
        ));
    }
    let (number, unit) = s.split_at(digits);
    let value: u64 = number
        .parse()
        .map_err(|_| format!("`{text}`: number too large"))?;
    let unit = unit.trim();
    if unit.is_empty() {
        return Err(format!(
            "`{text}` has no unit; write e.g. `{value}KiB` or `{value}B`"
        ));
    }
    let (_, factor) = UNITS.iter().find(|(u, _)| *u == unit).ok_or_else(|| {
        format!("`{text}`: unknown unit `{unit}` (use B, KB, MB, GB, KiB, MiB or GiB)")
    })?;
    value
        .checked_mul(*factor)
        .ok_or_else(|| format!("`{text}`: too large"))
}

/// Formats a size the way [`parse`] reads it, in the largest binary unit that divides it.
pub fn format(bytes: u64) -> String {
    for (unit, factor) in [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)] {
        if bytes != 0 && bytes.is_multiple_of(factor) {
            return format!("{}{unit}", bytes / factor);
        }
    }
    format!("{bytes}B")
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Raw {
    Text(String),
    Number(serde_json::Number),
}

fn from_raw<E: serde::de::Error>(raw: Raw) -> Result<u64, E> {
    match raw {
        Raw::Text(t) => parse(&t).map_err(E::custom),
        Raw::Number(n) => Err(E::custom(format!(
            "`{n}` has no unit; write e.g. `{n}KiB` or `{n}B`"
        ))),
    }
}

/// `#[serde(with = "size")]` for byte-size fields.
pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    from_raw(Raw::deserialize(d)?)
}

/// Serialises a size as text, e.g. `10MiB`.
pub fn serialize<S: Serializer>(bytes: &u64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format(*bytes))
}

/// `#[serde(with = "size::option")]` for optional byte-size fields.
pub mod option {
    use super::*;

    /// Reads an optional size.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
        Option::<Raw>::deserialize(d)?.map(from_raw).transpose()
    }

    /// Writes an optional size.
    pub fn serialize<S: Serializer>(bytes: &Option<u64>, s: S) -> Result<S::Ok, S::Error> {
        match bytes {
            Some(b) => s.serialize_str(&format(*b)),
            None => s.serialize_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_binary_and_decimal_units() {
        assert_eq!(parse("512B").unwrap(), 512);
        assert_eq!(parse("64KiB").unwrap(), 65_536);
        assert_eq!(parse("10MiB").unwrap(), 10 << 20);
        assert_eq!(parse("1GiB").unwrap(), 1 << 30);
        assert_eq!(parse("5MB").unwrap(), 5_000_000);
        assert_eq!(parse(" 3 KB ").unwrap(), 3_000);
    }

    #[test]
    fn rejects_missing_unknown_or_overflowing_sizes() {
        assert!(parse("1024").unwrap_err().contains("no unit"));
        assert!(parse("5TB").unwrap_err().contains("unknown unit"));
        assert!(parse("mib").is_err());
        assert!(parse("").is_err());
        assert!(parse("99999999999GiB").unwrap_err().contains("too large"));
    }

    #[test]
    fn formats_back() {
        assert_eq!(format(10 << 20), "10MiB");
        assert_eq!(format(1536), "1536B");
        assert_eq!(format(2048), "2KiB");
        assert_eq!(format(0), "0B");
        for s in ["512B", "64KiB", "10MiB", "1GiB"] {
            assert_eq!(format(parse(s).unwrap()), s);
        }
    }
}
