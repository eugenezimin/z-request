//! Env vars as a configuration layer: `<PREFIX>__POOL__HOLD=15s` → `pool.hold = "15s"`.

use serde_json::{Map, Value};

use crate::utils::error::Problem;

/// Overlays every `<prefix>__…` variable from `vars` onto `tree`; env wins over what is there.
///
/// Path segments are lower-cased. Values become numbers or booleans when they look like one
/// (`16`, `2.0`, `true`), text otherwise; the typed model accepts either where it makes sense.
pub fn overlay(
    tree: &mut Value,
    prefix: &str,
    vars: impl IntoIterator<Item = (String, String)>,
    problems: &mut Vec<Problem>,
) {
    let marker = format!("{prefix}__");
    let mut matched: Vec<(String, String)> = vars
        .into_iter()
        .filter(|(k, _)| k.starts_with(&marker))
        .collect();
    // Deterministic order, and parents (`…__AUTH`) before children (`…__AUTH__MODE`).
    matched.sort();
    for (key, value) in matched {
        let path: Vec<String> = key[marker.len()..]
            .split("__")
            .map(str::to_ascii_lowercase)
            .collect();
        if path.iter().any(String::is_empty) {
            problems.push(Problem {
                path: key.clone(),
                message: "empty segment in env var name (`____`)".into(),
            });
            continue;
        }
        insert(tree, &path, scalar(&value));
    }
}

/// Sets `path` in `tree`, turning any non-map on the way into a map (env wins).
fn insert(tree: &mut Value, path: &[String], value: Value) {
    let Some((last, parents)) = path.split_last() else {
        return;
    };
    let mut node = tree;
    for segment in parents {
        node = ensure_map(node)
            .entry(segment.clone())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    ensure_map(node).insert(last.clone(), value);
}

fn ensure_map(node: &mut Value) -> &mut Map<String, Value> {
    if !node.is_object() {
        *node = Value::Object(Map::new());
    }
    match node {
        Value::Object(map) => map,
        // Just replaced by an object above.
        _ => unreachable!("node is an object"),
    }
}

fn scalar(text: &str) -> Value {
    let t = text.trim();
    if let Ok(n) = t.parse::<u64>() {
        return Value::from(n);
    }
    if t.contains('.')
        && let Ok(f) = t.parse::<f64>()
        && let Some(n) = serde_json::Number::from_f64(f)
    {
        return Value::Number(n);
    }
    match t {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => Value::String(text.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn vars(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn builds_nested_values_and_wins_over_yaml() {
        let mut tree = json!({"pool": {"hold": "30s"}, "defaults": {"auth": "public"}});
        let mut problems = vec![];
        overlay(
            &mut tree,
            "APP",
            vars(&[
                ("APP__POOL__HOLD", "15s"),
                ("APP__POOL__MAX", "unlimited"),
                ("APP__CONNECTIONS__USERS__MAX", "16"),
                ("APP__DEFAULTS__AUTH__MODE", "env"),
                ("APP__DEFAULTS__AUTH__ENVVAR", "SECRET"),
                ("APP__DEFAULTS__RETRY__BACKOFF__MULTIPLIER", "1.5"),
                ("APP__DEFAULTS__RETRY__BACKOFF__JITTER", "false"),
                ("OTHER__POOL__HOLD", "1s"),
            ]),
            &mut problems,
        );
        assert!(problems.is_empty());
        assert_eq!(
            tree,
            json!({
                "pool": {"hold": "15s", "max": "unlimited"},
                "defaults": {
                    "auth": {"mode": "env", "envvar": "SECRET"},
                    "retry": {"backoff": {"multiplier": 1.5, "jitter": false}}
                },
                "connections": {"users": {"max": 16}}
            })
        );
    }

    #[test]
    fn reports_empty_segments() {
        let mut tree = json!({});
        let mut problems = vec![];
        overlay(
            &mut tree,
            "APP",
            vars(&[("APP__POOL____HOLD", "1s")]),
            &mut problems,
        );
        assert_eq!(problems.len(), 1);
    }
}
