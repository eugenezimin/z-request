//! [`ConfigSource`]: YAML (optional) plus env vars, loaded into [`ZRequestConfig`].

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::config::env;
use crate::config::model::{ConnectionConfig, DefaultsConfig, PoolConfig, ZRequestConfig};
use crate::utils::error::{ConfigError, Problem};
use crate::utils::spi::ConfigLoader;

/// The top-level sections a z-request configuration may have.
const SECTIONS: [&str; 3] = ["pool", "defaults", "connections"];

/// Builds a configuration from layers: YAML first, then env vars, which win.
///
/// ```
/// use z_request::config::ConfigSource;
///
/// # #[cfg(feature = "yaml")] {
/// let config = ConfigSource::new()
///     .yaml_str("pool: { hold: 15s }\nconnections: { users: { protocol: grpc, endpoint: 'http://users:9090' } }")
///     .unwrap()
///     .with_env_vars("APP", [("APP__POOL__MAX".to_string(), "16".to_string())])
///     .load()
///     .unwrap();
/// assert_eq!(config.pool.hold, std::time::Duration::from_secs(15));
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ConfigSource {
    tree: Value,
    problems: Vec<Problem>,
}

impl Default for ConfigSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigSource {
    /// An empty source: every setting at its default.
    pub fn new() -> Self {
        Self {
            tree: Value::Object(Map::new()),
            problems: Vec::new(),
        }
    }

    /// Reads a YAML file as the base layer.
    #[cfg(feature = "yaml")]
    pub fn yaml_file(self, path: impl AsRef<std::path::Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::new(path.display().to_string(), e.to_string()))?;
        self.yaml_named(&text, &path.display().to_string())
    }

    /// Reads YAML text as the base layer.
    #[cfg(feature = "yaml")]
    pub fn yaml_str(self, yaml: &str) -> Result<Self, ConfigError> {
        self.yaml_named(yaml, "<yaml>")
    }

    #[cfg(feature = "yaml")]
    fn yaml_named(mut self, yaml: &str, origin: &str) -> Result<Self, ConfigError> {
        let value: Value = if yaml.trim().is_empty() {
            Value::Object(Map::new())
        } else {
            serde_saphyr::from_str(yaml).map_err(|e| ConfigError::new(origin, e.to_string()))?
        };
        match value {
            Value::Object(_) => {
                self.tree = value;
                Ok(self)
            }
            Value::Null => Ok(self),
            _ => Err(ConfigError::new(
                origin,
                "the YAML document must be a mapping",
            )),
        }
    }

    /// Applies env vars named `<prefix>__…` from the process environment.
    pub fn with_env(self, prefix: &str) -> Self {
        self.with_env_vars(prefix, std::env::vars())
    }

    /// Applies `<prefix>__…` variables from the given list (useful in tests).
    pub fn with_env_vars(
        mut self,
        prefix: &str,
        vars: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        env::overlay(&mut self.tree, prefix, vars, &mut self.problems);
        self
    }

    /// Converts the layers into a [`ZRequestConfig`], reporting every structural problem
    /// (unknown keys, wrong types, bad durations) section by section.
    pub fn load(&self) -> Result<ZRequestConfig, ConfigError> {
        let mut problems = self.problems.clone();
        let Value::Object(root) = &self.tree else {
            return Err(ConfigError::new("", "the configuration must be a mapping"));
        };
        for key in root.keys().filter(|k| !SECTIONS.contains(&k.as_str())) {
            problems.push(Problem {
                path: key.clone(),
                message: "unknown section; expected pool, defaults or connections".into(),
            });
        }
        let pool: Option<PoolConfig> = section(root.get("pool"), "pool", &mut problems);
        let defaults: Option<DefaultsConfig> =
            section(root.get("defaults"), "defaults", &mut problems);
        let mut connections = std::collections::BTreeMap::new();
        match root.get("connections") {
            None | Some(Value::Null) => {}
            Some(Value::Object(map)) => {
                for (name, value) in map {
                    let path = format!("connections.{name}");
                    if let Some(c) = section::<ConnectionConfig>(Some(value), &path, &mut problems)
                    {
                        connections.insert(name.clone(), c);
                    }
                }
            }
            Some(_) => problems.push(Problem {
                path: "connections".into(),
                message: "must be a mapping of connection names to settings".into(),
            }),
        }
        match ConfigError::from_problems(problems) {
            Some(e) => Err(e),
            None => Ok(ZRequestConfig {
                pool: pool.unwrap_or_default(),
                defaults: defaults.unwrap_or_default(),
                connections,
            }),
        }
    }
}

impl ConfigLoader for ConfigSource {
    type Config = ZRequestConfig;

    fn load(&self) -> Result<ZRequestConfig, ConfigError> {
        ConfigSource::load(self)
    }
}

/// Deserialises one section, recording a problem instead of failing on the first error.
fn section<T: DeserializeOwned + Default>(
    value: Option<&Value>,
    path: &str,
    problems: &mut Vec<Problem>,
) -> Option<T> {
    match value {
        None | Some(Value::Null) => Some(T::default()),
        Some(v) => match serde_json::from_value::<T>(v.clone()) {
            Ok(t) => Some(t),
            Err(e) => {
                problems.push(Problem {
                    path: path.to_string(),
                    message: e.to_string(),
                });
                None
            }
        },
    }
}
