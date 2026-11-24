//! The configuration file: what it may say, and what it means for a client
//! that has just presented its credentials.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Deserializer};
use subtle::ConstantTimeEq;

use crate::wire::frame::PAYLOAD_MAX;

/// The user name that opens the admin console instead of a pool.
pub const ADMIN_USER: &str = "rfbouncer";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: SocketAddr,
    pub admin_password: Option<String>,
    /// Opens the report pages and the metrics, and nothing else. Without
    /// it they take the admin password.
    pub report_password: Option<String>,
    #[serde(default = "default_max_clients")]
    pub max_client_conn: usize,
    #[serde(default = "default_max_request", deserialize_with = "size")]
    pub max_request_bytes: usize,
    #[serde(default = "secs_30", deserialize_with = "duration")]
    pub shutdown_timeout: Duration,
    /// How long a client may sit between two requests; zero is forever.
    #[serde(default, deserialize_with = "duration")]
    pub client_idle_timeout: Duration,
    #[serde(default)]
    pub stats: StatsConfig,
    pub pool: Vec<PoolConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatsConfig {
    /// The address of the Rayforce process that keeps the statistics.
    /// Without it nothing is recorded.
    pub rayforce: Option<String>,
    /// What that process was started with (-u).
    pub password: Option<String>,
    #[serde(default = "default_wire")]
    pub wire_version: u8,
    #[serde(default = "yes")]
    pub store_samples: bool,
    #[serde(default = "days_30", deserialize_with = "duration")]
    pub retention_aggregates: Duration,
    #[serde(default = "days_7", deserialize_with = "duration")]
    pub retention_samples: Duration,
    #[serde(default = "default_fingerprints")]
    pub max_fingerprints_per_minute: usize,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            rayforce: None,
            password: None,
            wire_version: default_wire(),
            store_samples: true,
            retention_aggregates: days_30(),
            retention_samples: days_7(),
            max_fingerprints_per_minute: default_fingerprints(),
        }
    }
}

/// How a pool chooses the replica for a request.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// The next replica that is free. A request waits only when every
    /// replica is busy, and then for whichever is free first.
    #[default]
    Free,
    /// The next replica in turn, free or not. Every replica gets the same
    /// number of requests, and a request may wait behind a slow one while
    /// another replica is idle.
    RoundRobin,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    pub name: String,
    /// The user names, from a client's credentials, that select this pool.
    #[serde(default)]
    pub users: Vec<String>,
    pub password: Option<String>,
    /// Takes the clients whose user names no pool.
    #[serde(default)]
    pub default: bool,
    #[serde(default = "default_wire")]
    pub wire_version: u8,
    pub backend_password: Option<String>,
    pub backends: Vec<String>,
    #[serde(default)]
    pub mode: Mode,
    /// Requests a replica runs at once. A Rayforce process evaluates one
    /// message at a time, so more than one only queues inside the replica.
    #[serde(default = "one")]
    pub slots: usize,
    /// Refuse a replica that lets a write through (one not started with -U).
    #[serde(default = "yes")]
    pub require_restricted: bool,
    #[serde(default = "yes")]
    pub retry: bool,
    #[serde(default)]
    pub affinity: bool,
    #[serde(default = "default_queue")]
    pub queue_max: usize,
    #[serde(default = "secs_10", deserialize_with = "duration")]
    pub query_wait_timeout: Duration,
    /// How long a replica may take to start answering; zero is no limit.
    #[serde(default = "secs_30", deserialize_with = "duration")]
    pub query_timeout: Duration,
    /// How long a client may take to receive an answer once it has begun;
    /// zero is no limit.
    #[serde(default = "secs_60", deserialize_with = "duration")]
    pub transfer_timeout: Duration,
    #[serde(default = "secs_5", deserialize_with = "duration")]
    pub cancel_grace: Duration,
    #[serde(default = "secs_5", deserialize_with = "duration")]
    pub connect_timeout: Duration,
}

fn default_max_clients() -> usize {
    10_000
}
fn default_max_request() -> usize {
    16 * 1024 * 1024
}
fn default_fingerprints() -> usize {
    2000
}
fn default_wire() -> u8 {
    4
}
fn default_queue() -> usize {
    1000
}
fn one() -> usize {
    1
}
fn yes() -> bool {
    true
}
fn secs_5() -> Duration {
    Duration::from_secs(5)
}
fn secs_10() -> Duration {
    Duration::from_secs(10)
}
fn secs_60() -> Duration {
    Duration::from_secs(60)
}
fn secs_30() -> Duration {
    Duration::from_secs(30)
}
fn days_7() -> Duration {
    Duration::from_secs(7 * 86_400)
}
fn days_30() -> Duration {
    Duration::from_secs(30 * 86_400)
}

fn duration<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    let text = String::deserialize(d)?;
    parse_duration(&text).ok_or_else(|| {
        serde::de::Error::custom(format!(
            "bad duration {text:?}: expected e.g. 500ms, 10s, 5m, 2h, 30d"
        ))
    })
}

fn size<'de, D: Deserializer<'de>>(d: D) -> Result<usize, D::Error> {
    let text = String::deserialize(d)?;
    parse_size(&text).ok_or_else(|| {
        serde::de::Error::custom(format!(
            "bad size {text:?}: expected e.g. 4096, 64KiB, 16MiB"
        ))
    })
}

fn split_unit(text: &str) -> Option<(u64, &str)> {
    let text = text.trim();
    let digits = text.bytes().take_while(u8::is_ascii_digit).count();
    let number = text[..digits].parse().ok()?;
    Some((number, text[digits..].trim()))
}

pub fn parse_duration(text: &str) -> Option<Duration> {
    let (n, unit) = split_unit(text)?;
    let ms = match unit {
        "ms" => 1,
        "s" => 1000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "" if n == 0 => 0,
        _ => return None,
    };
    Some(Duration::from_millis(n.checked_mul(ms)?))
}

pub fn parse_size(text: &str) -> Option<usize> {
    let (n, unit) = split_unit(text)?;
    let scale: u64 = match unit {
        "" | "B" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        _ => return None,
    };
    usize::try_from(n.checked_mul(scale)?).ok()
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path}: {source}")]
    Parse {
        path: PathBuf,
        source: Box<toml::de::Error>,
    },
    #[error("{0}")]
    Invalid(String),
}

/// Whom a client's credentials name.
#[derive(Debug, PartialEq, Eq)]
pub enum Target<'a> {
    Admin,
    Pool(&'a PoolConfig),
    Rejected,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let config: Self = toml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_owned(),
            source: Box::new(source),
        })?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let bad = |why: String| Err(ConfigError::Invalid(why));
        if self.pool.is_empty() {
            return bad("at least one [[pool]] is required".into());
        }
        if self.max_request_bytes == 0 || self.max_request_bytes > PAYLOAD_MAX {
            return bad("max_request_bytes must be between 1 and 256MiB".into());
        }
        if self.max_client_conn == 0 {
            return bad("max_client_conn must be at least 1".into());
        }
        if !matches!(self.stats.wire_version, 3 | 4) {
            return bad("[stats] wire_version must be 3 or 4".into());
        }
        if let Some(addr) = &self.stats.rayforce
            && !is_host_port(addr)
        {
            return bad(format!("[stats] rayforce {addr:?}: expected host:port"));
        }
        if self.pool.iter().filter(|p| p.default).count() > 1 {
            return bad("only one pool may be the default".into());
        }
        let mut names = HashSet::new();
        let mut users = HashSet::new();
        for p in &self.pool {
            if !names.insert(&p.name) {
                return bad(format!("pool {:?} is defined twice", p.name));
            }
            // A name goes into URLs and metric labels as it is.
            if p.name.is_empty()
                || !p
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            {
                return bad(format!(
                    "pool name {:?}: use letters, digits, - and _",
                    p.name
                ));
            }
            if !matches!(p.wire_version, 3 | 4) {
                return bad(format!("pool {:?}: wire_version must be 3 or 4", p.name));
            }
            if p.backends.is_empty() || p.slots == 0 {
                return bad(format!(
                    "pool {:?}: needs a backend and at least one slot",
                    p.name
                ));
            }
            if let Some(addr) = p.backends.iter().find(|a| !is_host_port(a)) {
                return bad(format!(
                    "pool {:?}: backend {addr:?}: expected host:port",
                    p.name
                ));
            }
            if p.queue_max == 0 {
                return bad(format!("pool {:?}: queue_max must be at least 1", p.name));
            }
            if p.affinity && p.mode == Mode::RoundRobin {
                return bad(format!(
                    "pool {:?}: affinity has no effect under mode = \"round_robin\"; remove one",
                    p.name
                ));
            }
            for u in &p.users {
                if u == ADMIN_USER || !users.insert(u) {
                    return bad(format!(
                        "pool {:?}: user {u:?} is reserved or selects two pools",
                        p.name
                    ));
                }
            }
            if self.pool.len() > 1 && p.users.is_empty() && !p.default {
                return bad(format!(
                    "pool {:?}: no client can reach it; give it users or default",
                    p.name
                ));
            }
        }
        Ok(())
    }

    /// Whether a client is asked for credentials. A client names its pool
    /// with them, so several pools need them even without a password.
    pub fn auth_required(&self) -> bool {
        self.pool.len() > 1 || self.pool.iter().any(|p| p.password.is_some())
    }

    pub fn speaks(&self, version: u8) -> bool {
        self.pool.iter().any(|p| p.wire_version == version)
    }

    pub fn pool(&self, name: &str) -> Option<&PoolConfig> {
        self.pool.iter().find(|p| p.name == name)
    }

    /// The pool a client lands in with no credentials asked.
    pub fn only_pool(&self) -> &PoolConfig {
        &self.pool[0]
    }

    pub fn resolve(&self, user: &str, password: &[u8], version: u8) -> Target<'_> {
        if user == ADMIN_USER {
            return match &self.admin_password {
                Some(p) if same(p.as_bytes(), password) => Target::Admin,
                _ => Target::Rejected,
            };
        }
        let named = self.pool.iter().find(|p| p.users.iter().any(|u| u == user));
        let pool = named.or_else(|| match &self.pool[..] {
            [only] => Some(only),
            pools => pools.iter().find(|p| p.default),
        });
        match pool {
            Some(p)
                if p.wire_version == version
                    && p.password
                        .as_ref()
                        .is_none_or(|x| same(x.as_bytes(), password)) =>
            {
                Target::Pool(p)
            }
            _ => Target::Rejected,
        }
    }
}

/// A name or address, a colon, and a port that is one.
fn is_host_port(addr: &str) -> bool {
    addr.rsplit_once(':')
        .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok_and(|p| p != 0))
}

fn same(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(text: &str) -> Result<Config, ConfigError> {
        let config: Config =
            toml::from_str(text).map_err(|e| ConfigError::Invalid(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    const TWO: &str = r#"
        listen = "127.0.0.1:5000"
        admin_password = "root"
        [[pool]]
        name = "main"
        users = ["app"]
        password = "a"
        wire_version = 3
        backends = ["127.0.0.1:1"]
        query_timeout = "30s"
        [[pool]]
        name = "reports"
        default = true
        backends = ["127.0.0.1:2"]
    "#;

    #[test]
    fn credentials_name_a_pool_or_the_admin() {
        let c = config(TWO).unwrap();
        assert!(c.auth_required());
        assert_eq!(c.pool[0].query_timeout, Duration::from_secs(30));
        let turn = config(&TWO.replace("default = true", "default = true\nmode = \"round_robin\""))
            .unwrap();
        assert_eq!(turn.pool[1].mode, Mode::RoundRobin);
        assert!(
            config(&TWO.replace("default = true", "default = true\nmode = \"random\"")).is_err()
        );
        assert_eq!(c.pool[1].query_timeout, Duration::from_secs(30));
        assert_eq!(c.pool[1].transfer_timeout, Duration::from_secs(60));
        assert_eq!(c.resolve("app", b"a", 3), Target::Pool(&c.pool[0]));
        assert_eq!(c.resolve("app", b"b", 3), Target::Rejected);
        assert_eq!(c.resolve("app", b"a", 4), Target::Rejected);
        assert_eq!(c.resolve("anyone", b"", 4), Target::Pool(&c.pool[1]));
        assert_eq!(c.resolve(ADMIN_USER, b"root", 4), Target::Admin);
        assert_eq!(c.resolve(ADMIN_USER, b"", 4), Target::Rejected);
    }

    #[test]
    fn one_pool_without_a_password_asks_for_nothing() {
        let c = config("listen = \"127.0.0.1:1\"\n[[pool]]\nname = \"p\"\nbackends = [\"h:1\"]")
            .unwrap();
        assert!(!c.auth_required());
        assert_eq!(c.only_pool().slots, 1);
        assert!(c.only_pool().require_restricted);
        assert_eq!(c.only_pool().mode, Mode::Free);
    }

    #[test]
    fn a_configuration_that_cannot_work_is_refused() {
        assert!(config("listen = \"127.0.0.1:1\"\npool = []").is_err());
        assert!(config(&TWO.replace("default = true", "")).is_err());
        assert!(config(&TWO.replace("reports", "main")).is_err());
        assert!(config(&TWO.replace("\"app\"", "\"rfbouncer\"")).is_err());
        assert!(config(&TWO.replace("wire_version = 3", "wire_version = 9")).is_err());
        assert!(config(&TWO.replace("30s", "30 parsecs")).is_err());
        assert!(
            config(&TWO.replace("backends = [\"127.0.0.1:1\"]", "backends = [\"localhost\"]"))
                .is_err()
        );
        assert!(config(&TWO.replace("default = true", "default = true\nqueue_max = 0")).is_err());
        assert!(
            config(&TWO.replace(
                "default = true",
                "default = true\naffinity = true\nmode = \"round_robin\""
            ))
            .is_err()
        );
        assert!(config(&format!("max_client_conn = 0\n{TWO}")).is_err());
        assert!(
            config(&TWO.replace(
                "[[pool]]\n        name = \"main\"",
                "[stats]\n        wire_version = 9\n        [[pool]]\n        name = \"main\""
            ))
            .is_err()
        );
    }

    #[test]
    fn durations_and_sizes_parse() {
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(
            parse_duration("30d"),
            Some(Duration::from_secs(30 * 86_400))
        );
        assert_eq!(parse_duration("0"), Some(Duration::ZERO));
        assert_eq!(parse_duration("5"), None);
        assert_eq!(parse_size("16MiB"), Some(16 << 20));
        assert_eq!(parse_size("4096"), Some(4096));
        assert_eq!(parse_size("1MB"), None);
    }
}
