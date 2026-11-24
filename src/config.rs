//! The configuration file: what it may say, and what it means for a client
//! that has just presented its credentials.

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
    /// Bind the port with SO_REUSEPORT, so that a second bouncer may be
    /// started on it while this one is still serving.
    #[serde(default)]
    pub reuse_port: bool,
    #[serde(default)]
    pub stats: StatsConfig,
    pub pool: Vec<PoolConfig>,
    /// Where the admin console, the report pages and the metrics are served
    /// instead of `listen`, which then takes pool clients and `/healthz`
    /// only.
    pub admin_listen: Option<SocketAddr>,
    pub tls: Option<TlsConfig>,
    /// Refused logins from one address, within `auth_lockout_window`, that
    /// lock it out for `auth_lockout_period`; zero never locks.
    #[serde(default = "default_lockout_failures")]
    pub auth_lockout_failures: u32,
    #[serde(default = "secs_60", deserialize_with = "duration")]
    pub auth_lockout_window: Duration,
    #[serde(default = "mins_5", deserialize_with = "duration")]
    pub auth_lockout_period: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// The certificate chain, leaf first, in PEM.
    pub cert: PathBuf,
    /// The leaf's private key, in PEM.
    pub key: PathBuf,
    /// Refuse HTTP in clear text, `/healthz` excepted.
    #[serde(default)]
    pub require_for_http: bool,
    /// Refuse Rayforce IPC in clear text, the admin console included.
    #[serde(default)]
    pub require_for_ipc: bool,
    /// What the two files held when the configuration was read. It lives
    /// here so that a reload replaces the certificate together with the
    /// rest, or not at all.
    #[serde(skip)]
    pub server: Option<Arc<rustls::ServerConfig>>,
}

impl TlsConfig {
    fn load(&self) -> Result<Arc<rustls::ServerConfig>, ConfigError> {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        let bad = |path: &Path, why: String| {
            ConfigError::Invalid(format!("[tls] {}: {why}", path.display()))
        };
        let chain = CertificateDer::pem_file_iter(&self.cert)
            .and_then(Iterator::collect::<Result<Vec<_>, _>>)
            .map_err(|e| bad(&self.cert, e.to_string()))?;
        if chain.is_empty() {
            return Err(bad(&self.cert, "no certificate in it".into()));
        }
        let key =
            PrivateKeyDer::from_pem_file(&self.key).map_err(|e| bad(&self.key, e.to_string()))?;
        // The provider is named rather than taken from the process default:
        // there is then nothing to install first and nothing to forget.
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .and_then(|b| b.with_no_client_auth().with_single_cert(chain, key))
            .map_err(|e| bad(&self.key, e.to_string()))?;
        Ok(Arc::new(server))
    }
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

/// Whose answers a pool compresses.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Compress {
    /// Those of clients on another host, as a Rayforce server does.
    #[default]
    Remote,
    /// Every client's, a local one's too.
    Always,
    Off,
}

impl Compress {
    /// Whether a client at this address is sent compressed answers. What is
    /// local is what `ray_sock_addr_is_local` says: 127/8, ::1, and a
    /// loopback address mapped into IPv6.
    pub fn applies(self, client: IpAddr) -> bool {
        let local = match client {
            IpAddr::V4(a) => a.is_loopback(),
            IpAddr::V6(a) => a.is_loopback() || a.to_ipv4_mapped().is_some_and(|a| a.is_loopback()),
        };
        match self {
            Self::Remote => !local,
            Self::Always => true,
            Self::Off => false,
        }
    }
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
    /// Whose answers are compressed on their way out.
    #[serde(default)]
    pub compress: Compress,
    /// The largest answer compressed. Compressing needs the whole answer in
    /// memory; a larger one is passed on as it comes.
    #[serde(default = "mib_16", deserialize_with = "size")]
    pub compress_max_bytes: usize,
    /// How long an answer may be given again to the same request without
    /// asking a replica; zero is never.
    #[serde(default, deserialize_with = "duration")]
    pub cache_ttl: Duration,
    /// The most memory the kept answers and their requests may take.
    #[serde(default = "mib_64", deserialize_with = "size")]
    pub cache_max_bytes: usize,
    /// The largest answer kept, counted together with its request.
    #[serde(default = "mib_1", deserialize_with = "size")]
    pub cache_max_entry: usize,
    /// How long a replica connection sits idle before it is asked whether
    /// the replica still answers; zero is never.
    #[serde(default = "secs_10", deserialize_with = "duration")]
    pub health_check_interval: Duration,
    /// How long the replica has to answer that; zero is no check at all.
    #[serde(default = "secs_5", deserialize_with = "duration")]
    pub health_check_timeout: Duration,
}

fn mib_1() -> usize {
    1 << 20
}
fn mib_16() -> usize {
    16 << 20
}
fn mib_64() -> usize {
    64 << 20
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
fn mins_5() -> Duration {
    Duration::from_secs(300)
}
fn default_lockout_failures() -> u32 {
    10
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
        let mut config: Self = toml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_owned(),
            source: Box::new(source),
        })?;
        config.validate()?;
        // Read here, so that `--check` and a reload refuse a certificate
        // that cannot be served.
        if let Some(tls) = &mut config.tls {
            tls.server = Some(tls.load()?);
        }
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
        if self.admin_listen == Some(self.listen) {
            return bad(
                "admin_listen must differ from listen; leave it out to share the port".into(),
            );
        }
        if self.auth_lockout_failures > 0
            && (self.auth_lockout_window.is_zero() || self.auth_lockout_period.is_zero())
        {
            return bad(
                "auth_lockout_window and auth_lockout_period must be more than 0; set auth_lockout_failures = 0 to turn the lockout off"
                    .into(),
            );
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
        assert!(!c.reuse_port);
        assert_eq!(c.only_pool().health_check_interval, Duration::from_secs(10));
        assert_eq!(c.only_pool().health_check_timeout, Duration::from_secs(5));
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
        assert!(config(&format!("admin_listen = \"127.0.0.1:5000\"\n{TWO}")).is_err());
        assert!(config(&format!("auth_lockout_period = \"0\"\n{TWO}")).is_err());
        let off = format!("auth_lockout_failures = 0\nauth_lockout_period = \"0\"\n{TWO}");
        assert!(config(&off).is_ok());
        assert!(
            config(&TWO.replace(
                "[[pool]]\n        name = \"main\"",
                "[stats]\n        wire_version = 9\n        [[pool]]\n        name = \"main\""
            ))
            .is_err()
        );
    }

    #[test]
    fn compression_is_for_clients_on_another_host() {
        let c = config(TWO).unwrap();
        assert_eq!(c.pool[0].compress, Compress::Remote);
        assert_eq!(c.pool[0].compress_max_bytes, 16 << 20);
        assert_eq!(c.pool[0].cache_ttl, Duration::ZERO);
        let ip = |text: &str| text.parse::<IpAddr>().unwrap();
        for local in ["127.0.0.1", "127.8.9.10", "::1", "::ffff:127.0.0.1"] {
            assert!(!Compress::Remote.applies(ip(local)), "{local}");
            assert!(Compress::Always.applies(ip(local)));
        }
        for remote in ["10.0.0.1", "::ffff:10.0.0.1", "fe80::1"] {
            assert!(Compress::Remote.applies(ip(remote)), "{remote}");
            assert!(!Compress::Off.applies(ip(remote)));
        }
        let set = TWO.replace(
            "default = true",
            "default = true\ncompress = \"always\"\ncache_ttl = \"2s\"\ncache_max_entry = \"64KiB\"",
        );
        let c = config(&set).unwrap();
        assert_eq!(c.pool[1].compress, Compress::Always);
        assert_eq!(c.pool[1].cache_ttl, Duration::from_secs(2));
        assert_eq!(c.pool[1].cache_max_entry, 64 << 10);
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
