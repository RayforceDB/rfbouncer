//! What the whole process shares: the configuration in force, the pools,
//! and who is connected.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::Instant;

use tokio::sync::watch;

use crate::config::{Config, ConfigError};
use crate::pool::{Cmd, PoolHandle, dispatcher};
use crate::stats::Stats;

pub struct ClientInfo {
    pub addr: SocketAddr,
    pub user: Arc<str>,
    pub pool: Arc<str>,
    pub since: Instant,
    pub requests: u64,
}

pub struct Core {
    pub config_path: PathBuf,
    config: RwLock<Arc<Config>>,
    pools: RwLock<HashMap<String, PoolHandle>>,
    pub stats: Arc<Stats>,
    clients: Mutex<HashMap<u64, ClientInfo>>,
    next_id: AtomicU64,
    pub shutdown: watch::Sender<bool>,
    pub started: Instant,
}

/// A lock is only ever held for a few field accesses, so a panic inside one
/// leaves nothing half-done worth refusing to read.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Core {
    /// Must be called inside the runtime: it starts the pools' tasks.
    pub fn new(config_path: PathBuf, config: Config, stats: Arc<Stats>) -> Arc<Self> {
        let pools = config
            .pool
            .iter()
            .map(|p| {
                (
                    p.name.clone(),
                    dispatcher::spawn(Arc::new(p.clone()), stats.clone()),
                )
            })
            .collect();
        Arc::new(Self {
            config_path,
            config: RwLock::new(Arc::new(config)),
            pools: RwLock::new(pools),
            stats,
            clients: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            shutdown: watch::channel(false).0,
            started: Instant::now(),
        })
    }

    pub fn config(&self) -> Arc<Config> {
        self.config
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn pool(&self, name: &str) -> Option<PoolHandle> {
        self.pools
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    /// The pools, in the configuration's order.
    pub fn pools(&self) -> Vec<PoolHandle> {
        let pools = self.pools.read().unwrap_or_else(PoisonError::into_inner);
        self.config()
            .pool
            .iter()
            .filter_map(|p| pools.get(&p.name).cloned())
            .collect()
    }

    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Relaxed)
    }

    /// Reads the file again and brings the pools to what it says. The listen
    /// address is the one thing that needs a restart.
    pub fn reload(&self) -> Result<(), ConfigError> {
        let new = Config::load(&self.config_path)?;
        let old = self.config();
        if new.listen != old.listen {
            tracing::warn!("listen changed in the configuration: restart to apply it");
        }
        let mut pools = self.pools.write().unwrap_or_else(PoisonError::into_inner);
        pools.retain(|name, handle| {
            let stays = new.pool(name).is_some();
            if !stays {
                handle.send(Cmd::Close);
            }
            stays
        });
        for p in &new.pool {
            match pools.get(&p.name) {
                Some(handle) if old.pool(&p.name) != Some(p) => {
                    handle.send(Cmd::Reconfigure(Arc::new(p.clone())));
                }
                Some(_) => {}
                None => {
                    pools.insert(
                        p.name.clone(),
                        dispatcher::spawn(Arc::new(p.clone()), self.stats.clone()),
                    );
                }
            }
        }
        drop(pools);
        *self.config.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(new);
        tracing::info!("configuration reloaded");
        Ok(())
    }

    pub fn client_count(&self) -> usize {
        lock(&self.clients).len()
    }

    pub fn with_clients<R>(&self, f: impl FnOnce(&HashMap<u64, ClientInfo>) -> R) -> R {
        f(&lock(&self.clients))
    }

    /// Lists a client until the returned guard is dropped.
    pub fn register(self: &Arc<Self>, info: ClientInfo) -> ClientGuard {
        let id = self.next_id();
        lock(&self.clients).insert(id, info);
        ClientGuard {
            core: self.clone(),
            id,
        }
    }
}

pub struct ClientGuard {
    core: Arc<Core>,
    id: u64,
}

impl ClientGuard {
    pub fn count_request(&self) {
        if let Some(c) = lock(&self.core.clients).get_mut(&self.id) {
            c.requests += 1;
        }
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        lock(&self.core.clients).remove(&self.id);
    }
}
