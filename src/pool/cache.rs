//! A pool's result cache: answers kept for a short while and given again to
//! a request with the same bytes, without asking a replica.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;

/// A request frame, and whether its client is sent compressed answers: the
/// two kinds of client are answered with different bytes.
type Key = (Bytes, bool);

struct Entry {
    answer: Bytes,
    /// When the request was handed to its replica: the answer says nothing
    /// of what changed after that.
    born: Instant,
}

#[derive(Default)]
struct Inner {
    map: HashMap<Key, Entry>,
    /// Every entry made, oldest first. One whose entry was replaced or
    /// removed since stays here until it comes to the front.
    order: VecDeque<(Instant, Key)>,
    bytes: usize,
    emptied: Option<Instant>,
}

impl Inner {
    fn remove(&mut self, key: &Key) -> Option<Key> {
        let (key, entry) = self.map.remove_entry(key)?;
        self.bytes -= key.0.len() + entry.answer.len();
        Some(key)
    }
}

#[derive(Default)]
pub struct Cache {
    inner: Mutex<Inner>,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl Cache {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The answer kept for this request, unless it is `ttl` old.
    pub fn get(&self, frame: &Bytes, compressed: bool, ttl: Duration) -> Option<Bytes> {
        let key = (frame.clone(), compressed);
        let mut inner = self.lock();
        let entry = inner.map.get(&key)?;
        if entry.born.elapsed() >= ttl {
            inner.remove(&key);
            return None;
        }
        self.hits.fetch_add(1, Relaxed);
        Some(entry.answer.clone())
    }

    /// Keeps `answer`, a whole frame, for the request `frame`, making room
    /// by letting the oldest entries go.
    pub fn insert(
        &self,
        frame: &Bytes,
        compressed: bool,
        answer: Bytes,
        born: Instant,
        cfg: &crate::config::PoolConfig,
    ) {
        let size = frame.len() + answer.len();
        if size > cfg.cache_max_entry.min(cfg.cache_max_bytes) || born.elapsed() >= cfg.cache_ttl {
            return;
        }
        let mut inner = self.lock();
        // Whoever emptied the cache meant what replicas had answered until
        // then, and this is one such answer, late.
        if inner.emptied.is_some_and(|at| born <= at) {
            return;
        }
        // The key is a copy: the frame as it was read shares its memory with
        // whatever else that client sent, which must not be held along.
        let key = inner
            .remove(&(frame.clone(), compressed))
            .unwrap_or_else(|| (Bytes::copy_from_slice(frame), compressed));
        while let Some((made, _)) = inner.order.front() {
            if made.elapsed() < cfg.cache_ttl && inner.bytes + size <= cfg.cache_max_bytes {
                break;
            }
            if let Some((made, old)) = inner.order.pop_front()
                && inner.map.get(&old).is_some_and(|e| e.born == made)
            {
                inner.remove(&old);
            }
        }
        inner.bytes += size;
        inner.order.push_back((born, key.clone()));
        inner.map.insert(key, Entry { answer, born });
    }

    pub fn clear(&self) {
        let mut inner = self.lock();
        *inner = Inner {
            emptied: Some(Instant::now()),
            ..Inner::default()
        };
    }

    /// How many answers are kept, and the bytes they and their requests take.
    pub fn size(&self) -> (usize, usize) {
        let inner = self.lock();
        (inner.map.len(), inner.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PoolConfig;

    fn cfg(ttl_ms: u64, max_bytes: usize, max_entry: usize) -> PoolConfig {
        let text = format!(
            "name = \"p\"\nbackends = [\"h:1\"]\ncache_ttl = \"{ttl_ms}ms\"\n\
             cache_max_bytes = \"{max_bytes}\"\ncache_max_entry = \"{max_entry}\""
        );
        toml::from_str(&text).unwrap()
    }

    fn bytes(byte: u8, len: usize) -> Bytes {
        Bytes::from(vec![byte; len])
    }

    #[test]
    fn an_answer_is_given_again_to_the_same_request_and_client_kind() {
        let (cache, c) = (Cache::default(), cfg(60_000, 1000, 100));
        cache.insert(&bytes(1, 10), false, bytes(9, 20), Instant::now(), &c);
        assert_eq!(
            cache.get(&bytes(1, 10), false, c.cache_ttl),
            Some(bytes(9, 20))
        );
        assert_eq!(cache.get(&bytes(1, 10), true, c.cache_ttl), None);
        assert_eq!(cache.get(&bytes(2, 10), false, c.cache_ttl), None);
        assert_eq!(cache.size(), (1, 30));
        assert_eq!(cache.hits.load(Relaxed), 1);
        // Kept again, it takes the place of what was there.
        cache.insert(&bytes(1, 10), false, bytes(8, 5), Instant::now(), &c);
        assert_eq!(
            cache.get(&bytes(1, 10), false, c.cache_ttl),
            Some(bytes(8, 5))
        );
        assert_eq!(cache.size(), (1, 15));
    }

    #[test]
    fn nothing_as_old_as_the_ttl_is_served_or_stored() {
        let (cache, c) = (Cache::default(), cfg(50, 1000, 100));
        let old = Instant::now() - Duration::from_millis(50);
        cache.insert(&bytes(1, 10), false, bytes(9, 20), old, &c);
        assert_eq!(cache.size(), (0, 0));
        let young = Instant::now() - Duration::from_millis(10);
        cache.insert(&bytes(1, 10), false, bytes(9, 20), young, &c);
        // The same entry under a shorter ttl is too old, and goes.
        assert_eq!(
            cache.get(&bytes(1, 10), false, Duration::from_millis(10)),
            None
        );
        assert_eq!(cache.size(), (0, 0));
    }

    #[test]
    fn the_oldest_go_to_keep_within_the_bytes_allowed() {
        let (cache, c) = (Cache::default(), cfg(60_000, 100, 40));
        for i in 0..10 {
            cache.insert(&bytes(i, 10), false, bytes(i, 20), Instant::now(), &c);
            assert!(cache.size().1 <= 100);
        }
        assert_eq!(cache.size(), (3, 90));
        assert_eq!(cache.get(&bytes(6, 10), false, c.cache_ttl), None);
        assert_eq!(
            cache.get(&bytes(7, 10), false, c.cache_ttl),
            Some(bytes(7, 20))
        );
        // Past the largest entry allowed, it is not kept at all.
        cache.insert(&bytes(20, 10), false, bytes(0, 31), Instant::now(), &c);
        assert_eq!(cache.get(&bytes(20, 10), false, c.cache_ttl), None);
        assert_eq!(cache.size(), (3, 90));
    }

    #[test]
    fn emptying_also_keeps_out_what_was_already_running() {
        let (cache, c) = (Cache::default(), cfg(60_000, 1000, 100));
        let started = Instant::now();
        cache.insert(&bytes(1, 10), false, bytes(9, 20), started, &c);
        cache.clear();
        assert_eq!(cache.size(), (0, 0));
        cache.insert(&bytes(2, 10), false, bytes(9, 20), started, &c);
        assert_eq!(cache.size(), (0, 0));
        // The clock may not have moved since the emptying: one tick later.
        let later = Instant::now() + Duration::from_millis(1);
        cache.insert(&bytes(2, 10), false, bytes(9, 20), later, &c);
        assert_eq!(cache.size(), (1, 30));
    }
}
