//! Addresses that kept presenting wrong credentials, and are refused a
//! login for a while whatever they present.
//!
//! An address is the peer of a TCP connection whose handshake completed, so
//! it cannot be forged by someone who does not receive what is sent to it:
//! nobody can have another address locked out from afar. Clients behind one
//! NAT or proxy do share an address, and are locked out together.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::config::Config;

/// Addresses remembered at once.
// shortcut: past this many addresses failing within one window, a new one is
// not counted and so never locked out; it is still delayed. Evict the oldest
// instead if that ever matters.
const TRACKED_MAX: usize = 10_000;

struct Entry {
    failures: u32,
    /// The first failure of the window being counted.
    since: Instant,
    locked_until: Option<Instant>,
}

#[derive(Default)]
pub struct Lockout {
    table: Mutex<HashMap<IpAddr, Entry>>,
    /// Times an address was locked out.
    pub lockouts: AtomicU64,
    /// Logins refused unexamined because their address was locked out.
    pub turned_away: AtomicU64,
}

/// What is counted as one client: an IPv4 address, or an IPv6 /64, since
/// one host usually has a whole /64 to pick addresses from.
fn key(addr: IpAddr) -> IpAddr {
    match addr.to_canonical() {
        IpAddr::V6(v6) => IpAddr::V6((u128::from(v6) & !u128::from(u64::MAX)).into()),
        v4 => v4,
    }
}

impl Lockout {
    /// How long `addr` stays locked out, if it is. Counts the login this is
    /// asked for as turned away.
    pub fn locked(&self, addr: IpAddr, config: &Config) -> Option<Duration> {
        self.locked_at(addr, config, Instant::now())
    }

    fn locked_at(&self, addr: IpAddr, config: &Config, now: Instant) -> Option<Duration> {
        if config.auth_lockout_failures == 0 {
            return None;
        }
        let table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        let until = table.get(&key(addr))?.locked_until?;
        let left = until.checked_duration_since(now).filter(|d| !d.is_zero())?;
        self.turned_away.fetch_add(1, Relaxed);
        Some(left)
    }

    /// Counts a refused login, and locks the address out if it is one too
    /// many.
    pub fn failed(&self, addr: IpAddr, config: &Config) {
        self.failed_at(addr, config, Instant::now());
    }

    fn failed_at(&self, addr: IpAddr, config: &Config, now: Instant) {
        let (limit, window) = (config.auth_lockout_failures, config.auth_lockout_window);
        if limit == 0 {
            return;
        }
        let key = key(addr);
        let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        if table.len() >= TRACKED_MAX && !table.contains_key(&key) {
            table.retain(|_, e| {
                e.locked_until.is_some_and(|until| until > now)
                    || now.duration_since(e.since) <= window
            });
            if table.len() >= TRACKED_MAX {
                return;
            }
        }
        let entry = table.entry(key).or_insert(Entry {
            failures: 0,
            since: now,
            locked_until: None,
        });
        if now.duration_since(entry.since) > window {
            entry.failures = 0;
            entry.since = now;
        }
        entry.failures += 1;
        if entry.failures >= limit {
            entry.failures = 0;
            entry.locked_until = Some(now + config.auth_lockout_period);
            self.lockouts.fetch_add(1, Relaxed);
            tracing::warn!(
                address = %key,
                failures = limit,
                period = ?config.auth_lockout_period,
                "address locked out: its logins are refused until the period ends"
            );
        }
    }

    /// Addresses locked out at this moment.
    pub fn locked_now(&self) -> usize {
        let now = Instant::now();
        let table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        table
            .values()
            .filter(|e| e.locked_until.is_some_and(|until| until > now))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The defaults, a minute's window and five minutes locked, with
    /// `limit` failures allowed.
    fn config(limit: u32) -> Config {
        let text = format!(
            "listen = \"127.0.0.1:1\"\nauth_lockout_failures = {limit}\n[[pool]]\nname = \"p\"\nbackends = [\"h:1\"]"
        );
        toml::from_str(&text).unwrap()
    }

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    #[test]
    fn the_third_failure_in_a_window_locks_for_the_period() {
        let (lockout, c, t) = (Lockout::default(), config(3), Instant::now());
        let (a, b) = (ip("10.0.0.1"), ip("10.0.0.2"));
        lockout.failed_at(a, &c, t);
        lockout.failed_at(a, &c, t);
        assert_eq!(lockout.locked_at(a, &c, t), None);
        lockout.failed_at(a, &c, t + Duration::from_secs(59));
        assert_eq!(
            lockout.locked_at(a, &c, t + Duration::from_secs(60)),
            Some(Duration::from_secs(299))
        );
        assert_eq!(lockout.locked_at(b, &c, t + Duration::from_secs(60)), None);
        assert_eq!(lockout.locked_at(a, &c, t + Duration::from_secs(359)), None);
        assert_eq!(lockout.lockouts.load(Relaxed), 1);
        assert_eq!(lockout.turned_away.load(Relaxed), 1);
    }

    #[test]
    fn failures_spread_wider_than_the_window_do_not_lock() {
        let (lockout, c, t) = (Lockout::default(), config(3), Instant::now());
        let a = ip("10.0.0.1");
        for minutes in 0..10 {
            lockout.failed_at(a, &c, t + Duration::from_secs(61 * minutes));
        }
        assert_eq!(lockout.lockouts.load(Relaxed), 0);
    }

    #[test]
    fn zero_turns_it_off_and_lifts_what_was_locked() {
        let (lockout, c, t) = (Lockout::default(), config(3), Instant::now());
        let a = ip("10.0.0.1");
        (0..3).for_each(|_| lockout.failed_at(a, &c, t));
        assert!(lockout.locked_at(a, &c, t).is_some());
        let off = config(0);
        assert_eq!(lockout.locked_at(a, &off, t), None);
        let b = ip("10.0.0.2");
        (0..9).for_each(|_| lockout.failed_at(b, &off, t));
        assert_eq!(lockout.locked_at(b, &c, t), None);
    }

    #[test]
    fn one_ipv6_network_is_one_client_and_a_mapped_address_is_its_ipv4() {
        let (lockout, c, t) = (Lockout::default(), config(3), Instant::now());
        lockout.failed_at(ip("2001:db8:0:1::1"), &c, t);
        lockout.failed_at(ip("2001:db8:0:1::2"), &c, t);
        lockout.failed_at(ip("2001:db8:0:1:ffff::3"), &c, t);
        assert!(lockout.locked_at(ip("2001:db8:0:1::9"), &c, t).is_some());
        assert_eq!(lockout.locked_at(ip("2001:db8:0:2::1"), &c, t), None);
        (0..3).for_each(|_| lockout.failed_at(ip("::ffff:10.0.0.1"), &c, t));
        assert!(lockout.locked_at(ip("10.0.0.1"), &c, t).is_some());
    }

    #[test]
    fn the_table_is_bounded_and_an_address_past_it_is_never_locked() {
        let (lockout, c, t) = (Lockout::default(), config(3), Instant::now());
        for n in 0..TRACKED_MAX as u32 + 50 {
            lockout.failed_at(IpAddr::V4((0x0a00_0000 + n).into()), &c, t);
        }
        assert_eq!(lockout.table.lock().unwrap().len(), TRACKED_MAX);
        // Once the window has passed the stale addresses make room.
        let later = t + Duration::from_secs(61);
        (0..3).for_each(|_| lockout.failed_at(ip("192.168.0.1"), &c, later));
        assert!(lockout.locked_at(ip("192.168.0.1"), &c, later).is_some());
        assert_eq!(lockout.table.lock().unwrap().len(), 1);
    }
}
