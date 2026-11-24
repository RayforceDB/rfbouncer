//! What each replica says about itself, and what that means for the choice
//! of a replica.
//!
//! One connection per replica address carries the replica's status stream
//! (`wire::status`). It is opened and reopened by a task of its own and is
//! never in a request's way: a replica without the stream, or whose stream
//! broke, serves requests exactly as before and is merely not known.
//! What the stream says is information for the choice and for the operator;
//! it is never a verdict on whether the replica is alive.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use crate::config::PoolConfig;
use crate::wire::frame::{HEADER_LEN, Header};
use crate::wire::greeting;
use crate::wire::status::{self as wire, Frame, MSG_STATUS, State};

/// A stream silent for this many intervals says nothing any more, and what
/// it said last is not used past that.
const QUIET: u32 = 3;
/// A replica answers the greeting from its evaluating thread, which may be
/// busy for as long as a query takes.
const GREET_TIMEOUT: Duration = Duration::from_secs(60);
/// A stream that worked and broke is tried again this soon, and no stream
/// is tried again sooner.
const RETRY_LOST: Duration = Duration::from_secs(5);
/// A replica that never sent a status frame most likely cannot: it is asked
/// again this rarely, and when the pool's own connection to it comes up,
/// which is when it may have been replaced by a newer one.
const RETRY_ABSENT: Duration = Duration::from_secs(300);
/// A status payload is a few hundred bytes; a frame larger than this is not
/// one.
const FRAME_MAX: usize = 4096;
/// The share of its memory limit from which a replica counts as loaded for
/// that alone.
const MEM_PRESSURE: f64 = 0.8;

/// A status frame, when it arrived, and what follows from it and the frame
/// before it.
#[derive(Debug, Clone)]
pub struct Reading {
    pub frame: Frame,
    pub at: Instant,
    /// The interval the stream was asked for.
    pub interval: Duration,
    /// The share of the last interval the replica's main loop spent working.
    pub util: Option<f64>,
    /// CPU cores used over the last interval, all threads together.
    pub cpu: Option<f64>,
}

fn ratio(part: Option<u64>, whole: Option<u64>) -> Option<f64> {
    match (part, whole) {
        (Some(part), Some(whole)) if whole > 0 => Some(part as f64 / whole as f64),
        _ => None,
    }
}

impl Reading {
    /// The counters are cumulative, so a rate needs the frame before. A
    /// replica that restarted counts from zero again: its first frame has
    /// no interval behind it, and neither has one whose counters ran
    /// backwards.
    pub fn after(prev: Option<&Reading>, frame: Frame, at: Instant, interval: Duration) -> Self {
        let prev = prev.map(|p| &p.frame).filter(|p| p.pid == frame.pid);
        let cpu_ns = |f: &Frame| f.cpu_user_ns.checked_add(f.cpu_sys_ns);
        let util = prev.and_then(|p| {
            ratio(
                frame.loop_busy_ns.checked_sub(p.loop_busy_ns),
                frame.loop_total_ns.checked_sub(p.loop_total_ns),
            )
        });
        let cpu = prev.and_then(|p| {
            let used = cpu_ns(&frame)
                .zip(cpu_ns(p))
                .and_then(|(a, b)| a.checked_sub(b));
            let wall = frame.now.checked_sub(p.now).and_then(|d| d.try_into().ok());
            ratio(used, wall)
        });
        Self {
            frame,
            at,
            interval,
            util: util.map(|u| u.min(1.0)),
            cpu,
        }
    }

    fn fresh(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.at) <= self.interval * QUIET
    }

    /// Resident memory as a share of the replica's limit, where it has one.
    pub fn mem_share(&self) -> Option<f64> {
        ratio(Some(self.frame.rss_bytes), Some(self.frame.mem_limit_bytes))
    }

    /// For how long the replica had been running what it runs, by its own
    /// clock, when it wrote the frame.
    pub fn running_for(&self) -> Option<Duration> {
        let f = &self.frame;
        if f.state == State::Idle || f.busy_since <= 0 {
            return None;
        }
        let nanos = f.now.checked_sub(f.busy_since)?;
        Some(Duration::from_nanos(u64::try_from(nanos).ok()?))
    }

    /// Whether the replica is running something the bouncer did not send.
    ///
    /// `ours` is how many requests the bouncer has on the replica now, and
    /// `ours_ended` when its last one there ended. The frame is up to an
    /// interval old and the bouncer's knowledge of its own requests is not:
    /// a busy frame counts only when nothing of ours is there and what it
    /// describes began after our last request ended. Otherwise it may be
    /// our own request, seen late.
    pub fn not_ours(&self, ours: usize, ours_ended: Option<Instant>) -> bool {
        let began = self.at.checked_sub(self.running_for().unwrap_or_default());
        self.frame.state != State::Idle
            && ours == 0
            && ours_ended.is_none_or(|ended| began.is_some_and(|began| began > ended))
    }
}

/// How good a choice a free connection is: the smaller the better, compared
/// field by field in this order.
#[derive(Debug, PartialEq, PartialOrd)]
pub struct Rank {
    /// The replica is running something that is not ours. Such a replica is
    /// chosen only when every free one is in that state.
    pub not_ours: bool,
    /// Requests of ours it is running on its other connections.
    pub ours: usize,
    /// Its utilisation, 0 for idle and 1 for a loop that never rests. For
    /// a replica that says nothing of its load, what the caller assumes.
    pub load: f64,
}

/// A replica's load as far as it is known.
///
/// `own` is the time requests of ours ran on the replica since the frame
/// arrived. The frame cannot know of it, and without it every request of an
/// interval would go to the replica that looked idlest when the interval
/// began.
///
/// Memory is a penalty and not a tie-breaker: two utilisations are never
/// equal, so a tie-breaker would never be asked, and below the pressure
/// mark the memory a replica holds says nothing about how soon it answers.
/// Past the mark the penalty grows to that of a fully loaded replica at the
/// limit, where the next query may fail for want of memory.
pub fn load(reading: Option<&Reading>, own: Duration) -> Option<f64> {
    let r = reading?;
    let pressure = (r.mem_share().unwrap_or(0.0) - MEM_PRESSURE).max(0.0) / (1.0 - MEM_PRESSURE);
    Some(r.util? + own.as_secs_f64() / r.interval.as_secs_f64() + pressure)
}

/// Ranks a free connection of a replica. One that says nothing of its load
/// is taken to carry `unknown`: the caller gives the average of the replicas
/// that do say, so a pool where only some have a stream neither starves the
/// silent ones nor sends them everything.
pub fn rank(
    reading: Option<&Reading>,
    ours: usize,
    ours_ended: Option<Instant>,
    own: Duration,
    unknown: f64,
) -> Rank {
    Rank {
        not_ours: reading.is_some_and(|r| r.not_ours(ours, ours_ended)),
        ours,
        load: load(reading, own).unwrap_or(unknown),
    }
}

/// What a status task and the pool share.
#[derive(Default)]
struct Slot {
    reading: Mutex<Option<Arc<Reading>>>,
    /// A frame has arrived at some time: a stream that is silent now was
    /// lost, not absent.
    ever_up: AtomicBool,
    kick: Notify,
}

impl Slot {
    fn set(&self, reading: Option<Arc<Reading>>) {
        *self.reading.lock().unwrap_or_else(PoisonError::into_inner) = reading;
    }
}

/// The status stream of one replica address. Dropping it ends its task.
pub struct Monitor {
    slot: Arc<Slot>,
    task: JoinHandle<()>,
}

impl Drop for Monitor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Monitor {
    /// Must be called inside the runtime.
    pub fn spawn(pool: Arc<str>, addr: Arc<str>, cfg: watch::Receiver<Arc<PoolConfig>>) -> Self {
        let slot = Arc::new(Slot::default());
        let task = tokio::spawn(run(pool, addr, cfg, slot.clone()));
        Self { slot, task }
    }

    /// The last frame, unless the stream has gone quiet since.
    pub fn reading(&self) -> Option<Arc<Reading>> {
        let reading = self
            .slot
            .reading
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        reading.clone().filter(|r| r.fresh(Instant::now()))
    }

    /// Has a stream that waits to be tried again tried now. Only a waiting
    /// one hears this: an attempt under way is not followed by another.
    fn retry(&self) {
        self.slot.kick.notify_waiters();
    }
}

/// Resolves when the configuration changed in something the stream depends
/// on; false when there is no pool any more.
async fn changed(cfg: &mut watch::Receiver<Arc<PoolConfig>>, old: &PoolConfig) -> bool {
    loop {
        if cfg.changed().await.is_err() {
            return false;
        }
        let new = cfg.borrow();
        if new.status_interval != old.status_interval
            || new.wire_version != old.wire_version
            || new.backend_password != old.backend_password
            || new.connect_timeout != old.connect_timeout
        {
            return true;
        }
    }
}

async fn run(
    pool: Arc<str>,
    addr: Arc<str>,
    mut cfg: watch::Receiver<Arc<PoolConfig>>,
    slot: Arc<Slot>,
) {
    // Said once for the life of the task: a pool of replicas that have no
    // stream must not fill the log with it.
    let mut noted = false;
    loop {
        let c = cfg.borrow_and_update().clone();
        let session = async {
            if c.status_interval.is_zero() {
                return std::future::pending().await;
            }
            let mut worked = false;
            let Err(reason) = stream(&pool, &addr, &c, &slot, &mut worked).await;
            slot.set(None);
            if worked {
                tracing::info!(%pool, replica = %addr, %reason, "status stream lost");
            } else if !noted {
                noted = true;
                tracing::info!(%pool, replica = %addr, %reason, "replica has no status stream; it is used without one");
            } else {
                tracing::debug!(%pool, replica = %addr, %reason, "still no status stream");
            }
            // A replica whose connection came up is asked ahead of time,
            // but no sooner than a lost stream is: one that greets and
            // hangs up comes up many times a second.
            let wait = if worked { RETRY_LOST } else { RETRY_ABSENT };
            let soonest = sleep(RETRY_LOST);
            tokio::select! {
                () = sleep(wait) => {}
                () = async { tokio::join!(slot.kick.notified(), soonest); } => {}
            }
        };
        tokio::select! {
            () = session => {}
            alive = changed(&mut cfg, &c) => {
                slot.set(None);
                if !alive {
                    return;
                }
            }
        }
    }
}

/// Opens the stream and keeps the slot filled from it. Returns why it ended;
/// `worked` says whether a frame arrived first.
async fn stream(
    pool: &str,
    addr: &str,
    c: &PoolConfig,
    slot: &Slot,
    worked: &mut bool,
) -> Result<Infallible, String> {
    let interval = c.status_interval;
    let mut stream = timeout(c.connect_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| "connect timed out".to_owned())?
        .map_err(|e| format!("connect: {e}"))?;
    let password = c.backend_password.as_deref();
    timeout(
        GREET_TIMEOUT,
        greeting::connect(&mut stream, c.wire_version, password),
    )
    .await
    .map_err(|_| "greeting timed out".to_owned())?
    .map_err(|e| format!("greeting: {e}"))?;
    let ms = u32::try_from(interval.as_millis()).unwrap_or(u32::MAX);
    stream
        .write_all(&wire::request(c.wire_version, ms))
        .await
        .map_err(|e| format!("send: {e}"))?;
    let mut prev: Option<Arc<Reading>> = None;
    loop {
        // A server that does not know the request may say nothing at all,
        // so the first frame is waited for no longer than any other.
        let frame = timeout(interval * QUIET, read(&mut stream, c.wire_version))
            .await
            .map_err(|_| "no status frame in time".to_owned())??;
        let reading = Arc::new(Reading::after(
            prev.as_deref(),
            frame,
            Instant::now(),
            interval,
        ));
        slot.set(Some(reading.clone()));
        prev = Some(reading);
        if !*worked {
            *worked = true;
            slot.ever_up.store(true, Relaxed);
            tracing::info!(%pool, replica = %addr, "status stream up");
        }
    }
}

async fn read(stream: &mut TcpStream, version: u8) -> Result<Frame, String> {
    let io = |e: std::io::Error| match e.kind() {
        std::io::ErrorKind::UnexpectedEof => "closed by the replica".to_owned(),
        _ => format!("receive: {e}"),
    };
    let mut raw = [0u8; HEADER_LEN];
    stream.read_exact(&mut raw).await.map_err(io)?;
    let header = Header::parse(&raw, version, FRAME_MAX).map_err(|e| e.to_string())?;
    if header.msgtype != MSG_STATUS || header.flags != 0 {
        return Err("a frame that is not a status frame".to_owned());
    }
    let mut payload = vec![0u8; header.size];
    stream.read_exact(&mut payload).await.map_err(io)?;
    wire::decode(&payload)
        .ok_or_else(|| "a status frame of a layout this version does not read".to_owned())
}

/// A replica's load as a snapshot of the pool shows it.
pub struct Load {
    /// `up`; `none` for a replica that has never sent a frame; `lost` for
    /// one that has and is silent now; `off` when the pool asks for none.
    pub stream: &'static str,
    pub reading: Option<Arc<Reading>>,
    /// It is running something the bouncer did not send.
    pub not_ours: bool,
}

struct Entry {
    monitor: Monitor,
    /// What the last look at the pool's replicas found, and what this look
    /// has found so far.
    connected: bool,
    ended: u64,
    seen: bool,
    seen_connected: bool,
    seen_ended: u64,
    /// When the last request of ours on this replica ended.
    ours_ended: Option<Instant>,
    /// When the request of ours that is running there was handed over.
    started: Option<Instant>,
    /// The time requests of ours ran there since `own_since`, which is when
    /// the frame in force arrived.
    own: Duration,
    own_since: Option<Instant>,
}

impl Entry {
    /// A newer frame has seen what was counted before it.
    fn settle(&mut self, frame_at: Option<Instant>) {
        if frame_at > self.own_since {
            self.own = Duration::ZERO;
            self.own_since = frame_at;
        }
    }
}

/// The status streams of a pool, one per replica address, with what the
/// pool itself knows of each address and the streams cannot.
#[derive(Default)]
pub struct Board {
    entries: HashMap<Arc<str>, Entry>,
}

impl Board {
    /// Looks at the pool's replica connections: their address, whether
    /// they are connected, and how many requests have ended on them. Starts
    /// a stream for an address seen first, ends the stream of one no longer
    /// there, has a stream tried again when the replica's connection came
    /// up, and notes when a request of ours ended.
    pub fn observe<'a>(
        &mut self,
        replicas: impl Iterator<Item = (&'a Arc<str>, bool, u64)>,
        monitor: impl Fn(&Arc<str>) -> Monitor,
    ) {
        for (addr, connected, ended) in replicas {
            if !self.entries.contains_key(addr) {
                let entry = Entry {
                    monitor: monitor(addr),
                    connected: false,
                    ended: 0,
                    seen: false,
                    seen_connected: false,
                    seen_ended: 0,
                    ours_ended: None,
                    started: None,
                    own: Duration::ZERO,
                    own_since: None,
                };
                self.entries.insert(addr.clone(), entry);
            }
            if let Some(e) = self.entries.get_mut(addr) {
                e.seen = true;
                e.seen_connected |= connected;
                e.seen_ended += ended;
            }
        }
        let now = Instant::now();
        self.entries.retain(|_, e| {
            if !std::mem::take(&mut e.seen) {
                return false;
            }
            let connected = std::mem::take(&mut e.seen_connected);
            if connected && !e.connected {
                e.monitor.retry();
            }
            e.connected = connected;
            let ended = std::mem::take(&mut e.seen_ended);
            if ended != e.ended {
                e.ended = ended;
                e.ours_ended = Some(now);
                if let Some(started) = e.started.take() {
                    e.settle(e.monitor.reading().map(|r| r.at));
                    let from = e.own_since.map_or(started, |since| since.max(started));
                    e.own += now.saturating_duration_since(from);
                }
            }
            true
        });
    }

    /// A request of ours was handed to this address just now.
    pub fn started(&mut self, addr: &str) {
        if let Some(e) = self.entries.get_mut(addr) {
            e.started = Some(Instant::now());
        }
    }

    /// This address's load, if its stream says enough for one.
    pub fn load(&mut self, addr: &str) -> Option<f64> {
        let e = self.entries.get_mut(addr)?;
        let reading = e.monitor.reading();
        e.settle(reading.as_ref().map(|r| r.at));
        load(reading.as_deref(), e.own)
    }

    /// Ranks a free connection of this address, on which `ours` requests of
    /// the bouncer are running over its other connections. `unknown` is the
    /// load it is given if it reports none.
    pub fn rank(&mut self, addr: &str, ours: usize, unknown: f64) -> Rank {
        let Some(e) = self.entries.get_mut(addr) else {
            return rank(None, ours, None, Duration::ZERO, unknown);
        };
        let reading = e.monitor.reading();
        e.settle(reading.as_ref().map(|r| r.at));
        rank(reading.as_deref(), ours, e.ours_ended, e.own, unknown)
    }

    pub fn view(&self, addr: &str, ours: usize, off: bool) -> Load {
        let entry = self.entries.get(addr);
        let reading = entry.and_then(|e| e.monitor.reading());
        let ever_up = entry.is_some_and(|e| e.monitor.slot.ever_up.load(Relaxed));
        Load {
            stream: match (&reading, off, ever_up) {
                (Some(_), _, _) => "up",
                (None, true, _) => "off",
                (None, false, true) => "lost",
                (None, false, false) => "none",
            },
            not_ours: reading
                .as_ref()
                .is_some_and(|r| r.not_ours(ours, entry.and_then(|e| e.ours_ended))),
            reading,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: u64 = 1_000_000_000;

    fn frame(state: State, busy_ns: u64, total_ns: u64) -> Frame {
        Frame {
            state,
            pid: 7,
            now: (1000 * SEC + total_ns) as i64,
            busy_since: 0,
            loop_busy_ns: busy_ns,
            loop_total_ns: total_ns,
            cpu_user_ns: 2 * busy_ns,
            cpu_sys_ns: busy_ns,
            rss_bytes: 500,
            heap_bytes: 400,
            mem_limit_bytes: 1000,
            conns: 1,
            workers: 4,
            cores: 8,
            query: String::new(),
        }
    }

    /// A reading whose loop worked `util` of the last second.
    fn reading(state: State, util: f64, at: Instant) -> Reading {
        let interval = Duration::from_secs(1);
        let first = Reading::after(None, frame(state, 0, 0), at, interval);
        let busy = (util * SEC as f64) as u64;
        Reading::after(Some(&first), frame(state, busy, SEC), at, interval)
    }

    #[test]
    fn rates_come_from_two_frames_of_one_process() {
        let now = Instant::now();
        let first = Reading::after(None, frame(State::Idle, 0, 0), now, Duration::from_secs(1));
        assert_eq!((first.util, first.cpu), (None, None));
        assert_eq!(first.mem_share(), Some(0.5));
        let second = reading(State::Idle, 0.25, now);
        assert_eq!(second.util, Some(0.25));
        // User and system time together, over the server's own clock.
        assert_eq!(second.cpu, Some(0.75));

        // A restarted replica counts from zero: nothing is derived across it.
        let mut restarted = frame(State::Idle, SEC / 2, 2 * SEC);
        restarted.pid = 8;
        let r = Reading::after(Some(&second), restarted, now, Duration::from_secs(1));
        assert_eq!((r.util, r.cpu), (None, None));
        // Counters that ran backwards, or did not move, give no rate either.
        let back = Reading::after(
            Some(&second),
            frame(State::Idle, 0, 0),
            now,
            Duration::from_secs(1),
        );
        assert_eq!((back.util, back.cpu), (None, None));
        let mut wild = frame(State::Idle, 0, 0);
        (wild.loop_busy_ns, wild.loop_total_ns) = (u64::MAX, u64::MAX);
        (wild.cpu_user_ns, wild.cpu_sys_ns) = (u64::MAX, u64::MAX);
        wild.now = i64::MIN;
        wild.mem_limit_bytes = 0;
        let wild = Reading::after(Some(&second), wild, now, Duration::from_secs(1));
        assert_eq!((wild.cpu, wild.mem_share()), (None, None));
        assert_eq!(wild.util, Some(1.0));
    }

    #[test]
    fn a_reading_is_not_used_past_a_few_intervals() {
        let now = Instant::now();
        let r = reading(State::Idle, 0.0, now);
        assert!(r.fresh(now + Duration::from_secs(3)));
        assert!(!r.fresh(now + Duration::from_millis(3001)));
    }

    #[test]
    fn busy_counts_as_not_ours_only_when_it_cannot_be_ours() {
        let now = Instant::now();
        let ago = |ms| now - Duration::from_millis(ms);
        let busy = reading(State::Other, 0.5, now);
        assert!(busy.not_ours(0, None));
        assert!(!reading(State::Idle, 0.5, now).not_ours(0, None));
        // A request of ours is on it now: what it runs may be that.
        assert!(!busy.not_ours(1, None));
        // Our request ended after the frame arrived: the frame saw ours.
        assert!(!reading(State::Request, 0.5, ago(200)).not_ours(0, Some(ago(100))));
        assert!(reading(State::Request, 0.5, ago(100)).not_ours(0, Some(ago(200))));
        // The frame arrived after our request ended, but what it describes
        // began before that: it is our request, seen late.
        let mut late = reading(State::Request, 0.5, ago(100));
        late.frame.busy_since = late.frame.now - 300_000_000;
        assert_eq!(late.running_for(), Some(Duration::from_millis(300)));
        assert!(!late.not_ours(0, Some(ago(200))));
        late.frame.busy_since = late.frame.now - 50_000_000;
        assert!(late.not_ours(0, Some(ago(200))));
    }

    #[test]
    fn the_better_choice_ranks_lower() {
        let now = Instant::now();
        let none = Duration::ZERO;
        let idle = |util| reading(State::Idle, util, now);
        let of = |r: &Reading| rank(Some(r), 0, None, none, 0.0);
        assert!(of(&idle(0.1)) < of(&idle(0.6)));
        // Busy with someone else's work loses to anything, however loaded
        // and however little is known.
        assert!(of(&idle(0.99)) < of(&reading(State::Other, 0.0, now)));
        assert!(rank(None, 0, None, none, 0.99) < of(&reading(State::Request, 0.0, now)));
        // A replica that says nothing of its load carries what it is given,
        // the average of the others: it comes after the lighter of them and
        // before the heavier. One whose first frame only just came says
        // nothing of its load yet.
        let silent = rank(None, 0, None, none, 0.5);
        assert!(of(&idle(0.2)) < silent && silent < of(&idle(0.8)));
        let first = Reading::after(None, frame(State::Idle, 0, 0), now, Duration::from_secs(1));
        assert_eq!(load(Some(&first), none), None);
        assert_eq!(rank(Some(&first), 0, None, none, 0.5), silent);
        // A request of ours on another of its connections outweighs load.
        assert!(of(&idle(0.9)) < rank(Some(&idle(0.0)), 1, None, none, 0.0));
    }

    #[test]
    fn what_the_bouncer_ran_since_the_frame_counts_as_load() {
        let now = Instant::now();
        let (a, b) = (
            reading(State::Idle, 0.1, now),
            reading(State::Idle, 0.3, now),
        );
        let of = |r: &Reading, own_ms| rank(Some(r), 0, None, Duration::from_millis(own_ms), 0.0);
        assert!(of(&a, 0) < of(&b, 0));
        // 300 ms of our requests in a one-second interval is 0.3 more.
        assert!(of(&b, 0) < of(&a, 300));
    }

    #[test]
    fn memory_weighs_only_near_the_limit() {
        let now = Instant::now();
        let with = |util, rss| {
            let mut r = reading(State::Idle, util, now);
            r.frame.rss_bytes = rss;
            rank(Some(&r), 0, None, Duration::ZERO, 0.0)
        };
        // Under the mark the fuller replica is as good as the emptier one.
        assert_eq!(with(0.2, 100).load, with(0.2, 800).load);
        assert!(with(0.2, 100) < with(0.1, 950));
        // At the limit it weighs as much as a loop that never rests.
        assert!((with(0.0, 1000).load - 1.0).abs() < 1e-9);
        // Without a limit there is no share and no penalty.
        let mut free = reading(State::Idle, 0.2, now);
        free.frame.mem_limit_bytes = 0;
        assert_eq!(rank(Some(&free), 0, None, Duration::ZERO, 0.0).load, 0.2);
    }
}
