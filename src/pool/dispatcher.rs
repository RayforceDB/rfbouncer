//! A pool's one task: it owns the queue and knows which replica is free.
//! Everything else talks to it through its channel, so nothing here is
//! shared and nothing needs a lock.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, watch};

use super::backend::{self, Msg};
use super::status;
use super::{
    BackendInfo, Cmd, Counters, Ending, Job, PoolHandle, QueuedInfo, Reply, Shared, Snapshot,
};
use crate::config::{Mode, PoolConfig};
use crate::stats::Stats;
use crate::stats::event::Outcome;

/// How often the queue is checked for requests that have waited too long.
const TICK: Duration = Duration::from_millis(100);
/// Clients remembered for `affinity` before the memory is started over.
const AFFINITY_MAX: usize = 100_000;

/// The request a replica is running.
struct Running {
    id: u64,
    client: SocketAddr,
    since: Instant,
    frame: Bytes,
}

/// A replica as the pool knows it. Whether its connection is open and
/// whether it runs a request are told by different messages, which may
/// arrive in any order, so they are kept apart: a connection that drops and
/// comes back never makes a running replica look free.
struct Replica {
    addr: Arc<str>,
    tx: mpsc::UnboundedSender<Msg>,
    connected: bool,
    running: Option<Running>,
    draining: bool,
    /// Left the configuration: stopped as soon as it is not busy.
    removed: bool,
    gone: bool,
    last_error: String,
    served: u64,
    failures: u64,
}

impl Replica {
    /// May be given requests at all.
    fn usable(&self) -> bool {
        self.connected && !self.draining && !self.removed && !self.gone
    }

    fn free(&self) -> bool {
        self.usable() && self.running.is_none()
    }
}

struct Dispatcher {
    cfg: Arc<PoolConfig>,
    cfg_tx: watch::Sender<Arc<PoolConfig>>,
    events: mpsc::UnboundedSender<Cmd>,
    shared: Arc<Shared>,
    replicas: Vec<Replica>,
    queue: VecDeque<Job>,
    cursor: usize,
    paused: bool,
    closing: bool,
    affinity: HashMap<IpAddr, usize>,
    /// Running requests whose client asked for a cancel. One that its
    /// replica hands back is answered `cancel`, not run again.
    cancelled: HashSet<u64>,
    /// The replicas' status streams, one per address.
    load: status::Board,
}

pub fn spawn(cfg: Arc<PoolConfig>, stats: Arc<Stats>) -> PoolHandle {
    let (tx, mut rx) = mpsc::unbounded_channel();
    // What the replicas report comes on a channel of its own and is read
    // first. On the clients' channel a replica that has just become free
    // would wait behind every request sent meanwhile, and when clients that
    // were turned away ask again at once, that is thousands: the replicas
    // then idle while the pool is busy refusing.
    let (events, mut reports) = mpsc::unbounded_channel();
    let shared = Arc::new(Shared {
        name: cfg.name.as_str().into(),
        counters: Counters::default(),
        stats,
        queue_max: (cfg.queue_max as u64).into(),
        cache: super::cache::Cache::default(),
    });
    let (cfg_tx, _) = watch::channel(cfg.clone());
    let mut d = Dispatcher {
        cfg: cfg.clone(),
        cfg_tx,
        events,
        shared: shared.clone(),
        replicas: Vec::new(),
        queue: VecDeque::new(),
        cursor: 0,
        paused: false,
        closing: false,
        affinity: HashMap::new(),
        cancelled: HashSet::new(),
        load: status::Board::default(),
    };
    for addr in &cfg.backends {
        d.add(addr, cfg.slots);
    }
    d.note_status();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(TICK);
        loop {
            tokio::select! {
                biased;
                Some(report) = reports.recv() => d.handle(report).await,
                // Ahead of the clients, or a steady stream of requests would
                // keep the queue from ever being looked over.
                _ = tick.tick() => d.expire().await,
                cmd = rx.recv() => match cmd {
                    Some(cmd) => d.handle(cmd).await,
                    None => break,
                },
            }
            if d.closing && d.queue.is_empty() && d.replicas.iter().all(|r| r.gone) {
                break;
            }
        }
    });
    PoolHandle { shared, tx }
}

impl Dispatcher {
    fn add(&mut self, addr: &str, slots: usize) {
        let addr: Arc<str> = addr.into();
        for _ in 0..slots {
            let (tx, rx) = mpsc::unbounded_channel();
            // The place of a replica that is gone is taken again: its task
            // said so as the last thing it did, and will say nothing more.
            let idx = self
                .replicas
                .iter()
                .position(|r| r.gone)
                .unwrap_or(self.replicas.len());
            let ctx = backend::Ctx {
                idx,
                addr: addr.clone(),
                cfg: self.cfg_tx.subscribe(),
                events: self.events.clone(),
                shared: self.shared.clone(),
            };
            tokio::spawn(backend::run(ctx, rx));
            let replica = Replica {
                addr: addr.clone(),
                tx,
                connected: false,
                running: None,
                draining: false,
                removed: false,
                gone: false,
                last_error: "connecting".into(),
                served: 0,
                failures: 0,
            };
            if idx == self.replicas.len() {
                self.replicas.push(replica);
            } else {
                self.replicas[idx] = replica;
            }
        }
    }

    async fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Submit(job) => self.submit(job).await,
            Cmd::Cancel(id) => self.cancel(id).await,
            Cmd::CancelRunning => {
                // As a client's own cancel: the replica is told to stop and
                // its answer to that, the error `cancel`, goes to the client.
                for r in &self.replicas {
                    if let Some(run) = &r.running {
                        self.cancelled.insert(run.id);
                        let _ = r.tx.send(Msg::Cancel(run.id));
                    }
                }
            }
            Cmd::Up(idx) => {
                let r = &mut self.replicas[idx];
                r.connected = true;
                r.last_error.clear();
                tracing::info!(pool = %self.shared.name, replica = %r.addr, "replica up");
            }
            Cmd::Down(idx, reason) => {
                let r = &mut self.replicas[idx];
                if r.connected || r.last_error != reason {
                    tracing::warn!(pool = %self.shared.name, replica = %r.addr, %reason, "replica down");
                }
                r.connected = false;
                r.last_error = reason;
            }
            Cmd::Finished(idx) => {
                let r = &mut self.replicas[idx];
                if let Some(run) = r.running.take() {
                    self.cancelled.remove(&run.id);
                    r.served += 1;
                }
            }
            Cmd::Failed {
                idx,
                job,
                sent,
                reason,
            } => self.failed(idx, job, sent, reason).await,
            Cmd::Gone(idx) => {
                let r = &mut self.replicas[idx];
                r.gone = true;
                r.connected = false;
                r.running = None;
            }
            Cmd::Snapshot(reply) => {
                let _ = reply.send(self.snapshot());
            }
            Cmd::Pause(on) => {
                // What waited through a pause has not waited for a replica.
                if self.paused && !on {
                    let now = Instant::now();
                    self.queue.iter_mut().for_each(|j| j.queued_at = now);
                }
                self.paused = on;
            }
            Cmd::Drain(addr, on, reply) => {
                let mut found = false;
                for r in self
                    .replicas
                    .iter_mut()
                    .filter(|r| *r.addr == *addr && !r.removed && !r.gone)
                {
                    r.draining = on;
                    found = true;
                }
                let _ = reply.send(found);
            }
            Cmd::Reconnect => {
                for r in self.replicas.iter().filter(|r| !r.gone) {
                    let _ = r.tx.send(Msg::Reconnect);
                }
            }
            Cmd::Reconfigure(cfg) => self.reconfigure(cfg),
            Cmd::Close => {
                self.closing = true;
                for r in &mut self.replicas {
                    r.removed = true;
                }
                while let Some(job) = self.queue.pop_front() {
                    job.refuse("io", "pool removed from the configuration")
                        .await;
                    self.end(&job, Outcome::Failed);
                }
            }
        }
        self.note_status();
        self.retire();
        self.pump();
    }

    async fn submit(&mut self, job: Job) {
        if self.closing {
            job.refuse("io", "pool removed from the configuration")
                .await;
            self.end(&job, Outcome::Failed);
        } else if !self.paused
            && let Some(answer) = self.cached(&job)
        {
            // Before the queue is looked at: an answer in memory needs no
            // place in it. A paused pool holds every request, this one too.
            self.answer_cached(job, answer);
        } else if self.queue.len() >= self.cfg.queue_max {
            job.refuse("busy", "the queue is full").await;
            self.end(&job, Outcome::Busy);
        } else {
            self.queue.push_back(job);
        }
    }

    async fn cancel(&mut self, id: u64) {
        if let Some(at) = self.queue.iter().position(|j| j.id == id) {
            if let Some(job) = self.queue.remove(at) {
                job.refuse("cancel", "interrupted").await;
                self.end(&job, Outcome::Cancelled);
            }
            return;
        }
        for r in &self.replicas {
            if r.running.as_ref().is_some_and(|run| run.id == id) {
                self.cancelled.insert(id);
                let _ = r.tx.send(Msg::Cancel(id));
            }
        }
    }

    /// A replica gave a request back.
    async fn failed(&mut self, idx: usize, mut job: Job, sent: bool, reason: String) {
        let r = &mut self.replicas[idx];
        if r.running.as_ref().is_some_and(|run| run.id == job.id) {
            r.running = None;
        }
        if sent {
            r.failures += 1;
            r.last_error.clone_from(&reason);
            tracing::warn!(pool = %self.shared.name, replica = %r.addr, %reason, "request failed");
        }
        if self.cancelled.remove(&job.id) {
            // Its client asked for it to stop while it was on its way back.
            job.refuse("cancel", "interrupted").await;
            self.end(&job, Outcome::Cancelled);
        } else if !sent || (self.cfg.retry && job.retries == 0) {
            // A request that never left may always go again; one that did
            // is run a second time only once, and only where that is known
            // to be safe.
            job.retries += u8::from(sent);
            job.queued_at = Instant::now();
            job.avoid = sent.then_some(idx);
            job.target = None;
            self.queue.push_front(job);
        } else {
            job.refuse("io", &format!("replica failed: {reason}")).await;
            self.end(&job, Outcome::Failed);
        }
    }

    /// The answer the cache holds for this request, if the pool has a cache.
    /// A request too large to be kept is not looked for, which also spares
    /// this task the hashing of it.
    fn cached(&self, job: &Job) -> Option<Bytes> {
        let cfg = &self.cfg;
        if cfg.cache_ttl.is_zero() || job.frame.len() >= cfg.cache_max_entry {
            return None;
        }
        let compressed = cfg.compress.applies(job.client.ip());
        self.shared.cache.get(&job.frame, compressed, cfg.cache_ttl)
    }

    /// Sends a kept answer, on a task of its own: a client that reads
    /// slowly holds no replica now, and must not hold the pool either.
    fn answer_cached(&self, job: Job, answer: Bytes) {
        let shared = self.shared.clone();
        let wait = job.arrived.elapsed();
        tokio::spawn(async move {
            let mut rest = answer.clone();
            let mut left = false;
            while !rest.is_empty() && !left {
                let part = rest.split_to(rest.len().min(backend::CHUNK));
                left = job.reply.send(Reply::Data(part)).await.is_err();
            }
            left = left || job.reply.send(Reply::Done).await.is_err();
            let ending = Ending {
                backend: None,
                wait,
                exec: Duration::ZERO,
                bytes_out: (answer.len() - rest.len()) as u64,
                outcome: if left {
                    Outcome::Cancelled
                } else {
                    Outcome::Cached
                },
            };
            shared.report(&job, ending);
        });
    }

    /// Reports a request that ended in the queue, where no replica saw it.
    fn end(&self, job: &Job, outcome: Outcome) {
        let ending = Ending {
            backend: None,
            wait: job.arrived.elapsed(),
            exec: Duration::ZERO,
            bytes_out: 0,
            outcome,
        };
        self.shared.report(job, ending);
    }

    /// Turns away whoever has waited for a replica too long. A paused pool
    /// holds its requests for as long as it is paused.
    async fn expire(&mut self) {
        let limit = self.cfg.query_wait_timeout;
        let mut at = 0;
        while !self.paused && at < self.queue.len() {
            if self.queue[at].queued_at.elapsed() > limit {
                if let Some(job) = self.queue.remove(at) {
                    job.refuse("busy", "no replica became free in time").await;
                    self.end(&job, Outcome::Busy);
                }
            } else {
                at += 1;
            }
        }
        if !self.paused {
            self.forget_gone();
        }
        self.pump();
    }

    /// Hands queued requests to replicas, as the pool's mode says.
    fn pump(&mut self) {
        if self.paused {
            return;
        }
        match self.cfg.mode {
            Mode::Free => self.pump_free(),
            Mode::RoundRobin => {
                self.forget_gone();
                self.pump_in_turn();
            }
            Mode::LeastLoaded => self.pump_least_loaded(),
        }
    }

    /// Takes out of the queue the requests whose client left while they
    /// waited: there is nobody to answer. This reads the whole queue, which
    /// is why it is not done for every command: `pump_free` looks only at
    /// the request it is about to hand out, and the tick finds the rest.
    fn forget_gone(&mut self) {
        let mut at = 0;
        while at < self.queue.len() {
            if self.queue[at].reply.is_closed() {
                if let Some(job) = self.queue.remove(at) {
                    self.end(&job, Outcome::Cancelled);
                }
            } else {
                at += 1;
            }
        }
    }

    /// The head of the queue goes to a free replica, until one of the two
    /// runs out.
    fn pump_free(&mut self) {
        while let Some((client, avoid)) = self.queue.front().map(|j| (j.client.ip(), j.avoid)) {
            // A client that left while queued has nobody to answer.
            if self.queue.front().is_some_and(|j| j.reply.is_closed()) {
                if let Some(job) = self.queue.pop_front() {
                    self.end(&job, Outcome::Cancelled);
                }
                continue;
            }
            let Some(idx) = self.pick(client, avoid) else {
                return;
            };
            if let Some(job) = self.queue.pop_front() {
                self.start(idx, job);
            }
        }
    }

    /// Every request is given the next replica in turn when it arrives, and
    /// waits for that one. A request whose replica went away is given
    /// another, so a dead replica holds nothing back.
    fn pump_in_turn(&mut self) {
        let n = self.replicas.len();
        for at in 0..self.queue.len() {
            if self.queue[at]
                .target
                .is_some_and(|t| self.replicas[t].usable())
            {
                continue;
            }
            let next = (0..n)
                .map(|k| (self.cursor + k) % n)
                .find(|i| self.replicas[*i].usable());
            self.queue[at].target = next;
            if let Some(idx) = next {
                self.cursor = (idx + 1) % n;
            }
        }
        for idx in 0..n {
            // More than once: a request answered from the cache leaves its
            // replica free for the next that waits for it.
            while self.replicas[idx].free() {
                let waiting = self.queue.iter().position(|j| j.target == Some(idx));
                let Some(job) = waiting.and_then(|at| self.queue.remove(at)) else {
                    break;
                };
                self.start(idx, job);
            }
        }
    }

    fn start(&mut self, idx: usize, job: Job) {
        // Asked again here: the same request may have been answered while
        // this one waited in the queue.
        if let Some(answer) = self.cached(&job) {
            self.answer_cached(job, answer);
            return;
        }
        if !self.cfg.cache_ttl.is_zero() {
            self.shared.cache.misses.fetch_add(1, Relaxed);
        }
        let r = &mut self.replicas[idx];
        // Here and not where the replica is chosen: a request answered from
        // the cache above ran nowhere, and is no load on the one chosen.
        self.load.started(&r.addr);
        r.running = Some(Running {
            id: job.id,
            client: job.client,
            since: Instant::now(),
            frame: job.frame.clone(),
        });
        if let Err(mpsc::error::SendError(Msg::Run(job))) = r.tx.send(Msg::Run(job)) {
            r.running = None;
            r.connected = false;
            r.gone = true;
            self.queue.push_front(job);
        }
    }

    /// A free replica: the client's last one under `affinity`, else the next
    /// in turn, and not the one that just failed under this request while
    /// another is free.
    fn pick(&mut self, client: IpAddr, avoid: Option<usize>) -> Option<usize> {
        let n = self.replicas.len();
        let free = |i: &usize| self.replicas.get(*i).is_some_and(Replica::free);
        let sticky = self
            .cfg
            .affinity
            .then(|| self.affinity.get(&client).copied())
            .flatten();
        let in_turn = || (0..n).map(|k| (self.cursor + k) % n);
        let idx = sticky
            .filter(|i| free(i) && Some(*i) != avoid)
            .or_else(|| in_turn().find(|i| free(i) && Some(*i) != avoid))
            .or_else(|| in_turn().find(free))?;
        if sticky != Some(idx) {
            self.cursor = (idx + 1) % n;
        }
        if self.cfg.affinity {
            if self.affinity.len() >= AFFINITY_MAX {
                self.affinity.clear();
            }
            self.affinity.insert(client, idx);
        }
        Some(idx)
    }

    /// Stops the replicas that left the configuration, once they are free.
    fn retire(&mut self) {
        for r in &mut self.replicas {
            if r.removed && !r.gone && r.running.is_none() {
                let _ = r.tx.send(Msg::Stop);
            }
        }
    }

    fn reconfigure(&mut self, cfg: Arc<PoolConfig>) {
        let reconnect = cfg.backend_password != self.cfg.backend_password
            || cfg.wire_version != self.cfg.wire_version
            || cfg.require_restricted != self.cfg.require_restricted;
        for r in &mut self.replicas {
            if !cfg.backends.iter().any(|a| **a == *r.addr) {
                r.removed = true;
            }
        }
        // Every address is brought to the new number of slots: the ones that
        // stay keep theirs, the surplus goes, the missing are added.
        for addr in &cfg.backends {
            let mut have = 0;
            for r in self
                .replicas
                .iter_mut()
                .filter(|r| *r.addr == **addr && !r.removed && !r.gone)
            {
                have += 1;
                r.removed = have > cfg.slots;
            }
            if have < cfg.slots {
                self.add(addr, cfg.slots - have);
            }
        }
        self.shared.queue_max.store(cfg.queue_max as u64, Relaxed);
        // The replicas or the settings changed: what was kept was answered
        // under the old ones.
        self.shared.cache.clear();
        self.cfg = cfg.clone();
        self.cfg_tx.send_replace(cfg);
        if reconnect {
            for r in self.replicas.iter().filter(|r| !r.gone && !r.removed) {
                let _ = r.tx.send(Msg::Reconnect);
            }
        }
    }

    /// The pool at this instant. Frames are handed out as they are, a
    /// reference each: nothing is decoded on this task.
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            name: self.shared.name.clone(),
            paused: self.paused,
            queue: self
                .queue
                .iter()
                .map(|j| QueuedInfo {
                    client: j.client,
                    waited: j.arrived.elapsed(),
                    frame: j.frame.clone(),
                })
                .collect(),
            backends: self
                .replicas
                .iter()
                .filter(|r| !r.gone)
                .map(|r| BackendInfo {
                    addr: r.addr.clone(),
                    connected: r.connected,
                    draining: r.draining || r.removed,
                    last_error: r.last_error.clone(),
                    served: r.served,
                    failures: r.failures,
                    running: r
                        .running
                        .as_ref()
                        .map(|run| (run.client, run.since.elapsed(), run.frame.clone())),
                    load: self.load_of(&r.addr),
                })
                .collect(),
        }
    }
}

/// The replicas' status streams and the choice that reads them.
impl Dispatcher {
    /// Shows the status board the replicas as they are after a command: it
    /// keeps a stream per address in use, and learns from here when a
    /// replica came up and when a request of ours ended.
    fn note_status(&mut self) {
        let (pool, cfg) = (&self.shared.name, &self.cfg_tx);
        self.load.observe(
            self.replicas
                .iter()
                .filter(|r| !r.gone)
                .map(|r| (&r.addr, r.connected, r.served + r.failures)),
            |addr| status::Monitor::spawn(pool.clone(), addr.clone(), cfg.subscribe()),
        );
    }

    /// Requests of ours running on an address, over all its connections.
    fn ours(&self, addr: &str) -> usize {
        let on = |r: &&Replica| *r.addr == *addr && !r.gone && r.running.is_some();
        self.replicas.iter().filter(on).count()
    }

    fn load_of(&self, addr: &str) -> status::Load {
        let off = self.cfg.status_interval.is_zero();
        self.load.view(addr, self.ours(addr), off)
    }

    /// As `pump_free`: the head of the queue goes to a free replica until
    /// one of the two runs out. Only the choice among the free differs, so
    /// no request waits for a replica to look better.
    fn pump_least_loaded(&mut self) {
        while let Some(avoid) = self.queue.front().map(|j| j.avoid) {
            // As in `pump_free`: a client that left has nobody to answer.
            if self.queue.front().is_some_and(|j| j.reply.is_closed()) {
                if let Some(job) = self.queue.pop_front() {
                    self.end(&job, Outcome::Cancelled);
                }
                continue;
            }
            let Some(idx) = self.pick_least_loaded(avoid) else {
                return;
            };
            if let Some(job) = self.queue.pop_front() {
                self.start(idx, job);
            }
        }
    }

    /// The free replica that ranks best, and not the one that just failed
    /// under this request while another is free. They are looked at in
    /// turn and only a strictly better one replaces the first, so replicas
    /// that rank alike, as those without a status stream do, are taken in
    /// turn. Only a free replica is looked at, and one whose connection is
    /// down, by the health check's word or any other, is not free: a stream
    /// that still calls it idle changes nothing about that.
    fn pick_least_loaded(&mut self, avoid: Option<usize>) -> Option<usize> {
        let n = self.replicas.len();
        // A free replica that reports no load is ranked at the average of
        // the free ones that do.
        let mut known = (0.0, 0u32);
        for i in 0..n {
            if self.replicas[i].free()
                && let Some(load) = self.load.load(&self.replicas[i].addr)
            {
                known = (known.0 + load, known.1 + 1);
            }
        }
        let unknown = if known.1 == 0 {
            0.0
        } else {
            known.0 / f64::from(known.1)
        };
        let mut best = None;
        for i in (0..n).map(|k| (self.cursor + k) % n) {
            if !self.replicas[i].free() {
                continue;
            }
            let addr = &self.replicas[i].addr;
            let key = (
                Some(i) == avoid,
                self.load.rank(addr, self.ours(addr), unknown),
            );
            if best.as_ref().is_none_or(|(least, _)| key < *least) {
                best = Some((key, i));
            }
        }
        let (_, idx) = best?;
        self.cursor = (idx + 1) % n;
        Some(idx)
    }
}
