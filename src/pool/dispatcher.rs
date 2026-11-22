//! A pool's one task: it owns the queue and knows which replica is free.
//! Everything else talks to it through its channel, so nothing here is
//! shared and nothing needs a lock.

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, watch};

use super::backend::{self, Msg};
use super::{
    BackendInfo, Cmd, Counters, Ending, Job, PoolHandle, QueuedInfo, Shared, Snapshot, request_text,
};
use crate::config::{Mode, PoolConfig};
use crate::stats::Stats;
use crate::stats::event::Outcome;

/// How often the head of the queue is checked for having waited too long.
const TICK: Duration = Duration::from_millis(100);
/// Clients remembered for `affinity` before the memory is started over.
const AFFINITY_MAX: usize = 100_000;

enum State {
    Down,
    Idle,
    Busy {
        id: u64,
        client: SocketAddr,
        since: Instant,
        frame: Bytes,
    },
}

struct Replica {
    addr: Arc<str>,
    tx: mpsc::UnboundedSender<Msg>,
    state: State,
    draining: bool,
    /// Left the configuration: stopped as soon as it is not busy.
    removed: bool,
    gone: bool,
    last_error: String,
    served: u64,
    failures: u64,
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
}

pub fn spawn(cfg: Arc<PoolConfig>, stats: Arc<Stats>) -> PoolHandle {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let shared = Arc::new(Shared {
        name: cfg.name.as_str().into(),
        counters: Counters::default(),
        stats,
        queue_max: (cfg.queue_max as u64).into(),
    });
    let (cfg_tx, _) = watch::channel(cfg.clone());
    let mut d = Dispatcher {
        cfg: cfg.clone(),
        cfg_tx,
        events: tx.clone(),
        shared: shared.clone(),
        replicas: Vec::new(),
        queue: VecDeque::new(),
        cursor: 0,
        paused: false,
        closing: false,
        affinity: HashMap::new(),
    };
    for addr in &cfg.backends {
        d.add(addr, cfg.slots);
    }
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(TICK);
        loop {
            tokio::select! {
                cmd = rx.recv() => match cmd {
                    Some(cmd) => d.handle(cmd).await,
                    None => break,
                },
                _ = tick.tick() => d.expire().await,
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
            let ctx = backend::Ctx {
                idx: self.replicas.len(),
                addr: addr.clone(),
                cfg: self.cfg_tx.subscribe(),
                events: self.events.clone(),
                shared: self.shared.clone(),
            };
            tokio::spawn(backend::run(ctx, rx));
            self.replicas.push(Replica {
                addr: addr.clone(),
                tx,
                state: State::Down,
                draining: false,
                removed: false,
                gone: false,
                last_error: "connecting".into(),
                served: 0,
                failures: 0,
            });
        }
    }

    async fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Submit(job) => self.submit(job).await,
            Cmd::Cancel(id) => self.cancel(id).await,
            Cmd::Up(idx) => {
                let r = &mut self.replicas[idx];
                r.state = State::Idle;
                r.last_error.clear();
                tracing::info!(pool = %self.shared.name, replica = %r.addr, "replica up");
            }
            Cmd::Down(idx, reason) => {
                let r = &mut self.replicas[idx];
                if !matches!(r.state, State::Down) || r.last_error != reason {
                    tracing::warn!(pool = %self.shared.name, replica = %r.addr, %reason, "replica down");
                }
                r.state = State::Down;
                r.last_error = reason;
            }
            Cmd::Finished(idx) => {
                let r = &mut self.replicas[idx];
                r.state = State::Idle;
                r.served += 1;
            }
            Cmd::Failed {
                idx,
                job,
                sent,
                reason,
            } => self.failed(idx, job, sent, reason).await,
            Cmd::Gone(idx) => self.replicas[idx].gone = true,
            Cmd::Snapshot(reply) => {
                let _ = reply.send(self.snapshot());
            }
            Cmd::Pause(on) => self.paused = on,
            Cmd::Drain(addr, on, reply) => {
                let mut found = false;
                for r in self
                    .replicas
                    .iter_mut()
                    .filter(|r| *r.addr == *addr && !r.removed)
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
        self.retire();
        self.pump().await;
    }

    async fn submit(&mut self, job: Job) {
        if self.closing {
            job.refuse("io", "pool removed from the configuration")
                .await;
            self.end(&job, Outcome::Failed);
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
            if matches!(r.state, State::Busy { id: running, .. } if running == id) {
                let _ = r.tx.send(Msg::Cancel(id));
            }
        }
    }

    async fn failed(&mut self, idx: usize, mut job: Job, sent: bool, reason: String) {
        let r = &mut self.replicas[idx];
        r.state = State::Down;
        if sent {
            r.failures += 1;
            r.last_error.clone_from(&reason);
            tracing::warn!(pool = %self.shared.name, replica = %r.addr, %reason, "request failed");
        }
        // A request that never left may always go again; one that did is run
        // a second time only once, and only where that is known to be safe.
        if !sent || (self.cfg.retry && job.retries == 0) {
            job.retries += u8::from(sent);
            self.queue.push_front(job);
        } else {
            job.refuse("io", &format!("replica failed: {reason}")).await;
            self.end(&job, Outcome::Failed);
        }
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

    /// Turns away whoever has waited at the head of the queue too long.
    async fn expire(&mut self) {
        while self
            .queue
            .front()
            .is_some_and(|j| j.arrived.elapsed() > self.cfg.query_wait_timeout)
        {
            if let Some(job) = self.queue.pop_front() {
                job.refuse("busy", "no replica became free in time").await;
                self.end(&job, Outcome::Busy);
            }
        }
        self.pump().await;
    }

    /// Hands queued requests to replicas, as the pool's mode says.
    async fn pump(&mut self) {
        if self.paused {
            return;
        }
        // A client that left while queued has nobody to answer.
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
        match self.cfg.mode {
            Mode::Free => self.pump_free(),
            Mode::RoundRobin => self.pump_in_turn(),
        }
    }

    /// The head of the queue goes to a free replica, until one of the two
    /// runs out.
    fn pump_free(&mut self) {
        while let Some(client) = self.queue.front().map(|j| j.client) {
            let Some(idx) = self.pick(client.ip()) else {
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
        let usable =
            |r: &Replica| !matches!(r.state, State::Down) && !r.draining && !r.removed && !r.gone;
        for at in 0..self.queue.len() {
            if self.queue[at]
                .target
                .is_some_and(|t| usable(&self.replicas[t]))
            {
                continue;
            }
            let next = (0..n)
                .map(|k| (self.cursor + k) % n)
                .find(|i| usable(&self.replicas[*i]));
            self.queue[at].target = next;
            if let Some(idx) = next {
                self.cursor = (idx + 1) % n;
            }
        }
        for idx in 0..n {
            if !matches!(self.replicas[idx].state, State::Idle) || !usable(&self.replicas[idx]) {
                continue;
            }
            let waiting = self.queue.iter().position(|j| j.target == Some(idx));
            if let Some(job) = waiting.and_then(|at| self.queue.remove(at)) {
                self.start(idx, job);
            }
        }
    }

    fn start(&mut self, idx: usize, job: Job) {
        let r = &mut self.replicas[idx];
        r.state = State::Busy {
            id: job.id,
            client: job.client,
            since: Instant::now(),
            frame: job.frame.clone(),
        };
        if let Err(mpsc::error::SendError(Msg::Run(job))) = r.tx.send(Msg::Run(job)) {
            r.state = State::Down;
            r.gone = true;
            self.queue.push_front(job);
        }
    }

    /// A free replica: the client's last one under `affinity`, else the next
    /// in turn.
    fn pick(&mut self, client: IpAddr) -> Option<usize> {
        let free =
            |r: &Replica| matches!(r.state, State::Idle) && !r.draining && !r.removed && !r.gone;
        let n = self.replicas.len();
        let sticky = self
            .cfg
            .affinity
            .then(|| self.affinity.get(&client).copied())
            .flatten();
        let idx = sticky
            .filter(|i| self.replicas.get(*i).is_some_and(free))
            .or_else(|| {
                (0..n)
                    .map(|k| (self.cursor + k) % n)
                    .find(|i| free(&self.replicas[*i]))
            })?;
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
            if r.removed && !r.gone && !matches!(r.state, State::Busy { .. }) {
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
                .filter(|r| *r.addr == **addr && !r.removed)
            {
                have += 1;
                r.removed = have > cfg.slots;
            }
            if have < cfg.slots {
                self.add(addr, cfg.slots - have);
            }
        }
        self.shared
            .queue_max
            .store(cfg.queue_max as u64, std::sync::atomic::Ordering::Relaxed);
        self.cfg = cfg.clone();
        self.cfg_tx.send_replace(cfg);
        if reconnect {
            for r in self.replicas.iter().filter(|r| !r.gone && !r.removed) {
                let _ = r.tx.send(Msg::Reconnect);
            }
        }
    }

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
                    text: j.text(),
                })
                .collect(),
            backends: self
                .replicas
                .iter()
                .filter(|r| !r.gone)
                .map(|r| BackendInfo {
                    addr: r.addr.clone(),
                    state: match r.state {
                        State::Down => "down",
                        State::Idle => "idle",
                        State::Busy { .. } => "busy",
                    },
                    draining: r.draining || r.removed,
                    last_error: r.last_error.clone(),
                    served: r.served,
                    failures: r.failures,
                    running: match &r.state {
                        State::Busy {
                            client,
                            since,
                            frame,
                            ..
                        } => Some((*client, since.elapsed(), request_text(frame))),
                        _ => None,
                    },
                })
                .collect(),
        }
    }
}
