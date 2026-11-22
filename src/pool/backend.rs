//! One connection to one replica: open it, keep it, run one request at a
//! time over it, and open it again when anything about it looks wrong.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep, sleep_until, timeout};

use super::{Cmd, Ending, Job, Reply, Shared};
use crate::config::PoolConfig;
use crate::stats::event::Outcome;
use crate::wire::frame::{self, HEADER_LEN, Header, MSG_RESPONSE, MSG_SYNC, PAYLOAD_MAX};
use crate::wire::{greeting, oob, value};

/// A replica answers the greeting from its evaluating thread, so this is how
/// long a replica busy with someone else's query may keep a new connection.
const GREET_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a replica may go silent in the middle of an answer.
const TRANSFER_STALL: Duration = Duration::from_secs(60);
const RETRY_MIN: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(5);
const CHUNK: usize = 64 * 1024;
/// An answer nobody waits for is read off the wire up to this size; past it
/// a new connection is cheaper.
const DRAIN_MAX: usize = 1024 * 1024;
/// Restricted and without effect: a replica started with -U refuses it, any
/// other answers it, and neither changes anything.
const PROBE: &str = "(.os.getenv \"RFBOUNCER_PROBE\")";

pub enum Msg {
    Run(Job),
    Cancel(u64),
    Reconnect,
    Stop,
}

pub struct Ctx {
    pub idx: usize,
    pub addr: Arc<str>,
    pub cfg: watch::Receiver<Arc<PoolConfig>>,
    pub events: mpsc::UnboundedSender<Cmd>,
    pub shared: Arc<Shared>,
}

enum Next {
    Reconnect(String),
    Stop,
}

pub async fn run(ctx: Ctx, mut rx: mpsc::UnboundedReceiver<Msg>) {
    loop {
        let Some(mut stream) = connect(&ctx, &mut rx).await else {
            break;
        };
        let _ = ctx.events.send(Cmd::Up(ctx.idx));
        match serve(&ctx, &mut stream, &mut rx).await {
            Next::Reconnect(reason) => {
                let _ = ctx.events.send(Cmd::Down(ctx.idx, reason));
            }
            Next::Stop => break,
        }
    }
    let _ = ctx.events.send(Cmd::Gone(ctx.idx));
}

/// Tries until a connection is open, backing off between attempts. `None`
/// when told to stop meanwhile.
async fn connect(ctx: &Ctx, rx: &mut mpsc::UnboundedReceiver<Msg>) -> Option<TcpStream> {
    let mut delay = Duration::ZERO;
    loop {
        let pause = delay;
        let attempt = async move {
            sleep(pause).await;
            open(ctx).await
        };
        tokio::pin!(attempt);
        let result = loop {
            tokio::select! {
                r = &mut attempt => break r,
                msg = rx.recv() => match msg {
                    None | Some(Msg::Stop) => return None,
                    Some(Msg::Run(job)) => {
                        // Handed over just as the connection went: give it back.
                        let reason = "replica not connected".to_owned();
                        let _ = ctx.events.send(Cmd::Failed { idx: ctx.idx, job, sent: false, reason });
                    }
                    Some(Msg::Cancel(_) | Msg::Reconnect) => {}
                },
            }
        };
        match result {
            Ok(stream) => return Some(stream),
            Err(reason) => {
                let _ = ctx.events.send(Cmd::Down(ctx.idx, reason));
                delay = (delay * 2).clamp(RETRY_MIN, RETRY_MAX);
            }
        }
    }
}

async fn open(ctx: &Ctx) -> Result<TcpStream, String> {
    let cfg = ctx.cfg.borrow().clone();
    let mut stream = timeout(cfg.connect_timeout, TcpStream::connect(&*ctx.addr))
        .await
        .map_err(|_| "connect timed out".to_owned())?
        .map_err(|e| format!("connect: {e}"))?;
    let _ = stream.set_nodelay(true);
    timeout(GREET_TIMEOUT, async {
        greeting::connect(
            &mut stream,
            cfg.wire_version,
            cfg.backend_password.as_deref(),
        )
        .await
        .map_err(|e| format!("greeting: {e}"))?;
        if cfg.require_restricted {
            probe(&mut stream, cfg.wire_version).await?;
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|_| "greeting timed out".to_owned())??;
    Ok(stream)
}

/// Asks the replica for something only a restricted connection is refused.
async fn probe(stream: &mut TcpStream, version: u8) -> Result<(), String> {
    let io = |e: std::io::Error| format!("probe: {e}");
    stream
        .write_all(&frame::frame(version, MSG_SYNC, &value::str_atom(PROBE)))
        .await
        .map_err(io)?;
    let mut raw = [0u8; HEADER_LEN];
    stream.read_exact(&mut raw).await.map_err(io)?;
    let header = Header::parse(&raw, version, DRAIN_MAX).map_err(|e| format!("probe: {e}"))?;
    let mut payload = vec![0u8; header.size];
    stream.read_exact(&mut payload).await.map_err(io)?;
    match value::error_code(&payload).as_deref() {
        Some("access") => Ok(()),
        _ => Err(
            "replica is not restricted: start it with -U, or set require_restricted = false"
                .to_owned(),
        ),
    }
}

/// Holds an open connection: runs what it is handed, and notices a replica
/// that closes it or speaks out of turn.
async fn serve(ctx: &Ctx, stream: &mut TcpStream, rx: &mut mpsc::UnboundedReceiver<Msg>) -> Next {
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                None | Some(Msg::Stop) => return Next::Stop,
                Some(Msg::Reconnect) => return Next::Reconnect("reconnect requested".into()),
                Some(Msg::Cancel(_)) => {}
                Some(Msg::Run(job)) => {
                    let mut after = None;
                    match exchange(ctx, stream, job, rx, &mut after).await {
                        Exchange::Clean => {
                            let _ = ctx.events.send(Cmd::Finished(ctx.idx));
                        }
                        Exchange::Broken(reason) => return after.unwrap_or(Next::Reconnect(reason)),
                        Exchange::Retry(job, reason) => {
                            let failed = Cmd::Failed { idx: ctx.idx, job, sent: true, reason: reason.clone() };
                            let _ = ctx.events.send(failed);
                            return after.unwrap_or(Next::Reconnect(reason));
                        }
                    }
                    if let Some(next) = after {
                        return next;
                    }
                }
            },
            ready = stream.readable() => {
                let mut byte = [0u8; 1];
                match ready.and_then(|()| stream.try_read(&mut byte)) {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Ok(0) => return Next::Reconnect("closed by the replica".into()),
                    // A subscription a client left behind pushes here; a new
                    // connection is the only way to be rid of it.
                    Ok(_) => return Next::Reconnect("unsolicited data from the replica".into()),
                    Err(e) => return Next::Reconnect(e.to_string()),
                }
            }
        }
    }
}

enum Exchange {
    /// Answered, and the connection is on a frame boundary.
    Clean,
    /// The request is over but the connection cannot be used again.
    Broken(String),
    /// Nothing of an answer reached the client: another replica may try.
    Retry(Job, String),
}

/// Why the replica was sent a cancel byte.
#[derive(Clone, Copy, PartialEq)]
enum Why {
    Client,
    ClientGone,
    Timeout,
}

async fn exchange(
    ctx: &Ctx,
    stream: &mut TcpStream,
    job: Job,
    rx: &mut mpsc::UnboundedReceiver<Msg>,
    after: &mut Option<Next>,
) -> Exchange {
    let cfg = ctx.cfg.borrow().clone();
    let started = Instant::now();
    let wait = started.duration_since(job.arrived);
    let end = |outcome, bytes_out| Ending {
        backend: Some(ctx.addr.clone()),
        wait,
        exec: started.elapsed(),
        bytes_out,
        outcome,
    };

    if let Err(e) = stream.write_all(&job.frame).await {
        return Exchange::Retry(job, format!("send: {e}"));
    }

    let deadline = (!cfg.query_timeout.is_zero()).then(|| started + cfg.query_timeout);
    let mut cancel: Option<Why> = None;
    let mut grace: Option<Instant> = None;
    let mut raw = [0u8; HEADER_LEN];
    let mut filled = 0;
    let failure = loop {
        let cancel_now = tokio::select! {
            r = stream.read(&mut raw[filled..]) => match r {
                Ok(0) => break Some("closed by the replica".to_owned()),
                Ok(n) => {
                    filled += n;
                    if filled == HEADER_LEN {
                        break None;
                    }
                    continue;
                }
                Err(e) => break Some(format!("receive: {e}")),
            },
            msg = rx.recv(), if after.is_none() => match msg {
                Some(Msg::Cancel(id)) if id == job.id && cancel.is_none() => Why::Client,
                Some(Msg::Cancel(_)) => continue,
                Some(Msg::Run(other)) => {
                    let reason = "replica is busy".to_owned();
                    let _ = ctx.events.send(Cmd::Failed { idx: ctx.idx, job: other, sent: false, reason });
                    continue;
                }
                Some(Msg::Reconnect) => {
                    *after = Some(Next::Reconnect("reconnect requested".into()));
                    continue;
                }
                None | Some(Msg::Stop) => {
                    *after = Some(Next::Stop);
                    continue;
                }
            },
            () = job.reply.closed(), if cancel.is_none() => Why::ClientGone,
            () = sleep_until(deadline.unwrap_or(started).into()), if cancel.is_none() && deadline.is_some() => Why::Timeout,
            () = sleep_until(grace.unwrap_or(started).into()), if grace.is_some() => {
                break Some("did not answer a cancel in time".to_owned());
            }
        };
        let _ = oob::send(stream);
        cancel = Some(cancel_now);
        grace = Some(Instant::now() + cfg.cancel_grace);
    };

    let header = match failure {
        None => Header::parse(&raw, cfg.wire_version, PAYLOAD_MAX)
            .map_err(|e| e.to_string())
            .and_then(|h| {
                if h.msgtype == MSG_RESPONSE {
                    Ok(h)
                } else {
                    Err("unexpected frame from the replica".to_owned())
                }
            }),
        Some(reason) => Err(reason),
    };
    let header = match (header, cancel) {
        (Ok(header), _) => header,
        (Err(reason), None) => return Exchange::Retry(job, reason),
        // Cancelled already: nobody wants this request run again.
        (Err(reason), Some(why)) => {
            let outcome = match why {
                Why::Timeout => {
                    job.refuse("timeout", &timeout_message(&cfg)).await;
                    Outcome::Timeout
                }
                Why::Client => {
                    job.refuse("cancel", "interrupted").await;
                    Outcome::Cancelled
                }
                Why::ClientGone => Outcome::Cancelled,
            };
            ctx.shared.report(&job, end(outcome, 0));
            return Exchange::Broken(reason);
        }
    };

    // The budget ran out or the client left: the answer has no reader.
    if let Some(why @ (Why::Timeout | Why::ClientGone)) = cancel {
        let drained = drain(stream, header.size).await;
        let outcome = if why == Why::Timeout {
            job.refuse("timeout", &timeout_message(&cfg)).await;
            Outcome::Timeout
        } else {
            Outcome::Cancelled
        };
        ctx.shared.report(&job, end(outcome, 0));
        return match drained {
            Ok(()) => Exchange::Clean,
            Err(reason) => Exchange::Broken(reason),
        };
    }

    let mut client_gone = job
        .reply
        .send(Reply::Data(Bytes::copy_from_slice(&raw)))
        .await
        .is_err();
    let mut remaining = header.size;
    let mut head: Vec<u8> = Vec::with_capacity(9);
    let mut broken = None;
    while remaining > 0 {
        if client_gone && remaining > DRAIN_MAX {
            broken = Some("client left in the middle of a large answer".to_owned());
            break;
        }
        let mut chunk = BytesMut::with_capacity(remaining.min(CHUNK));
        let mut limited = (&mut *stream).take(remaining as u64);
        match timeout(TRANSFER_STALL, limited.read_buf(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => {
                remaining -= n;
                if head.len() < 9 {
                    head.extend_from_slice(&chunk[..n.min(9 - head.len())]);
                }
                if !client_gone {
                    client_gone = job.reply.send(Reply::Data(chunk.freeze())).await.is_err();
                }
            }
            Ok(Ok(_)) => {
                broken = Some("closed by the replica in the middle of an answer".to_owned())
            }
            Ok(Err(e)) => broken = Some(format!("receive: {e}")),
            Err(_) => broken = Some("replica stalled in the middle of an answer".to_owned()),
        }
        if broken.is_some() {
            break;
        }
    }
    let sent = (HEADER_LEN + header.size - remaining) as u64;
    if let Some(reason) = broken {
        let _ = job.reply.send(Reply::Abort).await;
        ctx.shared.report(&job, end(Outcome::Failed, sent));
        return Exchange::Broken(reason);
    }
    let _ = job.reply.send(Reply::Done).await;
    let outcome = match value::error_code(&head) {
        _ if client_gone => Outcome::Cancelled,
        Some(code) if code == "cancel" && cancel.is_some() => Outcome::Cancelled,
        Some(code) => Outcome::ServerError(code),
        None => Outcome::Ok,
    };
    ctx.shared.report(&job, end(outcome, sent));
    Exchange::Clean
}

fn timeout_message(cfg: &PoolConfig) -> String {
    format!(
        "query ran past its {} s budget",
        cfg.query_timeout.as_secs()
    )
}

/// Reads an answer nobody waits for off the wire, so the connection is on a
/// frame boundary again.
async fn drain(stream: &mut TcpStream, size: usize) -> Result<(), String> {
    if size > DRAIN_MAX {
        return Err("abandoned a large answer".to_owned());
    }
    let mut sink = vec![0u8; size];
    match timeout(TRANSFER_STALL, stream.read_exact(&mut sink)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!("receive: {e}")),
        Err(_) => Err("replica stalled in the middle of an answer".to_owned()),
    }
}
