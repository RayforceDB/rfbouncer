//! One connection to one replica: open it, keep it, run one request at a
//! time over it, and open it again when anything about it looks wrong.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep, sleep_until, timeout, timeout_at};

use super::{Cmd, Ending, Job, Reply, Shared};
use crate::config::PoolConfig;
use crate::stats::event::Outcome;
use crate::wire::frame::{
    self, FLAG_COMPRESSED, HEADER_LEN, Header, MSG_RESPONSE, MSG_SYNC, PAYLOAD_MAX,
};
use crate::wire::{compress, greeting, oob, value};

/// A replica answers the greeting from its evaluating thread, so this is how
/// long a replica busy with someone else's query may keep a new connection.
const GREET_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a replica may go silent in the middle of an answer, or refuse
/// to take more of a request.
const STALL: Duration = Duration::from_secs(60);
const RETRY_MIN: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(5);
/// A connection that lasted this long was a working one: the next attempt
/// after it is made at once.
const SETTLED: Duration = Duration::from_secs(1);
/// A peer that has vanished without closing is noticed within about this.
const KEEPALIVE: Duration = Duration::from_secs(30);
pub const CHUNK: usize = 64 * 1024;
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
    // How long to wait before the next attempt. It grows while attempts
    // fail, and also while they succeed only to be dropped at once: a
    // replica that greets and hangs up is not hammered.
    let mut delay = Duration::ZERO;
    loop {
        let Some(mut stream) = connect(&ctx, &mut rx, &mut delay).await else {
            break;
        };
        let born = Instant::now();
        let _ = ctx.events.send(Cmd::Up(ctx.idx));
        match serve(&ctx, &mut stream, &mut rx).await {
            Next::Reconnect(reason) => {
                let _ = ctx.events.send(Cmd::Down(ctx.idx, reason));
            }
            Next::Stop => break,
        }
        delay = if born.elapsed() >= SETTLED {
            Duration::ZERO
        } else {
            (delay * 2).clamp(RETRY_MIN, RETRY_MAX)
        };
    }
    let _ = ctx.events.send(Cmd::Gone(ctx.idx));
}

/// Tries until a connection is open, backing off between attempts. `None`
/// when told to stop meanwhile.
async fn connect(
    ctx: &Ctx,
    rx: &mut mpsc::UnboundedReceiver<Msg>,
    delay: &mut Duration,
) -> Option<TcpStream> {
    loop {
        let pause = *delay;
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
                    // A cancel for a request this replica no longer has: the
                    // pool remembers it and deals with the request itself.
                    Some(Msg::Cancel(_) | Msg::Reconnect) => {}
                },
            }
        };
        match result {
            Ok(stream) => return Some(stream),
            Err(reason) => {
                let _ = ctx.events.send(Cmd::Down(ctx.idx, reason));
                *delay = (*delay * 2).clamp(RETRY_MIN, RETRY_MAX);
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
    // Without this a replica whose host vanished mid-query, with nothing of
    // ours unacknowledged, would never be seen to be gone.
    let keepalive = TcpKeepalive::new()
        .with_time(KEEPALIVE)
        .with_interval(KEEPALIVE / 3);
    let _ = SockRef::from(&stream).set_tcp_keepalive(&keepalive);
    timeout(GREET_TIMEOUT, async {
        greeting::connect(
            &mut stream,
            cfg.wire_version,
            cfg.backend_password.as_deref(),
        )
        .await
        .map_err(|e| greeting_failure(&e, cfg.wire_version))?;
        if cfg.require_restricted {
            probe(&mut stream, cfg.wire_version).await?;
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|_| "greeting timed out".to_owned())??;
    Ok(stream)
}

/// Why a greeting failed. A server of another wire version may close
/// without a word, which reads as nothing more than an end: say what to
/// look at.
fn greeting_failure(e: &greeting::GreetError, version: u8) -> String {
    match e {
        greeting::GreetError::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof => {
            format!("greeting: closed by the replica; does it speak wire version {version}?")
        }
        e => format!("greeting: {e}"),
    }
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
                    // A connection that did not survive the request is said
                    // to be down before the request is said to be over, so
                    // the pool does not hand this replica the next one in
                    // between.
                    let broken = match exchange(ctx, stream, job, rx, &mut after).await {
                        Exchange::Clean => {
                            let _ = ctx.events.send(Cmd::Finished(ctx.idx));
                            None
                        }
                        Exchange::Broken(reason) => {
                            let _ = ctx.events.send(Cmd::Down(ctx.idx, reason.clone()));
                            let _ = ctx.events.send(Cmd::Finished(ctx.idx));
                            Some(reason)
                        }
                        Exchange::Retry(job, reason) => {
                            let _ = ctx.events.send(Cmd::Down(ctx.idx, reason.clone()));
                            let failed = Cmd::Failed { idx: ctx.idx, job, sent: true, reason: reason.clone() };
                            let _ = ctx.events.send(failed);
                            Some(reason)
                        }
                    };
                    match (after, broken) {
                        (Some(next), _) => return next,
                        (None, Some(reason)) => return Next::Reconnect(reason),
                        (None, None) => {}
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
    let deadline = (!cfg.query_timeout.is_zero()).then(|| started + cfg.query_timeout);

    // A replica that does not read, with a request too large for the socket
    // to swallow, would hold this write for good: it is given the request's
    // own budget, and no longer than a stalled transfer is given.
    let stalled = started + STALL;
    let written = tokio::select! {
        written = stream.write_all(&job.frame) => written.map_err(|e| format!("send: {e}")),
        () = sleep_until(deadline.unwrap_or(stalled).min(stalled).into()) => Err(String::new()),
        () = job.reply.closed() => {
            ctx.shared.report(&job, end(Outcome::Cancelled, 0));
            return Exchange::Broken("client left before its request was sent".into());
        }
    };
    match written {
        Ok(()) => {}
        Err(reason) if !reason.is_empty() => return Exchange::Retry(job, reason),
        Err(_) if deadline.is_some_and(|d| Instant::now() >= d) => {
            job.refuse("timeout", &timeout_message(&cfg)).await;
            ctx.shared.report(&job, end(Outcome::Timeout, 0));
            return Exchange::Broken("did not take the request within its budget".into());
        }
        Err(_) => return Exchange::Retry(job, "did not take the request".into()),
    }

    let mut cancel: Option<Why> = None;
    let mut grace: Option<Instant> = None;
    let mut closed = false;
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
            // Read on whatever is pending: a reconnect asked for meanwhile
            // must not make this request deaf to its client's cancel.
            msg = rx.recv(), if !closed => match msg {
                Some(Msg::Cancel(id)) if id == job.id && cancel.is_none() => Why::Client,
                Some(Msg::Cancel(_)) => continue,
                Some(Msg::Run(other)) => {
                    let reason = "replica is busy".to_owned();
                    let _ = ctx.events.send(Cmd::Failed { idx: ctx.idx, job: other, sent: false, reason });
                    continue;
                }
                Some(Msg::Reconnect) => {
                    after.get_or_insert(Next::Reconnect("reconnect requested".into()));
                    continue;
                }
                Some(Msg::Stop) => {
                    *after = Some(Next::Stop);
                    continue;
                }
                None => {
                    closed = true;
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

    // From here the replica's thread is free again but its connection is
    // not, until the client has taken the whole answer. A client that takes
    // it too slowly is cut off, so it cannot hold the replica as long as it
    // likes.
    let due = (!cfg.transfer_timeout.is_zero())
        .then(|| tokio::time::Instant::now() + cfg.transfer_timeout);
    let far = tokio::time::Instant::now() + Duration::from_secs(86_400 * 365);
    // An answer a Rayforce server would have compressed for this client,
    // had the client been its peer: the replica saw the bouncer, most often
    // on its own host, and sent it plain. It is taken whole before any of
    // it is sent, as its compressed size goes into the header.
    let to_compress = cfg.compress.applies(job.client.ip());
    let squeeze = to_compress
        && header.flags & FLAG_COMPRESSED == 0
        && header.size > compress::THRESHOLD
        && header.size <= cfg.compress_max_bytes;
    // The answer is also gathered where the cache may keep it.
    let keep = !cfg.cache_ttl.is_zero()
        && job.frame.len() + HEADER_LEN + header.size <= cfg.cache_max_entry;
    let mut whole = (squeeze || keep).then(|| {
        let mut whole = BytesMut::with_capacity(HEADER_LEN + header.size);
        whole.extend_from_slice(&raw);
        whole
    });
    let mut client_gone = if squeeze {
        job.reply.is_closed()
    } else {
        job.reply
            .send(Reply::Data(Bytes::copy_from_slice(&raw)))
            .await
            .is_err()
    };
    let mut remaining = header.size;
    let mut head: Vec<u8> = Vec::with_capacity(9);
    let mut broken = None;
    let mut slow = false;
    while remaining > 0 {
        if client_gone && remaining > DRAIN_MAX {
            broken = Some("client left in the middle of a large answer".to_owned());
            break;
        }
        let mut chunk = BytesMut::with_capacity(remaining.min(CHUNK));
        let mut limited = (&mut *stream).take(remaining as u64);
        match timeout(STALL, limited.read_buf(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => {
                remaining -= n;
                if head.len() < 9 {
                    head.extend_from_slice(&chunk[..n.min(9 - head.len())]);
                }
                if let Some(whole) = &mut whole {
                    whole.extend_from_slice(&chunk);
                }
                if squeeze {
                    client_gone = job.reply.is_closed();
                } else if !client_gone {
                    match timeout_at(
                        due.unwrap_or(far),
                        job.reply.send(Reply::Data(chunk.freeze())),
                    )
                    .await
                    {
                        Ok(sent) => client_gone = sent.is_err(),
                        Err(_) => {
                            slow = true;
                            broken = Some("client took the answer too slowly".to_owned());
                        }
                    }
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
    let mut sent = (HEADER_LEN + header.size - remaining) as u64;
    if squeeze {
        sent = 0;
        if broken.is_none() && !client_gone {
            let plain = whole.take().unwrap_or_default().freeze();
            let mut rest = squeezed(&ctx.shared.counters, &header, plain).await;
            // A copy: the compressed frame sits in memory sized for the
            // plain one, which the cache would hold without counting it.
            whole = keep.then(|| BytesMut::from(&rest[..]));
            while !rest.is_empty() && !client_gone && !slow {
                let part = rest.split_to(rest.len().min(CHUNK));
                let size = part.len() as u64;
                match timeout_at(due.unwrap_or(far), job.reply.send(Reply::Data(part))).await {
                    Ok(Ok(())) => sent += size,
                    Ok(Err(_)) => client_gone = true,
                    Err(_) => {
                        slow = true;
                        broken = Some("client took the answer too slowly".to_owned());
                    }
                }
            }
        }
    }
    if let Some(reason) = broken {
        // `try_send`: a client that is not taking the answer is not waited
        // for again; dropping the channel closes its connection just as well.
        let _ = job.reply.try_send(Reply::Abort);
        let outcome = if slow {
            Outcome::Timeout
        } else {
            Outcome::Failed
        };
        ctx.shared.report(&job, end(outcome, sent));
        return Exchange::Broken(reason);
    }
    let _ = job.reply.send(Reply::Done).await;
    // A compressed payload opens with its size, not with a type: only a
    // plain one can be told for an error by its first byte. Errors are far
    // too short to be compressed.
    let code = if header.flags & FLAG_COMPRESSED == 0 {
        value::error_code(&head)
    } else {
        None
    };
    let outcome = match code {
        _ if client_gone => Outcome::Cancelled,
        Some(code) if code == "cancel" && cancel.is_some() => Outcome::Cancelled,
        Some(code) => Outcome::ServerError(code),
        None => Outcome::Ok,
    };
    if let Some(whole) = whole.filter(|_| keep && outcome == Outcome::Ok) {
        let cache = &ctx.shared.cache;
        cache.insert(&job.frame, to_compress, whole.freeze(), started, &cfg);
    }
    ctx.shared.report(&job, end(outcome, sent));
    Exchange::Clean
}

/// The frame a client is sent in place of the plain one a replica gave:
/// compressed where that makes it smaller, the same where it does not.
async fn squeezed(counters: &super::Counters, header: &Header, plain: Bytes) -> Bytes {
    let input = plain.clone();
    let pack = move || compress::compress(&input[HEADER_LEN..]);
    // A large answer takes milliseconds of processor, which the tasks that
    // share this thread are not made to wait for.
    let packed = if plain.len() > CHUNK {
        tokio::task::spawn_blocking(pack).await.unwrap_or(None)
    } else {
        pack()
    };
    let Some(mut packed) = packed else {
        return plain;
    };
    counters.compressed.fetch_add(1, Relaxed);
    let saved = (plain.len() - HEADER_LEN - packed.len()) as u64;
    counters.compress_saved.fetch_add(saved, Relaxed);
    let head = Header {
        flags: header.flags | FLAG_COMPRESSED,
        size: packed.len(),
        ..*header
    };
    packed.splice(0..0, head.encode());
    Bytes::from(packed)
}

fn timeout_message(cfg: &PoolConfig) -> String {
    format!("query ran past its budget of {:?}", cfg.query_timeout)
}

/// Reads an answer nobody waits for off the wire, so the connection is on a
/// frame boundary again.
async fn drain(stream: &mut TcpStream, size: usize) -> Result<(), String> {
    if size > DRAIN_MAX {
        return Err("abandoned a large answer".to_owned());
    }
    let mut sink = vec![0u8; size];
    match timeout(STALL, stream.read_exact(&mut sink)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!("receive: {e}")),
        Err(_) => Err("replica stalled in the middle of an answer".to_owned()),
    }
}
