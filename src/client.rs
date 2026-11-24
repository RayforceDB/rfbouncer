//! One client connection: the greeting, then requests one at a time, each
//! handed to its pool and answered with whatever the pool sends back.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::admin;
use crate::config::Target;
use crate::core::{ClientInfo, Core};
use crate::pool::{Cmd, Job, Reply};
use crate::wire::frame::{HEADER_LEN, Header, MSG_SYNC};
use crate::wire::{greeting, oob};

/// How long a client may take over its greeting and credentials.
const GREET_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a peer with the wrong credentials waits to be told so.
const REFUSAL_DELAY: Duration = Duration::from_millis(500);
/// How long a client may refuse to take bytes of its answer. A client that
/// stops reading would otherwise hold a replica for as long as it liked.
const WRITE_STALL: Duration = Duration::from_secs(30);

pub async fn serve(mut stream: TcpStream, peer: SocketAddr, core: Arc<Core>) {
    let greeted = timeout(GREET_TIMEOUT, greet(&mut stream, &core)).await;
    let Ok(Ok(Some((version, user, pool)))) = greeted else {
        return;
    };
    match pool {
        None => admin::session(stream, version, &core).await,
        Some(pool) => {
            if let Err(e) = proxy(stream, peer, version, user.into(), pool.into(), &core).await {
                tracing::debug!(%peer, error = %e, "client connection ended");
            }
        }
    }
}

/// The wire version, the user, and the pool the credentials name, or no pool
/// for the admin console. `None` when the client was turned away.
async fn greet(
    stream: &mut TcpStream,
    core: &Core,
) -> std::io::Result<Option<(u8, String, Option<String>)>> {
    let version = greeting::read_hello(stream).await?;
    let config = core.config();
    if !config.speaks(version) {
        // Answer with a version of ours, so the client reports a mismatch
        // rather than a reset.
        greeting::write_hello(stream, config.only_pool().wire_version, false).await?;
        return Ok(None);
    }
    if !config.auth_required() {
        greeting::write_hello(stream, version, false).await?;
        return Ok(Some((
            version,
            String::new(),
            Some(config.only_pool().name.clone()),
        )));
    }
    greeting::write_hello(stream, version, true).await?;
    let credentials = greeting::read_credentials(stream).await?;
    let target = match config.resolve(&credentials.user, &credentials.password, version) {
        Target::Admin => Some(None),
        Target::Pool(pool) => Some(Some(pool.name.clone())),
        Target::Rejected => None,
    };
    if target.is_none() {
        // Said, counted, and answered late: guessing a password is then
        // neither silent nor fast.
        core.auth_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let peer = stream
            .peer_addr()
            .map_or_else(|_| "?".to_owned(), |a| a.to_string());
        tracing::warn!(%peer, user = %credentials.user, "credentials refused");
        tokio::time::sleep(REFUSAL_DELAY).await;
    }
    greeting::write_verdict(stream, target.is_some()).await?;
    Ok(target.map(|pool| (version, credentials.user, pool)))
}

async fn proxy(
    stream: TcpStream,
    peer: SocketAddr,
    version: u8,
    user: Arc<str>,
    pool_name: Arc<str>,
    core: &Arc<Core>,
) -> std::io::Result<()> {
    let registered = core.register(ClientInfo {
        addr: peer,
        user: user.clone(),
        pool: pool_name.clone(),
        since: Instant::now(),
        requests: 0,
    });
    let mut shutdown = core.shutdown.subscribe();
    let cancels = oob::Watch::new(&stream)?;
    let (mut rd, mut wr) = stream.into_split();
    let mut buf = BytesMut::new();
    loop {
        let config = core.config();
        let idle = config.client_idle_timeout;
        let next = read_frame(&mut rd, &mut buf, version, config.max_request_bytes);
        let frame = tokio::select! {
            frame = next => frame?,
            () = tokio::time::sleep(idle), if !idle.is_zero() => return Ok(()),
            // Between two requests is the one place a client can be let go
            // without losing anything.
            _ = shutdown.wait_for(|stop| *stop) => return Ok(()),
        };
        let Some(frame) = frame else {
            return Ok(());
        };
        if frame[7] != MSG_SYNC {
            tracing::warn!(%peer, msgtype = frame[7], "client sent a frame that is not a request; closing");
            return Ok(());
        }
        let Some(pool) = core.pool(&pool_name) else {
            return Ok(());
        };
        registered.count_request();
        let (reply, mut replies) = mpsc::channel(8);
        let id = core.next_id();
        pool.send(Cmd::Submit(Job {
            id,
            frame,
            reply,
            client: peer,
            user: user.clone(),
            arrived: Instant::now(),
            arrived_at: SystemTime::now(),
            queued_at: Instant::now(),
            retries: 0,
            target: None,
            avoid: None,
        }));

        let limit = HEADER_LEN + config.max_request_bytes;
        let mut cancelled = false;
        loop {
            tokio::select! {
                reply = replies.recv() => match reply {
                    Some(Reply::Data(bytes)) => match timeout(WRITE_STALL, wr.write_all(&bytes)).await {
                        Ok(written) => written?,
                        Err(_) => return Ok(()),
                    },
                    Some(Reply::Done) => break,
                    Some(Reply::Abort) | None => return Ok(()),
                },
                () = cancels.wait(), if !cancelled => {
                    cancelled = true;
                    pool.send(Cmd::Cancel(id));
                }
                // Reading on is how a client that went away is noticed; what
                // it reads is the start of the next request, kept for later.
                read = rd.read_buf(&mut buf), if buf.len() < limit => {
                    if read? == 0 {
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// One whole frame, header included, or `None` when the client closed the
/// connection between two frames.
pub async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    rd: &mut R,
    buf: &mut BytesMut,
    version: u8,
    max: usize,
) -> std::io::Result<Option<Bytes>> {
    let invalid = |e| std::io::Error::new(std::io::ErrorKind::InvalidData, e);
    let mut need = HEADER_LEN;
    loop {
        if buf.len() >= HEADER_LEN && need == HEADER_LEN {
            let mut raw = [0u8; HEADER_LEN];
            raw.copy_from_slice(&buf[..HEADER_LEN]);
            need += Header::parse(&raw, version, max).map_err(invalid)?.size;
            buf.reserve(need.saturating_sub(buf.len()));
        }
        if buf.len() >= need && need > HEADER_LEN {
            return Ok(Some(buf.split_to(need).freeze()));
        }
        if rd.read_buf(buf).await? == 0 {
            return if buf.is_empty() {
                Ok(None)
            } else {
                Err(std::io::ErrorKind::UnexpectedEof.into())
            };
        }
    }
}
