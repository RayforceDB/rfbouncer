//! rfbench: many clients at once against a Rayforce server or a bouncer,
//! which look the same from here. It is how docs/performance.md was measured.
//!
//! Every client sends its next request when the last one is answered, so the
//! load follows the server: a slow server is asked less, and the latencies
//! say how long a request took, not how late it would have been under a
//! fixed arrival rate.

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::time::timeout;

use rfbouncer::config::parse_duration;
use rfbouncer::wire::frame::{self, FLAG_COMPRESSED, HEADER_LEN, Header, MSG_SYNC, PAYLOAD_MAX};
use rfbouncer::wire::{greeting, value};

/// Load for a Rayforce server or an rfbouncer: throughput, latency, errors.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// The server: host:port of a replica or of the bouncer.
    addr: String,
    /// Clients sending at once.
    #[arg(short, long, default_value_t = 1)]
    clients: usize,
    /// A connection for every request, as a client without a pool of its
    /// own does it. Latency then includes the connect and the greeting.
    #[arg(long)]
    reconnect: bool,
    /// How long to send for: 500ms, 10s, 2m.
    #[arg(short, long, default_value = "5s", value_parser = duration)]
    duration: Duration,
    /// Stop after this many requests in all, however long they take.
    #[arg(short = 'n', long)]
    requests: Option<u64>,
    /// The request, as source text.
    #[arg(short, long, default_value = "(+ 1 2)")]
    query: String,
    /// The user name sent if the server asks for credentials.
    #[arg(long, default_value = "")]
    user: String,
    /// The password sent if the server asks for credentials.
    #[arg(long)]
    password: Option<String>,
    /// The wire version to speak: 3 for Rayforce up to 3.0, 4 after.
    #[arg(long, default_value_t = 4)]
    wire: u8,
    /// How long a connect or a request may take before it counts as an error.
    #[arg(long, default_value = "30s", value_parser = duration)]
    timeout: Duration,
}

fn duration(text: &str) -> Result<Duration, String> {
    parse_duration(text).ok_or_else(|| format!("not a duration: {text}"))
}

/// What one client saw.
#[derive(Default)]
struct Tally {
    /// Microseconds each answered request took.
    latencies: Vec<u64>,
    /// Requests that were not answered with a value, by what went wrong.
    errors: BTreeMap<String, u64>,
}

struct Run {
    args: Args,
    frame: bytes::Bytes,
    /// Requests still to send under `--requests`.
    left: AtomicU64,
    /// Persistent clients take turns to connect.
    connecting: Semaphore,
}

impl Run {
    /// Takes one request off the budget; false when there is none left.
    fn take(&self, deadline: Instant) -> bool {
        if self.args.requests.is_none() {
            return Instant::now() < deadline;
        }
        self.left
            .fetch_update(Relaxed, Relaxed, |n| n.checked_sub(1))
            .is_ok()
    }

    async fn connect(&self) -> Result<TcpStream, String> {
        let args = &self.args;
        let mut stream = match timeout(args.timeout, TcpStream::connect(&args.addr)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return Err(format!("connect: {:?}", e.kind())),
            Err(_) => return Err("connect: timed out".to_owned()),
        };
        let _ = stream.set_nodelay(true);
        // Closed with a reset, so the port is free at once. Closed in the
        // ordinary way it is kept for twice the segment lifetime, and a
        // machine has only so many: macOS runs out after 16 thousand
        // connections, and then nothing on it can connect for half a minute.
        let _ = socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO));
        let greeted =
            greeting::connect_as(&mut stream, args.wire, &args.user, args.password.as_deref());
        match timeout(args.timeout, greeted).await {
            Ok(Ok(())) => Ok(stream),
            Ok(Err(greeting::GreetError::Io(e))) => Err(format!("greeting: {:?}", e.kind())),
            Ok(Err(e)) => Err(format!("greeting: {e}")),
            Err(_) => Err("greeting: timed out".to_owned()),
        }
    }

    /// Sends the request and reads its whole answer: `Ok(Some(code))` when
    /// the answer is an error, which leaves the connection as good as a
    /// value does.
    async fn exchange(
        &self,
        stream: &mut TcpStream,
        answer: &mut Vec<u8>,
    ) -> Result<Option<String>, String> {
        let io = |what: &str, e: std::io::Error| format!("{what}: {:?}", e.kind());
        stream
            .write_all(&self.frame)
            .await
            .map_err(|e| io("send", e))?;
        let mut raw = [0u8; HEADER_LEN];
        stream
            .read_exact(&mut raw)
            .await
            .map_err(|e| io("receive", e))?;
        let header =
            Header::parse(&raw, self.args.wire, PAYLOAD_MAX).map_err(|e| format!("answer: {e}"))?;
        answer.resize(header.size, 0);
        stream
            .read_exact(answer)
            .await
            .map_err(|e| io("receive", e))?;
        // A compressed payload opens with its size, not with a type.
        if header.flags & FLAG_COMPRESSED != 0 {
            return Ok(None);
        }
        Ok(value::error_code(answer))
    }

    /// One client: connects, says it is ready, and sends from the moment it
    /// is given a deadline until that deadline.
    async fn client(
        &self,
        ready: mpsc::Sender<()>,
        mut go: watch::Receiver<Option<Instant>>,
    ) -> Tally {
        let mut tally = Tally::default();
        let mut answer = Vec::new();
        // Persistent clients are all connected before the first request, so
        // the measurement is of requests and not of the rush to connect.
        let mut held = None;
        if !self.args.reconnect {
            // A few at a time: a server's backlog of connections not yet
            // accepted is short (128 on macOS), and past it they are dropped.
            let _turn = self.connecting.acquire().await;
            // One that fails is tried again by the first request, and
            // counted there.
            held = self.connect().await.ok();
        }
        let _ = ready.send(()).await;
        let Ok(Some(deadline)) = go.wait_for(Option::is_some).await.map(|d| *d) else {
            return tally;
        };
        while self.take(deadline) {
            let began = Instant::now();
            let one = async {
                let mut stream = match held.take() {
                    Some(stream) => stream,
                    None => self.connect().await?,
                };
                let refused = self.exchange(&mut stream, &mut answer).await?;
                Ok::<_, String>((stream, refused))
            };
            let failure = match timeout(self.args.timeout, one).await {
                Ok(Ok((stream, refused))) => {
                    if !self.args.reconnect {
                        held = Some(stream);
                    }
                    refused.map(|code| format!("error: {code}"))
                }
                Ok(Err(e)) => {
                    // A server that is not there refuses in microseconds: do
                    // not turn that into a million errors a second.
                    if e.starts_with("connect") {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Some(e)
                }
                Err(_) => Some("timed out".to_owned()),
            };
            match failure {
                None => tally.latencies.push(began.elapsed().as_micros() as u64),
                Some(kind) => *tally.errors.entry(kind).or_default() += 1,
            }
        }
        tally
    }
}

/// The value `p` percent of the way through `sorted`.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    let at = ((sorted.len() as f64 * p / 100.0).ceil() as usize).max(1) - 1;
    sorted.get(at).copied().unwrap_or(0)
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let frame = frame::frame(args.wire, MSG_SYNC, &value::str_atom(&args.query));
    let clients = args.clients.max(1);
    let run = Arc::new(Run {
        frame,
        left: AtomicU64::new(args.requests.unwrap_or(0)),
        connecting: Semaphore::new(32),
        args,
    });
    let (ready, mut readied) = mpsc::channel(clients);
    let (go, _) = watch::channel(None);
    let tasks: Vec<_> = (0..clients)
        .map(|_| {
            let (run, ready, go) = (run.clone(), ready.clone(), go.subscribe());
            tokio::spawn(async move { run.client(ready, go).await })
        })
        .collect();
    for _ in 0..clients {
        let _ = readied.recv().await;
    }
    let started = Instant::now();
    go.send_replace(Some(started + run.args.duration));

    let mut latencies = Vec::new();
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    for task in tasks {
        let Ok(tally) = task.await else {
            *errors.entry("client panicked".to_owned()).or_default() += 1;
            continue;
        };
        latencies.extend(tally.latencies);
        for (kind, n) in tally.errors {
            *errors.entry(kind).or_default() += n;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let (ok, failed) = (latencies.len() as u64, errors.values().sum::<u64>());

    let args = &run.args;
    let how = if args.reconnect {
        "a connection per request"
    } else {
        "persistent connections"
    };
    println!(
        "target {}  clients {clients}  {how}  wire {}  query {:?}",
        args.addr, args.wire, args.query
    );
    println!("elapsed_s {elapsed:.3}  ok {ok}  errors {failed}");
    println!("throughput_rps {:.1}", ok as f64 / elapsed);
    println!(
        "latency_us  p50 {}  p95 {}  p99 {}  max {}",
        percentile(&latencies, 50.0),
        percentile(&latencies, 95.0),
        percentile(&latencies, 99.0),
        latencies.last().copied().unwrap_or(0)
    );
    for (kind, n) in &errors {
        println!("error  {n}  {kind}");
    }
    // Nothing answered is a failure to measure anything; errors among
    // answers are a result, and are reported as one.
    if ok == 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
