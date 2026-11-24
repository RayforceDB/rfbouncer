//! What the integration tests share: real Rayforce processes, the bouncer's
//! own binary, and a small blocking client that speaks the wire.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

pub mod fake;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use rfbouncer::wire::frame::{self, HEADER_LEN, Header, MSG_SYNC, PAYLOAD_MAX};
use rfbouncer::wire::value;

/// The wire version the suite speaks: the one the Rayforce under test
/// does. `RFBOUNCER_TEST_WIRE=3` runs every test against a version 3 server.
pub fn wire() -> u8 {
    std::env::var("RFBOUNCER_TEST_WIRE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4)
}

/// A wire version the suite does not speak.
pub fn other_wire() -> u8 {
    if wire() == 4 { 3 } else { 4 }
}

/// `body` with the suite's wire version given to every pool and to the
/// statistics store, unless it names a version itself.
fn versioned(body: &str) -> String {
    let mut body = body.to_owned();
    if !body.contains("wire_version") {
        let line = format!("wire_version = {}\n", wire());
        body = body
            .replace("[[pool]]\n", &format!("[[pool]]\n{line}"))
            .replace("[stats]\n", &format!("[stats]\n{line}"));
    }
    // The replicas' status streams are turned off in every pool as well,
    // unless the body sets `status_interval` itself. A stand-in replica is
    // scripted connection by connection, and the stream is one more
    // connection that opens with a frame of its own: the tests that are
    // about it ask for it.
    if !body.contains("status_interval") {
        body = body.replace("[[pool]]\n", "[[pool]]\nstatus_interval = \"0\"\n");
    }
    body
}

/// A query that keeps a replica's thread for about `n / 7500` seconds on the
/// machine these tests were written on, and stops at a cancel within
/// milliseconds. Its answer is `n`.
pub fn slow(n: u32) -> String {
    format!("(count (map (fn [i] (sum (til 1000000))) (til {n})))")
}

/// Held while a socket is made and while a process is started.
///
/// On macOS a new socket is marked close-on-exec in a second step, so a
/// process started by another thread in between inherits it. A Rayforce
/// started at that instant kept another test's listener open, and with it
/// that test's port. One at a time, nothing is ever inherited.
fn exclusive() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

pub fn connect(port: u16) -> std::io::Result<TcpStream> {
    let _one_at_a_time = exclusive();
    TcpStream::connect(("127.0.0.1", port))
}

pub fn listen(port: u16) -> std::io::Result<TcpListener> {
    let _one_at_a_time = exclusive();
    TcpListener::bind(("127.0.0.1", port))
}

fn spawn(command: &mut Command) -> Child {
    let _one_at_a_time = exclusive();
    command.spawn().unwrap()
}

/// A port no other test is given: from a range the system does not hand
/// out, a different stretch of it for each test process, each used once.
pub fn free_port() -> u16 {
    static NEXT_PORT: AtomicUsize = AtomicUsize::new(0);
    // `RFBOUNCER_TEST_PORT_BASE` gives a run a range of its own, for when
    // several suites share a machine and must not meet.
    let base = std::env::var("RFBOUNCER_TEST_PORT_BASE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());
    let stretch = (std::process::id() as usize % 100) * 120;
    let n = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
    assert!(
        n < 120,
        "a test process may take 120 ports; give it a longer stretch"
    );
    base.map_or(20_000 + stretch + n, |base| {
        base + (std::process::id() as usize % 8) * 120 + n
    }) as u16
}

fn wait_for_port(port: u16, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while connect(port).is_err() {
        assert!(Instant::now() < deadline, "{what} did not open port {port}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The Rayforce binary under test: `RAYFORCE_BIN`, or a copy kept in
/// `.cache/` beside the manifest.
fn rayforce_bin() -> PathBuf {
    match std::env::var_os("RAYFORCE_BIN") {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cache/rayforce"),
    }
}

pub struct Rayforce {
    child: Child,
    pub port: u16,
}

impl Rayforce {
    /// A server on a free port. `args` follow `-p PORT`, so `&["-U", "pw"]`
    /// starts a restricted one.
    pub fn start(args: &[&str]) -> Self {
        Self::start_on(free_port(), args)
    }

    pub fn start_on(port: u16, args: &[&str]) -> Self {
        let bin = rayforce_bin();
        assert!(
            bin.exists(),
            "no Rayforce binary at {}: set RAYFORCE_BIN",
            bin.display()
        );
        let child = spawn(
            Command::new(bin)
                .arg("-p")
                .arg(port.to_string())
                .args(args)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null()),
        );
        // Owned before the wait, so a server that never opens its port is
        // still killed when the wait gives up.
        let server = Self { child, port };
        wait_for_port(port, "rayforce");
        server
    }

    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Rayforce {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Runs `script` in a fresh Rayforce process and returns what it printed.
/// This is the real client: what it can read, any Rayforce client can.
pub fn rayforce_eval(script: &str) -> String {
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("rfbouncer-test-{}-{n}.rfl", std::process::id()));
    std::fs::write(&path, script).unwrap();
    let mut child = spawn(
        Command::new(rayforce_bin())
            .arg("-f")
            .arg(&path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("the script did not end: {script}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = std::fs::remove_file(&path);
    let output = child.wait_with_output().unwrap();
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

pub fn signal(pid: u32, name: &str) {
    let status = Command::new("kill")
        .arg(format!("-{name}"))
        .arg(pid.to_string())
        .status()
        .unwrap();
    assert!(status.success());
}

pub struct Bouncer {
    child: Child,
    pub port: u16,
    pub config: PathBuf,
}

static NEXT: AtomicUsize = AtomicUsize::new(0);

impl Bouncer {
    /// Starts the bouncer on a free port with `body` as the rest of its
    /// configuration, and waits for it to have reached its replicas, so the
    /// first request goes to the first replica listed. An admin password,
    /// `root`, is added for that unless `body` sets one.
    pub fn start(body: &str) -> Self {
        let body = if body.contains("admin_password") {
            body.to_owned()
        } else {
            format!("admin_password = \"root\"\n{body}")
        };
        let bouncer = Self::start_as_is(&body);
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            let (status, live) = http_get(bouncer.port, "/api/live", Some("root"));
            if status == 200 && !live.contains("\"state\":\"down\"") {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        bouncer
    }

    /// Starts the bouncer with exactly `body` after its `listen` line, and
    /// returns as soon as its port is open.
    pub fn start_as_is(body: &str) -> Self {
        let port = free_port();
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rfbouncer-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = dir.join("rfbouncer.toml");
        std::fs::write(
            &config,
            format!("listen = \"127.0.0.1:{port}\"\n{}", versioned(body)),
        )
        .unwrap();
        // Kept beside the configuration, for the tests that read it.
        let log = if std::env::var_os("RFBOUNCER_TEST_LOG").is_some() {
            Stdio::inherit()
        } else {
            std::fs::File::create(dir.join("log")).unwrap().into()
        };
        let child = spawn(
            Command::new(env!("CARGO_BIN_EXE_rfbouncer"))
                .arg("--config")
                .arg(&config)
                .stdin(Stdio::null())
                .stderr(log),
        );
        let bouncer = Self {
            child,
            port,
            config,
        };
        wait_for_port(port, "rfbouncer");
        bouncer
    }

    /// What `/api/live` says, read with the admin password `root`.
    pub fn live(&self) -> serde_json::Value {
        http_json(self.port, "/api/live", "root")
    }

    /// Waits until `n` replica connections are running a request: the
    /// condition a test means when it says "the slow query is running".
    pub fn wait_busy(&self, n: usize) {
        eventually(&format!("{n} replicas busy"), || {
            let live = self.live();
            let busy = live["pools"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|p| p["backends"].as_array().unwrap().iter());
            busy.filter(|b| b["state"] == "busy").count() >= n
        });
    }

    /// Waits until `n` requests are waiting for a replica.
    pub fn wait_queued(&self, n: usize) {
        eventually(&format!("{n} requests queued"), || {
            let live = self.live();
            live["pools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| p["queue"].as_array().unwrap().len())
                .sum::<usize>()
                >= n
        });
    }

    /// The value of the metric line that starts with `line`, e.g.
    /// `rfbouncer_requests_total{pool="main",outcome="ok"}`.
    pub fn metric(&self, line: &str) -> f64 {
        let (status, body) = http_get(self.port, "/metrics", Some("root"));
        assert_eq!(status, 200);
        let found = body
            .lines()
            .find(|l| l.starts_with(line))
            .unwrap_or_else(|| panic!("no metric {line} in:\n{body}"));
        found.rsplit(' ').next().unwrap().parse().unwrap()
    }

    /// Replaces the configuration and has the bouncer read it again.
    pub fn reload(&self, body: &str) {
        let body = versioned(body);
        std::fs::write(
            &self.config,
            format!("listen = \"127.0.0.1:{}\"\n{body}", self.port),
        )
        .unwrap();
        signal(self.child.id(), "HUP");
    }

    /// What the bouncer has logged so far.
    pub fn log(&self) -> String {
        std::fs::read_to_string(self.dir().join("log")).unwrap()
    }

    pub fn dir(&self) -> PathBuf {
        self.config.parent().unwrap().to_owned()
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Whether the process has ended, and how.
    pub fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().unwrap()
    }
}

impl Bouncer {
    /// A second process on the same port, from the same configuration file,
    /// which must say `reuse_port = true`. The port is open already, so
    /// this waits for the new process to say that it listens.
    pub fn start_beside(&self) -> Self {
        use std::io::BufRead;
        let mut child = spawn(
            Command::new(env!("CARGO_BIN_EXE_rfbouncer"))
                .arg("--config")
                .arg(&self.config)
                .stdin(Stdio::null())
                .stderr(Stdio::piped()),
        );
        let log = std::io::BufReader::new(child.stderr.take().unwrap());
        let bouncer = Self {
            child,
            port: self.port,
            config: self.config.clone(),
        };
        let (started, listening) = std::sync::mpsc::channel();
        // Read to the end, so the process never blocks on a full pipe.
        std::thread::spawn(move || {
            for line in log.lines().map_while(Result::ok) {
                if line.contains("rfbouncer started") {
                    let _ = started.send(());
                }
            }
        });
        listening
            .recv_timeout(Duration::from_secs(10))
            .expect("the second bouncer did not start");
        bouncer
    }
}

impl Drop for Bouncer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(self.dir());
    }
}

/// A pool named `main` over these replicas, with `extra` lines appended.
pub fn pool(replicas: &[&Rayforce], extra: &str) -> String {
    let backends: Vec<String> = replicas
        .iter()
        .map(|r| format!("\"{}\"", r.addr()))
        .collect();
    format!(
        "[[pool]]\nname = \"main\"\nbackends = [{}]\n{extra}\n",
        backends.join(", ")
    )
}

#[derive(Debug)]
pub enum Refused {
    Version(u8),
    Credentials,
    Closed,
}

pub struct Client {
    pub stream: TcpStream,
    pub version: u8,
}

impl Client {
    pub fn connect(port: u16) -> Self {
        Self::connect_as(port, wire(), None).unwrap()
    }

    /// Opens a connection the way a Rayforce client does. `credentials` are
    /// sent only if the server asks for them.
    pub fn connect_as(
        port: u16,
        version: u8,
        credentials: Option<(&str, &str)>,
    ) -> Result<Self, Refused> {
        let mut stream = connect(port).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        stream.set_nodelay(true).unwrap();
        stream.write_all(&[version, 0]).unwrap();
        let mut hello = [0u8; 2];
        stream.read_exact(&mut hello).map_err(|_| Refused::Closed)?;
        if hello[0] != version {
            return Err(Refused::Version(hello[0]));
        }
        if hello[1] & 1 == 1 {
            let (user, password) = credentials.unwrap_or(("", ""));
            let text = format!("{user}:{password}\0");
            stream.write_all(&[text.len() as u8]).unwrap();
            stream.write_all(text.as_bytes()).unwrap();
            let mut verdict = [1u8; 1];
            stream
                .read_exact(&mut verdict)
                .map_err(|_| Refused::Closed)?;
            if verdict[0] != 0 {
                return Err(Refused::Credentials);
            }
        }
        Ok(Self { stream, version })
    }

    pub fn send(&mut self, source: &str) {
        self.send_frame(MSG_SYNC, &value::str_atom(source));
    }

    pub fn send_frame(&mut self, msgtype: u8, payload: &[u8]) {
        self.stream
            .write_all(&frame::frame(self.version, msgtype, payload))
            .unwrap();
    }

    /// The next frame's header and payload, or the error that came instead.
    pub fn try_recv(&mut self) -> std::io::Result<(Header, Vec<u8>)> {
        let mut raw = [0u8; HEADER_LEN];
        self.stream.read_exact(&mut raw)?;
        let header = Header::parse(&raw, self.version, PAYLOAD_MAX).unwrap();
        let mut payload = vec![0u8; header.size];
        self.stream.read_exact(&mut payload)?;
        Ok((header, payload))
    }

    pub fn recv(&mut self) -> Vec<u8> {
        self.try_recv().unwrap().1
    }

    /// Sends `source` and returns the payload of its answer.
    pub fn query(&mut self, source: &str) -> Vec<u8> {
        self.send(source);
        self.recv()
    }

    /// The answer's error code, or `None` when it is a value.
    pub fn query_error(&mut self, source: &str) -> Option<String> {
        value::error_code(&self.query(source))
    }

    /// The error code of the answer to a request already sent and cancelled.
    pub fn query_error_after_cancel(&mut self) -> Option<String> {
        value::error_code(&self.recv())
    }

    pub fn cancel(&self) {
        socket2::SockRef::from(&self.stream)
            .send_out_of_band(b"!")
            .unwrap();
    }

    pub fn peer(&self) -> SocketAddr {
        self.stream.peer_addr().unwrap()
    }
}

/// A GET over a connection of its own; the status and the body.
pub fn http_get(port: u16, path: &str, password: Option<&str>) -> (u16, String) {
    use base64::Engine;
    let auth = password.map_or_else(String::new, |p| {
        let token = base64::engine::general_purpose::STANDARD.encode(format!("admin:{p}"));
        format!("Authorization: Basic {token}\r\n")
    });
    let mut stream = connect(port).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: rfbouncer\r\n{auth}Connection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response.split(' ').nth(1).unwrap().parse().unwrap();
    let body = response
        .split_once("\r\n\r\n")
        .map_or("", |(_, body)| body)
        .to_owned();
    (status, body)
}

pub fn http_json(port: u16, path: &str, password: &str) -> serde_json::Value {
    let (status, body) = http_get(port, path, Some(password));
    assert_eq!(status, 200, "{path}: {body}");
    serde_json::from_str(&body).unwrap()
}

/// The payload of an i64 atom, which is what most test queries answer.
pub fn int(n: i64) -> Vec<u8> {
    let mut out = vec![0xfb, 0];
    out.extend_from_slice(&n.to_le_bytes());
    out
}

/// Waits until `check` holds, or fails after ten seconds.
pub fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !check() {
        assert!(Instant::now() < deadline, "never happened: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}
