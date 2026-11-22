//! What the integration tests share: real Rayforce processes, the bouncer's
//! own binary, and a small blocking client that speaks the wire.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

pub mod fake;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rfbouncer::wire::frame::{self, HEADER_LEN, Header, MSG_SYNC, PAYLOAD_MAX};
use rfbouncer::wire::value;

/// A query that keeps a replica's thread for about `n / 7500` seconds on the
/// machine these tests were written on, and stops at a cancel within
/// milliseconds. Its answer is `n`.
pub fn slow(n: u32) -> String {
    format!("(count (map (fn [i] (sum (til 1000000))) (til {n})))")
}

pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn wait_for_port(port: u16, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
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
        let child = Command::new(bin)
            .arg("-p")
            .arg(port.to_string())
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        wait_for_port(port, "rayforce");
        Self { child, port }
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
    let mut child = Command::new(rayforce_bin())
        .arg("-f")
        .arg(&path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
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
    /// configuration, which is everything but `listen`.
    pub fn start(body: &str) -> Self {
        let port = free_port();
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rfbouncer-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = dir.join("rfbouncer.toml");
        let bouncer = Self {
            child: Command::new(env!("CARGO_BIN_EXE_rfbouncer"))
                .arg("--config")
                .arg(&config)
                .stdin(Stdio::null())
                .stderr(if std::env::var_os("RFBOUNCER_TEST_LOG").is_some() {
                    Stdio::inherit()
                } else {
                    Stdio::null()
                })
                .spawn_after(|| {
                    std::fs::write(&config, format!("listen = \"127.0.0.1:{port}\"\n{body}"))
                        .unwrap()
                }),
            port,
            config,
        };
        wait_for_port(port, "rfbouncer");
        bouncer
    }

    /// Replaces the configuration and has the bouncer read it again.
    pub fn reload(&self, body: &str) {
        std::fs::write(
            &self.config,
            format!("listen = \"127.0.0.1:{}\"\n{body}", self.port),
        )
        .unwrap();
        signal(self.child.id(), "HUP");
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

trait SpawnAfter {
    fn spawn_after(&mut self, before: impl FnOnce()) -> Child;
}

impl SpawnAfter for Command {
    fn spawn_after(&mut self, before: impl FnOnce()) -> Child {
        before();
        self.spawn().unwrap()
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
        Self::connect_as(port, 4, None).unwrap()
    }

    /// Opens a connection the way a Rayforce client does. `credentials` are
    /// sent only if the server asks for them.
    pub fn connect_as(
        port: u16,
        version: u8,
        credentials: Option<(&str, &str)>,
    ) -> Result<Self, Refused> {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
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
