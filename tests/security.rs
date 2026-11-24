//! TLS on the one port, an address of its own for administration, and the
//! lockout of an address that keeps presenting wrong credentials.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rfbouncer::wire::frame::{self, HEADER_LEN, Header, MSG_SYNC, PAYLOAD_MAX};
use rfbouncer::wire::value;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use support::{
    Bouncer, Client, Rayforce, Refused, eventually, free_port, http_get, int, pool, slow, wire,
};

const OPEN: &str = "require_restricted = false";
const ADMIN: (&str, &str) = ("rfbouncer", "root");

type Tls = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

/// A throwaway certificate for `localhost` and its key, both in PEM.
fn certificate() -> (String, String) {
    let made = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    (made.cert.pem(), made.signing_key.serialize_pem())
}

/// A directory with a certificate and a key in it, removed when dropped.
struct Files(PathBuf);

impl Files {
    fn new(cert: &str, key: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rfbouncer-tls-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let files = Self(dir);
        files.write(cert, key);
        files
    }

    fn write(&self, cert: &str, key: &str) {
        std::fs::write(self.0.join("cert.pem"), cert).unwrap();
        std::fs::write(self.0.join("key.pem"), key).unwrap();
    }

    /// The `[tls]` section that names the two files, with `extra` lines.
    fn section(&self, extra: &str) -> String {
        format!(
            "[tls]\ncert = \"{0}/cert.pem\"\nkey = \"{0}/key.pem\"\n{extra}\n",
            self.0.display()
        )
    }
}

impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A TLS connection that trusts `cert` alone, with the handshake done: a
/// certificate that is not that one is an error here.
fn tls(port: u16, cert: &str) -> std::io::Result<Tls> {
    let mut roots = rustls::RootCertStore::empty();
    for der in CertificateDer::pem_slice_iter(cert.as_bytes()) {
        roots.add(der.unwrap()).unwrap();
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = "localhost".try_into().unwrap();
    let conn = rustls::ClientConnection::new(Arc::new(config), name).unwrap();
    let sock = support::connect(port)?;
    sock.set_read_timeout(Some(Duration::from_secs(30)))?;
    sock.set_nodelay(true)?;
    let mut stream = rustls::StreamOwned::new(conn, sock);
    while stream.conn.is_handshaking() {
        stream.conn.complete_io(&mut stream.sock)?;
    }
    Ok(stream)
}

/// A GET through the tunnel; the status and the body.
fn https_get(port: u16, cert: &str, path: &str, password: Option<&str>) -> (u16, String) {
    use base64::Engine;
    let auth = password.map_or_else(String::new, |p| {
        let token = base64::engine::general_purpose::STANDARD.encode(format!("admin:{p}"));
        format!("Authorization: Basic {token}\r\n")
    });
    let mut stream = tls(port, cert).unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\n{auth}Connection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response.split(' ').nth(1).unwrap().parse().unwrap();
    let body = response.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    (status, body.to_owned())
}

/// The greeting, as a Rayforce client makes it; whether it was let in.
fn greet<S: Read + Write>(stream: &mut S, credentials: (&str, &str)) -> bool {
    stream.write_all(&[wire(), 0]).unwrap();
    let mut hello = [0u8; 2];
    stream.read_exact(&mut hello).unwrap();
    assert_eq!(hello[0], wire());
    if hello[1] & 1 == 0 {
        return true;
    }
    let text = format!("{}:{}\0", credentials.0, credentials.1);
    stream.write_all(&[text.len() as u8]).unwrap();
    stream.write_all(text.as_bytes()).unwrap();
    let mut verdict = [1u8; 1];
    stream.read_exact(&mut verdict).unwrap();
    verdict[0] == 0
}

fn send<S: Write>(stream: &mut S, source: &str) {
    let request = frame::frame(wire(), MSG_SYNC, &value::str_atom(source));
    stream.write_all(&request).unwrap();
}

fn recv<S: Read>(stream: &mut S) -> Vec<u8> {
    let mut raw = [0u8; HEADER_LEN];
    stream.read_exact(&mut raw).unwrap();
    let header = Header::parse(&raw, wire(), PAYLOAD_MAX).unwrap();
    let mut payload = vec![0u8; header.size];
    stream.read_exact(&mut payload).unwrap();
    payload
}

fn login(port: u16, credentials: (&str, &str)) -> Result<Client, Refused> {
    Client::connect_as(port, wire(), Some(credentials))
}

fn busy(bouncer: &Bouncer) -> usize {
    let live = bouncer.live();
    let backends = live["pools"][0]["backends"].as_array().unwrap();
    backends.iter().filter(|b| b["state"] == "busy").count()
}

#[test]
fn https_and_ipc_over_tls_share_the_port_with_clear_text() {
    let replica = Rayforce::start(&[]);
    let (cert, key) = certificate();
    let files = Files::new(&cert, &key);
    let body = format!(
        "{}{}",
        pool(&[&replica], &format!("{OPEN}\npassword = \"pw\"")),
        files.section("")
    );
    let bouncer = Bouncer::start(&body);

    assert_eq!(https_get(bouncer.port, &cert, "/healthz", None).0, 200);
    assert_eq!(https_get(bouncer.port, &cert, "/api/live", None).0, 401);
    let (status, live) = https_get(bouncer.port, &cert, "/api/live", Some("root"));
    assert_eq!(status, 200);
    assert!(live.contains(&replica.addr()), "{live}");

    let mut client = tls(bouncer.port, &cert).unwrap();
    assert!(greet(&mut client, ("app", "pw")));
    send(&mut client, "(+ 1 2)");
    assert_eq!(recv(&mut client), int(3));
    // A second request finds the tunnel where the first left it.
    send(&mut client, "(+ 2 2)");
    assert_eq!(recv(&mut client), int(4));
    assert!(!greet(
        &mut tls(bouncer.port, &cert).unwrap(),
        ("app", "wrong")
    ));

    let mut console = tls(bouncer.port, &cert).unwrap();
    assert!(greet(&mut console, ADMIN));
    send(&mut console, "show pools");
    let pools = String::from_utf8_lossy(&recv(&mut console)).into_owned();
    assert!(pools.contains("main"), "{pools}");

    // Clear text is still served: Rayforce's own client speaks nothing else.
    assert_eq!(
        login(bouncer.port, ("app", "pw")).unwrap().query("(+ 1 2)"),
        int(3)
    );
    assert_eq!(http_get(bouncer.port, "/api/live", Some("root")).0, 200);
    // A client that does not trust the certificate does not get through.
    assert!(tls(bouncer.port, &certificate().0).is_err());
}

#[test]
fn clear_text_is_refused_where_tls_is_required() {
    let replica = Rayforce::start(&[]);
    let (cert, key) = certificate();
    let files = Files::new(&cert, &key);
    let body = |ipc: bool| {
        format!(
            "admin_password = \"root\"\n{}{}",
            pool(&[&replica], OPEN),
            files.section(&format!("require_for_http = true\nrequire_for_ipc = {ipc}"))
        )
    };
    let bouncer = Bouncer::start_as_is(&body(false));
    eventually("a replica is up", || {
        http_get(bouncer.port, "/healthz", None).0 == 200
    });

    // The pages are for HTTPS alone; the health check has nothing to hide.
    assert_eq!(http_get(bouncer.port, "/api/live", Some("root")).0, 403);
    assert_eq!(http_get(bouncer.port, "/metrics", Some("root")).0, 403);
    assert_eq!(
        https_get(bouncer.port, &cert, "/api/live", Some("root")).0,
        200
    );
    // IPC in clear goes on, as Rayforce's own client needs.
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 2)"), int(3));

    bouncer.reload(&body(true));
    eventually("clear-text IPC is refused", || {
        matches!(
            Client::connect_as(bouncer.port, wire(), None),
            Err(Refused::Closed)
        )
    });
    let mut client = tls(bouncer.port, &cert).unwrap();
    assert!(greet(&mut client, ("", "")));
    send(&mut client, "(+ 1 2)");
    assert_eq!(recv(&mut client), int(3));
    assert_eq!(http_get(bouncer.port, "/healthz", None).0, 200);
    let (_, metrics) = https_get(bouncer.port, &cert, "/metrics", Some("root"));
    let refused = metrics
        .lines()
        .find_map(|l| l.strip_prefix("rfbouncer_cleartext_refused_total "))
        .unwrap();
    assert!(refused.parse::<f64>().unwrap() >= 3.0, "{refused}");
}

#[test]
fn a_reload_takes_the_new_certificate() {
    let replica = Rayforce::start(&[]);
    let (old, old_key) = certificate();
    let (new, new_key) = certificate();
    let files = Files::new(&old, &old_key);
    let body = format!("{}{}", pool(&[&replica], OPEN), files.section(""));
    let bouncer = Bouncer::start(&body);
    assert!(tls(bouncer.port, &old).is_ok());
    assert!(tls(bouncer.port, &new).is_err());

    files.write(&new, &new_key);
    bouncer.reload(&format!("admin_password = \"root\"\n{body}"));
    eventually("the new certificate is served", || {
        tls(bouncer.port, &new).is_ok()
    });
    assert!(tls(bouncer.port, &old).is_err());
}

#[test]
fn a_certificate_that_cannot_be_served_fails_the_check() {
    let (cert, key) = certificate();
    let files = Files::new(&cert, &key);
    let config = files.0.join("rfbouncer.toml");
    let check = |section: &str| {
        let text = format!(
            "listen = \"127.0.0.1:1\"\n[[pool]]\nname = \"main\"\nbackends = [\"h:1\"]\n{section}"
        );
        std::fs::write(&config, text).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_rfbouncer"))
            .arg("--check")
            .arg("--config")
            .arg(&config)
            .output()
            .unwrap();
        let said = String::from_utf8_lossy(&output.stderr).into_owned();
        (output.status.success(), said)
    };
    assert_eq!(check(&files.section("")), (true, String::new()));

    // A key that is not the certificate's.
    files.write(&cert, &certificate().1);
    let (ok, said) = check(&files.section(""));
    assert!(!ok && said.contains("key.pem"), "{said}");

    files.write("not a certificate\n", &key);
    let (ok, said) = check(&files.section(""));
    assert!(!ok && said.contains("cert.pem"), "{said}");

    files.write(&cert, "");
    let (ok, said) = check(&files.section(""));
    assert!(!ok && said.contains("key.pem"), "{said}");

    let (ok, said) = check("[tls]\ncert = \"/nowhere/cert.pem\"\nkey = \"/nowhere/key.pem\"\n");
    assert!(!ok && said.contains("/nowhere/cert.pem"), "{said}");
}

#[test]
fn over_tls_a_request_is_cancelled_beneath_the_tunnel_or_by_leaving() {
    let replica = Rayforce::start(&[]);
    let (cert, key) = certificate();
    let files = Files::new(&cert, &key);
    let body = format!(
        "{}{}",
        pool(&[&replica], &format!("{OPEN}\nquery_timeout = \"0\"")),
        files.section("")
    );
    let bouncer = Bouncer::start(&body);

    // The cancel byte is TCP's urgent data: it goes on the socket itself,
    // beside the tunnel, and the tunnel is whole afterwards.
    let mut client = tls(bouncer.port, &cert).unwrap();
    assert!(greet(&mut client, ("", "")));
    send(&mut client, &slow(4_000_000));
    bouncer.wait_busy(1);
    socket2::SockRef::from(&client.sock)
        .send_out_of_band(b"!")
        .unwrap();
    assert_eq!(
        value::error_code(&recv(&mut client)),
        Some("cancel".to_owned())
    );
    send(&mut client, "(+ 1 2)");
    assert_eq!(recv(&mut client), int(3));

    // A client that cannot reach its socket cancels by leaving.
    send(&mut client, &slow(4_000_000));
    bouncer.wait_busy(1);
    drop(client);
    eventually("the replica is free again", || busy(&bouncer) == 0);
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 2)"), int(3));
}

#[test]
fn administration_has_an_address_of_its_own() {
    let replica = Rayforce::start(&[]);
    let start = |extra: &str| {
        let admin = free_port();
        let body = format!(
            "admin_listen = \"127.0.0.1:{admin}\"\nadmin_password = \"root\"\n{}",
            pool(&[&replica], &format!("{OPEN}\n{extra}"))
        );
        let bouncer = Bouncer::start_as_is(&body);
        eventually("the admin address answers and a replica is up", || {
            support::connect(admin).is_ok() && http_get(admin, "/healthz", None).0 == 200
        });
        (bouncer, admin)
    };
    let (bouncer, admin) = start("password = \"pw\"");

    // The clients' port: the pool and the health check, nothing else.
    assert_eq!(
        login(bouncer.port, ("app", "pw")).unwrap().query("(+ 1 2)"),
        int(3)
    );
    assert!(matches!(
        login(bouncer.port, ADMIN),
        Err(Refused::Credentials)
    ));
    assert_eq!(
        http_get(bouncer.port, "/healthz", None),
        (200, "ok\n".to_owned())
    );
    for path in ["/", "/api/live", "/metrics"] {
        assert_eq!(http_get(bouncer.port, path, Some("root")).0, 404, "{path}");
    }

    // The admin address: the console and the pages, and no pool.
    let pools = login(admin, ADMIN).unwrap().query("show pools");
    assert!(String::from_utf8_lossy(&pools).contains("main"));
    assert!(matches!(
        login(admin, ("app", "pw")),
        Err(Refused::Credentials)
    ));
    assert_eq!(http_get(admin, "/api/live", Some("root")).0, 200);
    assert_eq!(http_get(admin, "/api/live", Some("wrong")).0, 401);
    let (status, metrics) = http_get(admin, "/metrics", Some("root"));
    assert_eq!(status, 200);
    // Both refusals above were logins, on whichever address they came.
    assert!(
        metrics.contains("rfbouncer_auth_failures_total 3"),
        "{metrics}"
    );
    drop(bouncer);

    // A pool that asks its clients for nothing: the admin address still
    // asks, so the console is there without giving the pool a password.
    let (bouncer, admin) = start("");
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 2)"), int(3));
    let pools = login(admin, ADMIN).unwrap().query("show pools");
    assert!(String::from_utf8_lossy(&pools).contains("main"));
    assert!(matches!(
        Client::connect_as(admin, wire(), None),
        Err(Refused::Credentials)
    ));
}

#[test]
fn an_address_that_keeps_failing_is_locked_out_for_a_while() {
    let replica = Rayforce::start(&[]);
    let body = |failures: u32| {
        format!(
            "auth_lockout_failures = {failures}\nauth_lockout_period = \"4s\"\nadmin_password = \"root\"\n{}",
            pool(&[&replica], &format!("{OPEN}\npassword = \"pw\""))
        )
    };
    let bouncer = Bouncer::start(&body(2));
    let refused = |port, credentials| matches!(login(port, credentials), Err(Refused::Credentials));

    // Two wrong passwords over IPC, and the right one is refused as well,
    // over IPC and over HTTP.
    let mut before = login(bouncer.port, ("app", "pw")).unwrap();
    assert!(refused(bouncer.port, ("app", "wrong")));
    assert!(refused(bouncer.port, (ADMIN.0, "wrong")));
    assert!(refused(bouncer.port, ("app", "pw")));
    assert!(refused(bouncer.port, ADMIN));
    assert_eq!(http_get(bouncer.port, "/metrics", Some("root")).0, 429);
    // What needs no login is as it was, and so is who was already in.
    assert_eq!(http_get(bouncer.port, "/healthz", None).0, 200);
    assert_eq!(http_get(bouncer.port, "/", None).0, 401);
    assert_eq!(before.query("(+ 1 2)"), int(3));

    eventually("the period ends", || {
        http_get(bouncer.port, "/metrics", Some("root")).0 == 200
    });
    assert_eq!(bouncer.metric("rfbouncer_auth_lockouts_total"), 1.0);
    assert_eq!(bouncer.metric("rfbouncer_auth_locked_addresses"), 0.0);
    assert!(bouncer.metric("rfbouncer_auth_locked_out_total") >= 3.0);
    assert_eq!(bouncer.metric("rfbouncer_auth_failures_total"), 2.0);
    assert_eq!(
        login(bouncer.port, ("app", "pw")).unwrap().query("(+ 1 2)"),
        int(3)
    );

    // Two wrong passwords over HTTP lock the address out of IPC too.
    assert_eq!(http_get(bouncer.port, "/metrics", Some("wrong")).0, 401);
    assert_eq!(http_get(bouncer.port, "/metrics", Some("wrong")).0, 401);
    assert_eq!(http_get(bouncer.port, "/metrics", Some("root")).0, 429);
    assert!(refused(bouncer.port, ("app", "pw")));

    // Zero turns it off, for those locked out as for everyone.
    bouncer.reload(&body(0));
    eventually("the lockout is lifted", || {
        login(bouncer.port, ("app", "pw")).is_ok()
    });
    for _ in 0..3 {
        assert_eq!(http_get(bouncer.port, "/metrics", Some("wrong")).0, 401);
    }
    assert_eq!(bouncer.metric("rfbouncer_auth_lockouts_total"), 2.0);
}

#[test]
fn a_client_half_way_into_tls_at_a_stop_is_still_served() {
    let (cert, key) = certificate();
    let files = Files::new(&cert, &key);
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&format!("{}{}", files.section(""), pool(&[&replica], OPEN)));
    // The first flight of a handshake is sent and its answer not yet read
    // when the stop comes: the bouncer knows no client here, only a
    // connection it has accepted.
    let mut roots = rustls::RootCertStore::empty();
    for der in CertificateDer::pem_slice_iter(cert.as_bytes()) {
        roots.add(der.unwrap()).unwrap();
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = "localhost".try_into().unwrap();
    let mut conn = rustls::ClientConnection::new(Arc::new(config), name).unwrap();
    let mut sock = support::connect(bouncer.port).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    conn.write_tls(&mut sock).unwrap();
    support::signal(bouncer.pid(), "TERM");
    eventually("the port is closed", || {
        support::connect(bouncer.port).is_err()
    });
    let mut stream = rustls::StreamOwned::new(conn, sock);
    while stream.conn.is_handshaking() {
        stream.conn.complete_io(&mut stream.sock).unwrap();
    }
    assert!(greet(&mut stream, ("", "")));
    send(&mut stream, "(+ 1 2)");
    assert_eq!(recv(&mut stream), int(3));
}

#[test]
fn a_restart_on_shared_ports_hands_over_the_admin_address_too() {
    let replica = Rayforce::start(&[]);
    let admin = free_port();
    let mut old = Bouncer::start_as_is(&format!(
        "reuse_port = true\nadmin_listen = \"127.0.0.1:{admin}\"\nadmin_password = \"root\"\n{}",
        pool(&[&replica], OPEN)
    ));
    eventually("the admin address answers and the replica is up", || {
        support::connect(admin).is_ok() && http_get(admin, "/healthz", None).0 == 200
    });
    // The new process opens both addresses while the old one holds them.
    let _new = old.start_beside();
    support::signal(old.pid(), "TERM");
    eventually("the old bouncer exited", || old.exited().is_some());
    // And is alone on both afterwards.
    eventually("the new bouncer has reached the replica", || {
        http_get(admin, "/healthz", None).0 == 200
    });
    assert_eq!(http_get(admin, "/metrics", Some("root")).0, 200);
    assert_eq!(Client::connect(old.port).query("(+ 1 2)"), int(3));
}
