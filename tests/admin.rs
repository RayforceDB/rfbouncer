//! The admin console, read with Rayforce's own client.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::time::Duration;

use support::{Bouncer, Client, Rayforce, Refused, eventually, int, pool, rayforce_eval, slow};

const ADMIN: &str = "admin_password = \"root\"\n";
const OPEN: &str = "require_restricted = false\npassword = \"pw\"";

fn admin(bouncer: &Bouncer) -> Client {
    Client::connect_as(bouncer.port, support::wire(), Some(("rfbouncer", "root"))).unwrap()
}

fn user(bouncer: &Bouncer) -> Client {
    Client::connect_as(bouncer.port, support::wire(), Some(("app", "pw"))).unwrap()
}

fn text(payload: &[u8]) -> String {
    String::from_utf8_lossy(payload).into_owned()
}

#[test]
fn a_rayforce_client_reads_every_table() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&format!("{ADMIN}{}", pool(&[&replica], OPEN)));
    let mut c = user(&bouncer);
    assert_eq!(c.query("(+ 1 2)"), int(3));
    assert_eq!(c.query_error("(nosuch 1)"), Some("name".to_owned()));
    let out = rayforce_eval(&format!(
        "(set h (.ipc.open \"127.0.0.1:{}:rfbouncer:root\"))\n\
         (set pools (.ipc.send h \"show pools\"))\n\
         (show pools)\n\
         (println (at pools 'requests) (at pools 'ok) (at pools 'errors) (at pools 'up))\n\
         (show (.ipc.send h \"show servers\"))\n\
         (show (.ipc.send h \"show clients\"))\n\
         (show (.ipc.send h \"show stats\"))\n\
         (show (.ipc.send h \"show config\"))\n\
         (println (.ipc.send h \"reconnect\"))\n",
        bouncer.port
    ));
    assert!(out.contains("[2] [1] [1] [1]"), "{out}");
    for expected in [
        "main",
        "idle",
        &replica.addr(),
        "app",
        "uptime_s",
        "max_client_conn",
        "ok",
    ] {
        assert!(out.contains(expected), "no {expected:?} in:\n{out}");
    }
    assert!(!out.contains("error:"), "{out}");
}

#[test]
fn only_the_admin_password_opens_the_console() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&format!("{ADMIN}{}", pool(&[&replica], OPEN)));
    assert!(matches!(
        Client::connect_as(bouncer.port, support::wire(), Some(("rfbouncer", "pw"))),
        Err(Refused::Credentials)
    ));
    let mut a = admin(&bouncer);
    assert_eq!(a.query_error("drop everything"), Some("admin".to_owned()));
    assert_eq!(a.query_error("drain 127.0.0.1:1"), Some("admin".to_owned()));
    // The console is not a way to the replicas.
    assert_eq!(a.query_error("(+ 1 2)"), Some("admin".to_owned()));
}

#[test]
fn a_busy_replica_shows_what_it_runs() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&format!("{ADMIN}{}", pool(&[&replica], OPEN)));
    let mut c = user(&bouncer);
    c.send(&slow(60_000));
    let mut a = admin(&bouncer);
    eventually("the request shows as running", || {
        let servers = text(&a.query("show servers"));
        servers.contains("busy") && servers.contains(&slow(60_000))
    });
    c.cancel();
    c.recv();
    eventually("the replica shows as idle", || {
        text(&a.query("show servers")).contains("idle")
    });
}

#[test]
fn a_paused_pool_holds_requests_until_resumed() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&format!("{ADMIN}{}", pool(&[&replica], OPEN)));
    let mut a = admin(&bouncer);
    a.query("pause main");
    let mut c = user(&bouncer);
    c.send("(+ 1 2)");
    c.stream
        .set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    assert!(c.try_recv().is_err(), "a paused pool answered");
    c.stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    a.query("resume");
    assert_eq!(c.recv(), int(3));
}

#[test]
fn a_drained_replica_gets_no_more_requests() {
    let a = Rayforce::start(&[]);
    let b = Rayforce::start(&[]);
    Client::connect(a.port).query("(set who 1)");
    Client::connect(b.port).query("(set who 2)");
    let bouncer = Bouncer::start(&format!("{ADMIN}{}", pool(&[&a, &b], OPEN)));
    let mut console = admin(&bouncer);
    console.query(&format!("drain {}", a.addr()));
    for _ in 0..10 {
        assert_eq!(user(&bouncer).query("who"), int(2));
    }
    console.query(&format!("undrain {}", a.addr()));
    eventually("the drained replica serves again", || {
        user(&bouncer).query("who") == int(1)
    });
}

#[test]
fn shutdown_stops_the_process() {
    let replica = Rayforce::start(&[]);
    let mut bouncer = Bouncer::start(&format!("{ADMIN}{}", pool(&[&replica], OPEN)));
    admin(&bouncer).query("shutdown");
    eventually("the bouncer exited", || bouncer.exited().is_some());
}
