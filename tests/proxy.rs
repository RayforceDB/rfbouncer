//! A client behind the bouncer gets what it would get from a replica.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::io::Write;

use rfbouncer::wire::frame::{self, MSG_RESPONSE};
use support::fake::{Fake, gate, read_request};
use support::{Bouncer, Client, Rayforce, Refused, int, pool, slow};

#[test]
fn an_answer_through_the_bouncer_is_the_replicas_own_bytes() {
    let replica = Rayforce::start(&["-U", "pw"]);
    let bouncer = Bouncer::start(&pool(&[&replica], "backend_password = \"pw\""));
    let mut direct = Client::connect_as(replica.port, support::wire(), Some(("t", "pw"))).unwrap();
    let mut through = Client::connect(bouncer.port);
    for source in [
        "(+ 1 2)",
        "\"text\"",
        "(table [a b] (list [1 2] [x y]))",
        "(nosuch 1)",
        "(set x 1)",
        "(til 200000)",
    ] {
        direct.send(source);
        through.send(source);
        let (want_header, want) = direct.try_recv().unwrap();
        let (got_header, got) = through.try_recv().unwrap();
        assert_eq!(got_header, want_header, "{source}");
        assert!(got == want, "{source}: payloads differ");
    }
}

#[test]
fn a_slow_query_on_one_replica_does_not_hold_the_others() {
    let replicas = [
        Rayforce::start(&[]),
        Rayforce::start(&[]),
        Rayforce::start(&[]),
    ];
    let all: Vec<&Rayforce> = replicas.iter().collect();
    let bouncer = Bouncer::start(&pool(&all, "require_restricted = false"));
    let mut hog = Client::connect(bouncer.port);
    hog.send(&slow(60_000));
    bouncer.wait_busy(1);

    // A connection per query, as a client without a pool of its own does it.
    for i in 0..40 {
        let mut c = Client::connect(bouncer.port);
        assert_eq!(c.query(&format!("(+ {i} 1)")), int(i + 1));
    }

    // The slow one is still there to be cancelled, so none of the short
    // queries waited behind it: no clock is needed to say so.
    hog.cancel();
    assert_eq!(hog.query_error_after_cancel(), Some("cancel".to_owned()));
    assert_eq!(hog.query("(+ 1 1)"), int(2));
}

#[test]
fn requests_queue_when_every_replica_is_busy() {
    // The replica answers its requests with 1, 2, ... and each when the test
    // says so: the first is certain to be running when the second comes.
    let (answer, held) = gate();
    let replica = Fake::start(move |mut stream, _| {
        let mut n = 0;
        while read_request(&mut stream) && held.wait() {
            n += 1;
            let _ = stream.write_all(&frame::frame(support::wire(), MSG_RESPONSE, &int(n)));
        }
    });
    let bouncer = Bouncer::start(&format!(
        "[[pool]]\nname = \"main\"\nrequire_restricted = false\nbackends = [\"{}\"]\n",
        replica.addr()
    ));
    let mut first = Client::connect(bouncer.port);
    first.send("(+ 1 1)");
    bouncer.wait_busy(1);
    let mut second = Client::connect(bouncer.port);
    second.send("(+ 2 2)");
    bouncer.wait_queued(1);
    answer.send(()).unwrap();
    assert_eq!(first.recv(), int(1));
    answer.send(()).unwrap();
    assert_eq!(second.recv(), int(2));
}

#[test]
fn one_client_may_send_requests_ahead_and_is_answered_in_order() {
    let replicas = [Rayforce::start(&[]), Rayforce::start(&[])];
    let all: Vec<&Rayforce> = replicas.iter().collect();
    let bouncer = Bouncer::start(&pool(&all, "require_restricted = false"));
    let mut c = Client::connect(bouncer.port);
    for i in 0..20 {
        c.send(&format!("(* {i} 2)"));
    }
    for i in 0..20 {
        assert_eq!(c.recv(), int(i * 2));
    }
}

#[test]
fn credentials_choose_the_pool() {
    let a = Rayforce::start(&[]);
    let b = Rayforce::start(&[]);
    let mut seed = Client::connect(a.port);
    seed.query("(set who 1)");
    let mut seed = Client::connect(b.port);
    seed.query("(set who 2)");
    let bouncer = Bouncer::start(&format!(
        "[[pool]]\nname = \"a\"\nusers = [\"alice\"]\npassword = \"pa\"\nrequire_restricted = false\nbackends = [\"{}\"]\n\
         [[pool]]\nname = \"b\"\ndefault = true\nrequire_restricted = false\nbackends = [\"{}\"]\n",
        a.addr(),
        b.addr()
    ));
    let mut alice =
        Client::connect_as(bouncer.port, support::wire(), Some(("alice", "pa"))).unwrap();
    assert_eq!(alice.query("who"), int(1));
    let mut other =
        Client::connect_as(bouncer.port, support::wire(), Some(("bob", "anything"))).unwrap();
    assert_eq!(other.query("who"), int(2));
    assert!(matches!(
        Client::connect_as(bouncer.port, support::wire(), Some(("alice", "wrong"))),
        Err(Refused::Credentials)
    ));
    assert!(matches!(
        Client::connect_as(bouncer.port, support::other_wire(), Some(("alice", "pa"))),
        Err(Refused::Version(v)) if v == support::wire()
    ));
    assert!(matches!(
        Client::connect_as(bouncer.port, 9, None),
        Err(Refused::Version(v)) if v == support::wire()
    ));
}

#[test]
fn a_replica_that_takes_writes_is_kept_out_of_the_pool() {
    let open = Rayforce::start(&[]);
    let restricted = Rayforce::start(&["-U", "pw"]);
    let bouncer = Bouncer::start(&pool(
        &[&open, &restricted],
        "backend_password = \"pw\"\nquery_wait_timeout = \"1s\"",
    ));
    // Every request lands on the restricted one, so every write is refused.
    for _ in 0..6 {
        let mut c = Client::connect(bouncer.port);
        assert_eq!(c.query_error("(set leaked 1)"), Some("access".to_owned()));
    }
    let mut direct = Client::connect(open.port);
    assert_eq!(direct.query_error("leaked"), Some("name".to_owned()));
}

#[test]
fn what_is_not_a_request_closes_the_connection() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], "require_restricted = false"));
    let mut c = Client::connect(bouncer.port);
    c.send_frame(0, &rfbouncer::wire::value::str_atom("(set x 1)"));
    assert!(c.try_recv().is_err());
    let mut direct = Client::connect(replica.port);
    assert_eq!(direct.query_error("x"), Some("name".to_owned()));
}

#[test]
fn a_request_past_the_size_limit_closes_the_connection() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&format!(
        "max_request_bytes = \"1KiB\"\n{}",
        pool(&[&replica], "require_restricted = false")
    ));
    let mut c = Client::connect(bouncer.port);
    assert_eq!(c.query("(+ 1 2)"), int(3));
    c.send(&format!("(count \"{}\")", "x".repeat(2000)));
    assert!(c.try_recv().is_err());
}
