//! The outcomes that are the bouncer's own verdict and not a replica's
//! answer (`io`, `timeout`, `busy`), from the client that is told to the
//! report that counts them.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::io::Read;

use support::fake::{Fake, read_request};
use support::{Bouncer, Client, Rayforce, http_json};

#[test]
fn what_the_bouncer_answers_for_a_replica_is_counted_under_its_own_name() {
    // The first connection dies under its request; every later one takes
    // a request and never answers, until the bouncer gives up on it.
    let replica = Fake::start(|mut stream, n| {
        if read_request(&mut stream) && n > 0 {
            let _ = stream.read(&mut [0u8; 1]);
        }
    });
    let store = Rayforce::start(&["-u", "s"]);
    let bouncer = Bouncer::start(&format!(
        "[stats]\nrayforce = \"{}\"\npassword = \"s\"\n[[pool]]\nname = \"main\"\n\
         require_restricted = false\nretry = false\nqueue_max = 1\nhealth_check_timeout = \"0\"\n\
         query_timeout = \"2s\"\ncancel_grace = \"100ms\"\nbackends = [\"{}\"]\n",
        store.addr(),
        replica.addr()
    ));

    let mut lost = Client::connect(bouncer.port);
    assert_eq!(lost.query_error("(lost 1)"), Some("io".to_owned()));

    let mut late = Client::connect(bouncer.port);
    late.send("(late 1)");
    bouncer.wait_busy(1);
    let mut waiting = Client::connect(bouncer.port);
    waiting.send("(waiting 1)");
    bouncer.wait_queued(1);
    let mut refused = Client::connect(bouncer.port);
    assert_eq!(refused.query_error("(refused 1)"), Some("busy".to_owned()));
    // Out of the queue before its turn, so only one request runs out its
    // two seconds.
    waiting.cancel();
    assert_eq!(
        waiting.query_error_after_cancel(),
        Some("cancel".to_owned())
    );
    assert_eq!(late.query_error_after_cancel(), Some("timeout".to_owned()));

    let errors = http_json(bouncer.port, "/api/errors", "root");
    let count_of = |status: &str| {
        let outcomes = errors["outcomes"].as_array().unwrap();
        outcomes
            .iter()
            .find(|o| o["status"] == status)
            .map_or(0, |o| o["count"].as_u64().unwrap())
    };
    for status in ["io", "busy", "cancel", "timeout"] {
        assert_eq!(count_of(status), 1, "{status} in {errors}");
    }
    assert_eq!(count_of("ok"), 0, "{errors}");
    // Each is kept as a failed request under the same name, with its text.
    let rows = errors["rows"].as_array().unwrap();
    for (status, text) in [
        ("io", "(lost 1)"),
        ("busy", "(refused 1)"),
        ("timeout", "(late 1)"),
    ] {
        assert!(
            rows.iter()
                .any(|r| r["status"] == status && r["text"] == text),
            "no {status} sample of {text} in {errors}"
        );
    }
    // And in the totals the page leads with.
    let overview = http_json(bouncer.port, "/api/overview", "root");
    let total = &overview["total"];
    assert_eq!(
        (
            &total["failed"],
            &total["refused"],
            &total["timeouts"],
            &total["cancels"]
        ),
        (&1.into(), &1.into(), &1.into(), &1.into()),
        "{total}"
    );
}
