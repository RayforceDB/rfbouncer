//! A statistics store that answers with what no bouncer wrote. The store
//! is a process of its own that several bouncers and any client with its
//! password write to; a report over it shows what is there or says it
//! cannot, and the page asking is answered either way.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::io::Write;

use rfbouncer::wire::frame::{self, MSG_RESPONSE};
use rfbouncer::wire::value::{Column, table};
use support::fake::{Fake, answer_forever, read_request};
use support::{Bouncer, http_get, http_json};

/// A bouncer whose store answers every request with `answer`.
fn with_store(answer: Vec<u8>) -> (Bouncer, [Fake; 2]) {
    let store = Fake::start(move |mut stream, _| {
        while read_request(&mut stream) {
            let _ = stream.write_all(&frame::frame(support::wire(), MSG_RESPONSE, &answer));
        }
    });
    let replica = Fake::start(|stream, _| answer_forever(stream, 1));
    let bouncer = Bouncer::start(&format!(
        "[stats]\nrayforce = \"{}\"\n[[pool]]\nname = \"main\"\nrequire_restricted = false\nbackends = [\"{}\"]\n",
        store.addr(),
        replica.addr()
    ));
    (bouncer, [store, replica])
}

#[test]
fn counts_that_do_not_fit_are_shown_at_the_most_a_count_can_be() {
    // Two steps, each with counts of -1: read as unsigned, either is the
    // largest count there is, and their sum is past it. The property
    // `counts_of_any_size_have_a_quantile` found the first such sum.
    let wild = || Column::I64(vec![-1, -1]);
    let (bouncer, _fakes) = with_store(table(&[
        ("k", Column::I64(vec![0, i64::MAX])),
        ("s_count", wild()),
        ("s_errors", wild()),
        ("s_timeouts", wild()),
        ("s_dur_sum", wild()),
        ("s_h5", wild()),
        ("s_h9", wild()),
        ("s_w3", wild()),
    ]));
    for path in ["/api/overview", "/api/top", "/api/query?fp=1"] {
        let report = http_json(bouncer.port, path, "root");
        assert_eq!(report["total"]["count"], u64::MAX, "{path}: {report}");
        assert_eq!(report["total"]["not_ok"], u64::MAX, "{path}: {report}");
    }
}

#[test]
fn an_answer_that_is_not_a_table_is_a_report_that_failed() {
    let (bouncer, _fakes) = with_store(support::int(7));
    let (status, body) = http_get(bouncer.port, "/api/overview", Some("root"));
    assert_eq!(status, 502, "{body}");
    assert!(body.contains("not a table"), "{body}");
}
