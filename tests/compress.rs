//! Answers compressed by the bouncer for clients a replica cannot see.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::io::{Read, Write};
use std::time::Instant;

use rfbouncer::wire::compress::{compress, decompress};
use rfbouncer::wire::frame::{
    self, FLAG_COMPRESSED, HEADER_LEN, Header, MSG_RESPONSE, PAYLOAD_MAX,
};
use support::fake::{Fake, answer_forever, read_request};
use support::{Bouncer, Client, Rayforce, eventually, int, pool, rayforce_eval};

/// The tests' clients are on loopback, where nothing is compressed unless
/// the pool is told to.
const ALWAYS: &str = "require_restricted = false\ncompress = \"always\"";

/// A table of numbers and one of symbols, strings and timestamps, as
/// results go, kept on the replica so that every reader gets the same rows.
const NUMBERS: &str = "(set numbers (table [id px qty] (list (til 100000) (/ (rand 100000 100000) 100.0) (rand 100000 1000))))";
const WORDS: &str = "(set words (table [sym venue note ts] (list (take ['AAPL 'MSFT 'GOOG 'TSLA] 50000) (take ['XNAS 'XNYS] 50000) (map (fn [i] (format \"order %\" i)) (til 50000)) (+ 2026.01.01D00:00:00.000000000 (til 50000)))))";

fn seeded() -> Rayforce {
    let replica = Rayforce::start(&[]);
    let mut seed = Client::connect(replica.port);
    for source in [NUMBERS, WORDS] {
        assert_eq!(seed.query_error(source), None, "{source}");
    }
    replica
}

#[test]
fn a_rayforce_client_reads_what_the_bouncer_compressed() {
    let replica = seeded();
    let bouncer = Bouncer::start(&pool(&[&replica], ALWAYS));
    let mut direct = Client::connect(replica.port);
    let mut through = Client::connect(bouncer.port);
    let sources = ["numbers", "words", "(til 200000)"];
    for source in sources {
        let plain = direct.query(source);
        through.send(source);
        let (header, packed) = through.try_recv().unwrap();
        assert_eq!(header.flags & FLAG_COMPRESSED, FLAG_COMPRESSED, "{source}");
        assert!(packed.len() < plain.len(), "{source}");
        assert!(
            decompress(&packed, PAYLOAD_MAX).unwrap() == plain,
            "{source}"
        );
    }
    let metric = |name: &str| bouncer.metric(&format!("rfbouncer_{name}{{pool=\"main\"}}"));
    assert_eq!(metric("compressed_total"), 3.0);
    assert!(metric("compress_saved_bytes_total") > 500_000.0);
    assert_eq!(
        bouncer.metric("rfbouncer_requests_total{pool=\"main\",outcome=\"ok\"}"),
        3.0
    );

    // The real client: the same values through the bouncer as from the
    // replica itself, told by the bytes they serialize to.
    let same: String = sources
        .iter()
        .map(|q| {
            format!(
                "(println (all (== (ser (.ipc.send h \"{q}\")) (ser (.ipc.send d \"{q}\")))))\n"
            )
        })
        .collect();
    let out = rayforce_eval(&format!(
        "(set h (.ipc.open \"127.0.0.1:{}\"))\n(set d (.ipc.open \"127.0.0.1:{}\"))\n\
         (println (count (.ipc.send h \"words\")))\n{same}",
        bouncer.port, replica.port
    ));
    assert_eq!(out, "50000\ntrue\ntrue\ntrue\n");
    assert_eq!(metric("compressed_total"), 7.0);
}

#[test]
fn what_a_server_would_send_plain_is_passed_on_as_it_comes() {
    let replica = Rayforce::start(&[]);
    let mut direct = Client::connect(replica.port);
    let limited = format!("{ALWAYS}\ncompress_max_bytes = \"1MiB\"");
    let bouncer = Bouncer::start(&pool(&[&replica], &limited));
    let mut through = Client::connect(bouncer.port);
    // Under the threshold, past the limit, and an error.
    for source in ["(til 200)", "(til 200000)", "(nosuch 1)"] {
        direct.send(source);
        through.send(source);
        let (want_header, want) = direct.try_recv().unwrap();
        let (got_header, got) = through.try_recv().unwrap();
        assert_eq!(got_header, want_header, "{source}");
        assert!(got == want, "{source}: payloads differ");
    }
    // Within both, it is compressed: the limit was what held the other back.
    through.send("(til 100000)");
    let (header, _) = through.try_recv().unwrap();
    assert_eq!(header.flags & FLAG_COMPRESSED, FLAG_COMPRESSED);
    let outcome = |o: &str| {
        bouncer.metric(&format!(
            "rfbouncer_requests_total{{pool=\"main\",outcome=\"{o}\"}}"
        ))
    };
    assert_eq!((outcome("ok"), outcome("error")), (3.0, 1.0));

    // A client on this host is sent nothing compressed unless told to.
    let plain = Bouncer::start(&pool(&[&replica], "require_restricted = false"));
    let mut local = Client::connect(plain.port);
    local.send("(til 100000)");
    assert_eq!(local.try_recv().unwrap().0.flags & FLAG_COMPRESSED, 0);
}

#[test]
fn a_slow_reader_of_a_compressed_answer_is_cut_off_too() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(
        &[&replica],
        &format!("{ALWAYS}\ncompress_max_bytes = \"64MiB\"\ntransfer_timeout = \"1s\""),
    ));
    let mut slow_reader = Client::connect(bouncer.port);
    // 15 MiB, some 13 MiB compressed, and it takes none of it.
    slow_reader.send("(til 2000000)");
    eventually("the request is over", || {
        bouncer.metric("rfbouncer_requests_total{pool=\"main\",outcome=\"timeout\"}") == 1.0
    });
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 1)"), int(2));
    // What it does get is the start of a compressed answer, cut short.
    let mut sink = Vec::new();
    let _ = slow_reader.stream.read_to_end(&mut sink);
    let raw: [u8; HEADER_LEN] = sink[..HEADER_LEN].try_into().unwrap();
    let header = Header::parse(&raw, support::wire(), PAYLOAD_MAX).unwrap();
    assert_eq!(header.flags & FLAG_COMPRESSED, FLAG_COMPRESSED);
    assert!(header.size < 16_000_000, "{}", header.size);
    assert!(sink.len() < HEADER_LEN + header.size, "got a whole answer");
}

#[test]
fn an_answer_that_breaks_off_before_it_is_compressed_is_run_again() {
    // The first connection breaks off in the middle of an answer; the one
    // opened in its place answers. One replica, so which is asked first
    // does not depend on which came up first.
    let fake = Fake::start(|mut stream, n| {
        if n > 0 {
            return answer_forever(stream, 2);
        }
        if read_request(&mut stream) {
            let whole = frame::frame(support::wire(), MSG_RESPONSE, &[0u8; 5000]);
            let _ = stream.write_all(&whole[..2500]);
        }
    });
    let bouncer = Bouncer::start(&format!(
        "[[pool]]\nname = \"main\"\n{ALWAYS}\nbackends = [\"{}\"]\n",
        fake.addr()
    ));
    // Half the answer is in the bouncer and none of it with the client.
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 1)"), int(2));
    assert_eq!(
        bouncer.metric("rfbouncer_retries_total{pool=\"main\"}"),
        1.0
    );
}

/// Not a test: prints what compressing costs and saves on a few results.
/// `cargo test --release --test compress -- --ignored --nocapture`
#[test]
#[ignore = "a measurement, run by hand"]
fn what_compressing_costs_and_saves() {
    let replica = seeded();
    let mut direct = Client::connect(replica.port);
    println!("result | plain bytes | compressed | saved | compress | MiB/s | decompress");
    for source in [
        "numbers",
        "words",
        "(til 1000000)",
        "(/ (rand 1000000 100000) 100.0)",
        "(at words 'note)",
        "(at words 'sym)",
    ] {
        let plain = direct.query(source);
        let rounds = 20;
        let started = Instant::now();
        let mut packed = None;
        for _ in 0..rounds {
            packed = compress(&plain);
        }
        let took = started.elapsed() / rounds;
        let size = packed.as_ref().map_or(plain.len(), Vec::len);
        let started = Instant::now();
        if let Some(packed) = &packed {
            for _ in 0..rounds {
                decompress(packed, PAYLOAD_MAX).unwrap();
            }
        }
        let back = started.elapsed() / rounds;
        println!(
            "{source} | {} | {size} | {:.1}% | {took:?} | {:.0} | {back:?}",
            plain.len(),
            100.0 * (plain.len() - size) as f64 / plain.len() as f64,
            plain.len() as f64 / (1 << 20) as f64 / took.as_secs_f64(),
        );
    }
}
