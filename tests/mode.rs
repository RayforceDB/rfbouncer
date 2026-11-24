//! The two ways a pool chooses a replica.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::time::Duration;

use support::{Bouncer, Client, Rayforce, eventually, int, pool, slow};

/// Two replicas that answer `who` with 1 and 2.
fn pair() -> [Rayforce; 2] {
    let pair = [Rayforce::start(&[]), Rayforce::start(&[])];
    Client::connect(pair[0].port).query("(set who 1)");
    Client::connect(pair[1].port).query("(set who 2)");
    pair
}

/// Keeps the first replica busy, then sends four requests one after another
/// and returns their clients, the requests still unanswered.
fn four_behind_a_slow_one(bouncer: &Bouncer) -> (Client, Vec<Client>) {
    let mut hog = Client::connect(bouncer.port);
    hog.send(&slow(60_000));
    bouncer.wait_busy(1);
    // A request has arrived when it is answered or in the queue. Each is
    // sent once the one before has, so they arrive in the order sent.
    let arrived = || {
        let pool = &bouncer.live()["pools"][0];
        pool["requests"].as_u64().unwrap() + pool["queue"].as_array().unwrap().len() as u64
    };
    let before = arrived();
    let clients = (1..=4)
        .map(|i| {
            let mut c = Client::connect(bouncer.port);
            c.send("who");
            eventually("the request arrived", || arrived() >= before + i);
            c
        })
        .collect();
    (hog, clients)
}

/// The answer if one comes within a third of a second.
fn answered(c: &mut Client) -> Option<Vec<u8>> {
    c.stream
        .set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let answer = c.try_recv().ok().map(|(_, payload)| payload);
    c.stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    answer
}

#[test]
fn free_sends_every_request_past_a_busy_replica() {
    let [a, b] = pair();
    let bouncer = Bouncer::start(&pool(&[&a, &b], "require_restricted = false"));
    let (mut hog, mut clients) = four_behind_a_slow_one(&bouncer);
    // Answered while the slow request still holds the first replica, so
    // however long an answer takes it did not wait for that one.
    for c in &mut clients {
        assert_eq!(c.recv(), int(2));
    }
    hog.cancel();
    assert_eq!(hog.query_error_after_cancel(), Some("cancel".to_owned()));
}

#[test]
fn round_robin_gives_each_replica_its_turn_busy_or_not() {
    let [a, b] = pair();
    let bouncer = Bouncer::start(&pool(
        &[&a, &b],
        "require_restricted = false\nmode = \"round_robin\"",
    ));
    let (hog, mut clients) = four_behind_a_slow_one(&bouncer);
    // The slow request took the first replica's turn; the four after it
    // are the second's, the first's, the second's, the first's.
    assert_eq!(clients[0].recv(), int(2));
    assert_eq!(clients[2].recv(), int(2));
    assert_eq!(answered(&mut clients[1]), None);
    assert_eq!(answered(&mut clients[3]), None);
    hog.cancel();
    assert_eq!(clients[1].recv(), int(1));
    assert_eq!(clients[3].recv(), int(1));
}

#[test]
fn round_robin_takes_no_turn_for_a_replica_that_is_down() {
    let [mut a, b] = pair();
    let bouncer = Bouncer::start(&pool(
        &[&a, &b],
        "require_restricted = false\nmode = \"round_robin\"",
    ));
    assert_eq!(Client::connect(bouncer.port).query("who"), int(1));
    assert_eq!(Client::connect(bouncer.port).query("who"), int(2));
    a.kill();
    for _ in 0..6 {
        assert_eq!(Client::connect(bouncer.port).query("who"), int(2));
    }
}

#[test]
fn a_reload_changes_the_mode() {
    let [a, b] = pair();
    let bouncer = Bouncer::start(&pool(&[&a, &b], "require_restricted = false"));
    bouncer.reload(&format!(
        "admin_password = \"root\"\n{}",
        pool(
            &[&a, &b],
            "require_restricted = false\nmode = \"round_robin\""
        )
    ));
    std::thread::sleep(Duration::from_millis(300));
    let (hog, mut clients) = four_behind_a_slow_one(&bouncer);
    // Two wait for the busy replica's turn and two were the other's: which
    // replica answered says so, with no guess at how long an answer takes.
    bouncer.wait_queued(2);
    hog.cancel();
    let mut who: Vec<Vec<u8>> = clients.iter_mut().map(Client::recv).collect();
    who.sort();
    assert_eq!(who, [int(1), int(1), int(2), int(2)]);
}
