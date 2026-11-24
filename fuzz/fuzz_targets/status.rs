//! The status stream: frames a replica writes about itself, and the rates
//! and ranks the pool works out of two of them. A replica is a peer like
//! any other; its counters and its clock are whatever it says they are.
#![no_main]

use std::time::{Duration, Instant};

use libfuzzer_sys::fuzz_target;
use rfbouncer::pool::status::{Reading, load, rank};
use rfbouncer::wire::status::{FIXED_LEN, Frame, QUERY_MAX, decode};

fn frame(payload: &[u8]) -> Option<Frame> {
    let frame = decode(payload)?;
    // Only a payload that holds what it declares is a frame.
    let declared = usize::from(u16::from_le_bytes([payload[2], payload[3]]));
    assert!(declared <= QUERY_MAX && payload.len() >= FIXED_LEN + declared);
    assert!(frame.query.len() <= 3 * QUERY_MAX);
    // Written as a server would write it, it reads back the same. Text that
    // was not UTF-8 grew on the way in and is cut on the way out.
    if frame.query.len() <= QUERY_MAX {
        assert_eq!(decode(&frame.encode()).as_ref(), Some(&frame));
    }
    Some(frame)
}

fuzz_target!(|data: &[u8]| {
    // An interval, how long ago our last request ended, then two frames.
    let Some((&[interval, ago, ours, cut], rest)) = data.split_first_chunk::<4>() else {
        return;
    };
    let (first, second) = rest.split_at(usize::from(cut).min(rest.len()));
    // From 100 ms up, as the configuration holds it: with no interval
    // there is no stream.
    let interval = Duration::from_millis(100 * (1 + u64::from(interval)));
    let now = Instant::now();
    let ended = now.checked_sub(Duration::from_millis(u64::from(ago) * 20));
    let mut before = None;
    for payload in [first, second] {
        let Some(frame) = frame(payload) else {
            continue;
        };
        let reading = Reading::after(before.as_ref(), frame, now, interval);
        // A share of an interval, and cores: never negative, never NaN.
        assert!(reading.util.is_none_or(|u| (0.0..=1.0).contains(&u)));
        assert!(reading.cpu.is_none_or(|c| c >= 0.0));
        assert!(reading.mem_share().is_none_or(|m| m >= 0.0));
        let _ = reading.running_for();
        for ours in [0, usize::from(ours)] {
            let _ = reading.not_ours(ours, ended);
            let own = Duration::from_millis(u64::from(ago));
            assert!(load(Some(&reading), own).is_none_or(|l| l >= 0.0));
            // A rank is compared with others: it must compare with itself.
            let rank = rank(Some(&reading), ours, ended, own, 0.5);
            assert!(rank.partial_cmp(&rank).is_some());
        }
        before = Some(reading);
    }
});
