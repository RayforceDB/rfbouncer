//! The frame reader a client's bytes and an admin's go through. It must
//! give back the stream's own bytes, cut at the sizes the headers declare,
//! and the same frames however the stream was cut into reads.
#![no_main]

mod common;

use bytes::{Bytes, BytesMut};
use libfuzzer_sys::fuzz_target;
use rfbouncer::client::read_frame;
use rfbouncer::wire::frame::HEADER_LEN;

/// Small, so that a header claiming the most allowed costs kilobytes.
const MAX: usize = 4096;

async fn frames(stream: &[u8], step: usize, version: u8) -> (Vec<Bytes>, bool) {
    let mut reader = common::Chunked {
        bytes: stream,
        step,
    };
    let mut buf = BytesMut::new();
    let mut out = Vec::new();
    loop {
        match read_frame(&mut reader, &mut buf, version, MAX).await {
            Ok(Some(frame)) => out.push(frame),
            Ok(None) => return (out, true),
            Err(_) => return (out, false),
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&[version, step], stream)) = data.split_first_chunk::<2>() else {
        return;
    };
    let (whole, clean) = common::block_on(frames(stream, usize::MAX, version));
    let mut at = 0;
    for frame in &whole {
        assert!(frame.len() > HEADER_LEN && frame.len() <= HEADER_LEN + MAX);
        assert_eq!(frame[4], version);
        assert_eq!(&frame[..], &stream[at..at + frame.len()]);
        at += frame.len();
    }
    // A stream that ended between two frames was read to its last byte.
    assert!(!clean || at == stream.len());
    let pieces = common::block_on(frames(stream, usize::from(step), version));
    assert_eq!(pieces, (whole, clean));
});
