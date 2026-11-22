//! Whatever bytes arrive, nothing that reads them panics. A stand-in for a
//! coverage-guided fuzzer that runs on every `cargo test`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use rfbouncer::stats::normalize::normalize;
use rfbouncer::wire::compress::decompress;
use rfbouncer::wire::frame::{HEADER_LEN, Header, PAYLOAD_MAX};
use rfbouncer::wire::value::{decode, error_code, request_text};

/// xorshift: the same bytes on every run, so a failure can be found again.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn bytes(&mut self, max: usize) -> Vec<u8> {
        let len = self.next() as usize % max;
        // Half the inputs lean on the bytes the parsers branch on.
        let special = b"\x00\x01\x7f\x80\xf3\xff\xfa\xde\xce()[]{}\"\\;' 019-.";
        let lean = self.next() & 1 == 0;
        (0..len)
            .map(|_| {
                let r = self.next();
                if lean && r & 3 != 0 {
                    special[(r >> 8) as usize % special.len()]
                } else {
                    (r >> 16) as u8
                }
            })
            .collect()
    }
}

#[test]
fn no_input_makes_a_parser_panic() {
    let mut random = Random(0x9e37_79b9_7f4a_7c15);
    for _ in 0..200_000 {
        let bytes = random.bytes(96);
        let mut raw = [0u8; HEADER_LEN];
        let n = bytes.len().min(HEADER_LEN);
        raw[..n].copy_from_slice(&bytes[..n]);
        if let Ok(header) = Header::parse(&raw, raw[4], PAYLOAD_MAX) {
            assert!(header.size > 0 && header.size <= PAYLOAD_MAX);
        }
        if let Ok(plain) = decompress(&bytes, 4096) {
            assert!(!plain.is_empty() && plain.len() <= 4096);
        }
        let _ = error_code(&bytes);
        let _ = decode(&bytes);
        let _ = request_text(0, &bytes);
        let _ = request_text(1, &bytes);
        let text = String::from_utf8_lossy(&bytes);
        let (normal, _) = normalize(&text);
        // Normalizing only ever drops or replaces: it never grows the text
        // past one placeholder and one space per input byte.
        assert!(normal.len() <= 2 * text.len() + 1, "{text:?} -> {normal:?}");
        // What is already normal stays as it is.
        assert_eq!(normalize(&normal).0, normal, "{text:?}");
    }
}
