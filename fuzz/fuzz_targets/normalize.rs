//! The normalizer: a client's source text in, the query's shape out.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rfbouncer::stats::normalize::{fnv1a, normalize};

fuzz_target!(|data: &[u8]| {
    // The request path hands it lossily decoded text, and so does this.
    let text = String::from_utf8_lossy(data);
    let (normal, fingerprint) = normalize(&text);
    assert_eq!(fingerprint, fnv1a(normal.as_bytes()));
    // One placeholder or one space per input byte at the very most.
    assert!(normal.len() <= 2 * text.len(), "{text:?} -> {normal:?}");
    // Strings, comments and line structure never survive.
    assert!(!normal.contains(['"', ';', '\n', '\r', '\t']), "{normal:?}");
    assert!(!normal.contains("  ") && normal.trim_matches(' ') == normal);
    // What is already a shape is its own shape.
    assert_eq!(normalize(&normal).0, normal, "{text:?}");
});
