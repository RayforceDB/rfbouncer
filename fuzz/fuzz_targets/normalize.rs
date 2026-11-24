//! The normalizer: a client's source text in, the query's shape out.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rfbouncer::stats::normalize::{fnv1a, normalize_with};

fuzz_target!(|data: &[u8]| {
    // The request path hands it lossily decoded text, and so does this.
    let text = String::from_utf8_lossy(data);
    // With symbols kept, and with `normalize_symbols` taking them for values.
    for symbols in [false, true] {
        let (normal, fingerprint) = normalize_with(&text, symbols);
        assert_eq!(fingerprint, fnv1a(normal.as_bytes()));
        // One placeholder or one space per input byte at the very most.
        assert!(normal.len() <= 2 * text.len(), "{text:?} -> {normal:?}");
        assert!(!normal.contains("  ") && normal.trim_matches(' ') == normal);
        // What is already a shape is its own shape.
        assert_eq!(normalize_with(&normal, symbols).0, normal, "{text:?}");
    }
});
