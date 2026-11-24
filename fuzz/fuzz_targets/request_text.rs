//! Reading the source text out of a request frame, compressed or not: done
//! for every request by the statistics and on every snapshot of a pool.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rfbouncer::pool::{binary_fingerprint, request_source, request_text};
use rfbouncer::stats::normalize::normalize;
use rfbouncer::wire::frame::{FLAG_COMPRESSED, HEADER_LEN};
use rfbouncer::wire::value::{self, TEXT_CAP};

fuzz_target!(|data: &[u8]| {
    // The input is taken as a whole frame: the flags are its sixth byte.
    let source = request_source(data);
    let shown = request_text(data);
    let _ = binary_fingerprint(data);
    match &source {
        Some(text) => {
            assert_eq!(&shown, text);
            // A byte that is not UTF-8 becomes a three-byte replacement,
            // and no more than the cap is ever read.
            assert!(text.len() <= 3 * TEXT_CAP);
            if data[5] & FLAG_COMPRESSED == 0 {
                let rest = &data[HEADER_LEN + 10..];
                assert!(text.len() <= 3 * rest.len());
            }
            // What the statistics do with it next.
            let (normal, _) = normalize(text);
            assert!(normal.len() <= 2 * text.len());
        }
        None => assert!(shown.starts_with("<binary ")),
    }
    // The payload alone, under both readings of its flags.
    for flags in [0, FLAG_COMPRESSED] {
        let _ = value::request_text(flags, data);
    }
    // A string atom is read back as it was written.
    if let Ok(text) = std::str::from_utf8(data) {
        let atom = value::str_atom(text);
        let cut = (0..=text.len().min(TEXT_CAP))
            .rev()
            .find(|i| text.is_char_boundary(*i))
            .unwrap_or(0);
        let read = value::request_text(0, &atom).expect("a string atom");
        assert!(read.starts_with(&text[..cut]));
        assert!(text.len() > TEXT_CAP || read == text);
    }
});
