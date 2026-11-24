//! Decompression of a payload a client or a replica marked compressed, and
//! the variant that reads only its start; and the compressor against both.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rfbouncer::wire::compress::{compress, decompress, decompress_prefix};

const MAX: usize = 1 << 16;

fuzz_target!(|data: &[u8]| {
    let Some((limit, body)) = data.split_first_chunk::<2>() else {
        return;
    };
    let limit = usize::from(u16::from_le_bytes(*limit));
    let start = decompress_prefix(body, limit);
    // Whatever the payload claims, the start costs what was asked for.
    assert!(start.len() <= limit);
    match decompress(body, MAX) {
        Ok(whole) => {
            assert!(!whole.is_empty() && whole.len() <= MAX);
            assert_eq!(
                Some(whole.len()),
                body.first_chunk::<4>()
                    .map(|n| u32::from_le_bytes(*n) as usize)
            );
            // The start is the start of the whole. The whole ends at the
            // size the payload declares and the start does not know it, so
            // only that much is compared.
            let n = limit.min(whole.len());
            assert_eq!(start[..n], whole[..n]);
        }
        Err(_) => {
            // A broken stream still has a start; asking for more of it
            // only ever adds to what a shorter ask gave.
            let shorter = decompress_prefix(body, limit / 2);
            assert_eq!(shorter[..], start[..shorter.len()]);
        }
    }
    // The other way: the same bytes as a payload to compress. What the
    // compressor takes is smaller and comes back whole, start included.
    if let Some(packed) = compress(body) {
        assert!(packed.len() < body.len());
        assert_eq!(decompress(&packed, body.len()).as_deref(), Ok(body));
        assert_eq!(
            decompress_prefix(&packed, limit),
            body[..limit.min(body.len())]
        );
    }
});
