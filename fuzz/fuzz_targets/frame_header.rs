//! The 16 bytes that open every frame: whatever they are, the header is
//! refused or it is one whose size may be allocated.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rfbouncer::wire::frame::{HEADER_LEN, Header, PAYLOAD_MAX};

fuzz_target!(|data: &[u8]| {
    let Some((raw, rest)) = data.split_first_chunk::<HEADER_LEN>() else {
        return;
    };
    // The version expected and the bound come from the input too, so the
    // fuzzer reaches both sides of each check.
    let version = rest.first().copied().unwrap_or(raw[4]);
    let mut max = [0u8; 8];
    let n = rest.len().saturating_sub(1).min(8);
    max[..n].copy_from_slice(&rest[rest.len() - n..]);
    let max = usize::try_from(u64::from_le_bytes(max)).unwrap_or(usize::MAX);
    if let Ok(header) = Header::parse(raw, version, max) {
        assert!(header.size > 0 && header.size <= max && header.size <= PAYLOAD_MAX);
        assert_eq!(header.version, version);
        // A header that is accepted is one the bouncer would write itself:
        // nothing in it is dropped on the way through.
        assert_eq!(&header.encode(), raw);
    }
});
