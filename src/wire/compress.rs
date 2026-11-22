//! Undoes `ray_ipc_compress` (rayforce `src/core/ipc.c`): a u32 uncompressed
//! size, then a run-length stream of byte deltas. A positive count byte is a
//! run followed by the byte to repeat; a count of zero or below is the negated
//! length of a literal stretch. Each output byte is the previous one plus the
//! delta, wrapping.

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("compressed payload: {0}")]
pub struct CompressError(&'static str);

pub fn decompress(body: &[u8], max: usize) -> Result<Vec<u8>, CompressError> {
    let (size, stream) = body
        .split_first_chunk::<4>()
        .ok_or(CompressError("shorter than its size prefix"))?;
    let expected = u32::from_le_bytes(*size) as usize;
    if expected == 0 || expected > max {
        return Err(CompressError("uncompressed size out of range"));
    }
    let mut out: Vec<u8> = Vec::with_capacity(expected);
    let mut at = 0;
    while at < stream.len() && out.len() < expected {
        let count = stream[at] as i8;
        at += 1;
        let len = usize::from(count.unsigned_abs());
        if out.len() + len > expected {
            return Err(CompressError("segment past the uncompressed size"));
        }
        if count > 0 {
            let value = *stream.get(at).ok_or(CompressError("run without a value"))?;
            at += 1;
            out.resize(out.len() + len, value);
        } else {
            let literal = stream
                .get(at..at + len)
                .ok_or(CompressError("literal past the end of the stream"))?;
            at += len;
            out.extend_from_slice(literal);
        }
    }
    if out.len() != expected {
        return Err(CompressError("decompressed size does not match"));
    }
    for i in 1..out.len() {
        out[i] = out[i].wrapping_add(out[i - 1]);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Recorded off a rayforce v2.5.14 process: a 2410-byte i64 vector.
    #[test]
    fn a_server_payload_decompresses() {
        let mut wire = vec![0x6a, 0x09, 0, 0, 0xfb, 0x05, 0xfb, 0x2c, 0xd5, 0xff];
        wire.extend(std::iter::repeat_n([0x7f, 0x00], 18).flatten());
        wire.extend_from_slice(&[0x77, 0x00]);
        let mut expected = vec![5, 0, 0x2c, 1, 0, 0, 0, 0, 0, 0];
        expected.extend(std::iter::repeat_n(0, 2400));
        assert_eq!(decompress(&wire, 1 << 20).unwrap(), expected);
    }

    #[test]
    fn malformed_streams_are_refused() {
        assert!(decompress(&[1, 0], 100).is_err());
        assert!(decompress(&[0, 0, 0, 0], 100).is_err());
        assert!(decompress(&[200, 0, 0, 0, 5, 1], 100).is_err());
        assert!(decompress(&[4, 0, 0, 0, 5, 1], 100).is_err()); // run past size
        assert!(decompress(&[4, 0, 0, 0, 3], 100).is_err()); // run without value
        assert!(decompress(&[4, 0, 0, 0, 0xfc, 1, 2], 100).is_err()); // short literal
        assert!(decompress(&[4, 0, 0, 0, 2, 1], 100).is_err()); // too short
    }
}
