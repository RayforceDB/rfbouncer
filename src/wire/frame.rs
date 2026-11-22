//! The 16-byte header that opens every frame, as `ray_ipc_header_t` in
//! rayforce's `src/store/serde.h` lays it out. Little-endian throughout.

use bytes::{BufMut, Bytes, BytesMut};

pub const PREFIX: u32 = 0xcefa_defa;
pub const HEADER_LEN: usize = 16;
/// The server's own ceiling on a payload (`RAY_IPC_BODY_MAX_BYTES`).
pub const PAYLOAD_MAX: usize = 256 * 1024 * 1024;

pub const MSG_ASYNC: u8 = 0;
pub const MSG_SYNC: u8 = 1;
pub const MSG_RESPONSE: u8 = 2;

pub const FLAG_COMPRESSED: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub version: u8,
    pub flags: u8,
    pub msgtype: u8,
    pub size: usize,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("bad frame prefix")]
    Prefix,
    #[error("wire version {got}, expected {expected}")]
    Version { got: u8, expected: u8 },
    #[error("big-endian frames are not supported")]
    Endian,
    #[error("payload size {0} out of range")]
    Size(i64),
}

impl Header {
    /// Checks everything a size read off the wire needs before it becomes an
    /// allocation: the prefix, the version, the byte order and the bound.
    pub fn parse(raw: &[u8; HEADER_LEN], version: u8, max: usize) -> Result<Self, FrameError> {
        let prefix = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
        if prefix != PREFIX {
            return Err(FrameError::Prefix);
        }
        if raw[4] != version {
            return Err(FrameError::Version {
                got: raw[4],
                expected: version,
            });
        }
        if raw[6] != 0 {
            return Err(FrameError::Endian);
        }
        let mut size = [0u8; 8];
        size.copy_from_slice(&raw[8..]);
        let size = i64::from_le_bytes(size);
        match usize::try_from(size) {
            Ok(n) if n > 0 && n <= max.min(PAYLOAD_MAX) => Ok(Self {
                version,
                flags: raw[5],
                msgtype: raw[7],
                size: n,
            }),
            _ => Err(FrameError::Size(size)),
        }
    }

    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[..4].copy_from_slice(&PREFIX.to_le_bytes());
        out[4] = self.version;
        out[5] = self.flags;
        out[7] = self.msgtype;
        out[8..].copy_from_slice(&(self.size as u64).to_le_bytes());
        out
    }
}

/// A whole uncompressed frame around `payload`.
pub fn frame(version: u8, msgtype: u8, payload: &[u8]) -> Bytes {
    let header = Header {
        version,
        flags: 0,
        msgtype,
        size: payload.len(),
    };
    let mut out = BytesMut::with_capacity(HEADER_LEN + payload.len());
    out.put_slice(&header.encode());
    out.put_slice(payload);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Read off a rayforce 3.0.2 process answering `(+ 1 2)`.
    const RESPONSE: [u8; 16] = [
        0xfa, 0xde, 0xfa, 0xce, 4, 0, 0, 2, 0x0a, 0, 0, 0, 0, 0, 0, 0,
    ];

    #[test]
    fn a_server_header_parses_and_encodes_back() {
        let h = Header::parse(&RESPONSE, 4, PAYLOAD_MAX).unwrap();
        assert_eq!(
            h,
            Header {
                version: 4,
                flags: 0,
                msgtype: MSG_RESPONSE,
                size: 10
            }
        );
        assert_eq!(h.encode(), RESPONSE);
    }

    #[test]
    fn a_header_is_refused_before_its_size_is_trusted() {
        let mut raw = RESPONSE;
        raw[0] = 0;
        assert_eq!(Header::parse(&raw, 4, 100), Err(FrameError::Prefix));
        assert_eq!(
            Header::parse(&RESPONSE, 3, 100),
            Err(FrameError::Version {
                got: 4,
                expected: 3
            })
        );
        let mut raw = RESPONSE;
        raw[6] = 1;
        assert_eq!(Header::parse(&raw, 4, 100), Err(FrameError::Endian));
        assert_eq!(Header::parse(&RESPONSE, 4, 9), Err(FrameError::Size(10)));
        let mut raw = RESPONSE;
        raw[8..].copy_from_slice(&(-1i64).to_le_bytes());
        assert_eq!(Header::parse(&raw, 4, 100), Err(FrameError::Size(-1)));
        raw[8..].copy_from_slice(&0i64.to_le_bytes());
        assert_eq!(Header::parse(&raw, 4, 100), Err(FrameError::Size(0)));
    }
}
