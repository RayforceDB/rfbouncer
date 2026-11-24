//! The status stream: a connection on which a replica says, every interval
//! and independently of its evaluating thread, what it is doing and what it
//! costs. The client asks for it with one frame of type `MSG_STATUS` whose
//! payload is the interval in milliseconds; from then on the server writes
//! frames of that type and reads nothing.

use bytes::Bytes;

use super::frame;

pub const MSG_STATUS: u8 = 4;
/// The one layout this code reads.
pub const LAYOUT: u8 = 1;
/// The fixed part of a status payload; the running query's text follows.
pub const FIXED_LEN: usize = 96;
pub const QUERY_MAX: usize = 256;
/// The interval a server accepts, in milliseconds.
pub const INTERVAL_MS: std::ops::RangeInclusive<u32> = 100..=10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Idle,
    /// Running a request that came over IPC.
    Request,
    /// Running something else: a timer, a refresh of its own.
    Other,
}

/// One status frame, as the server laid it out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub state: State,
    pub pid: u32,
    /// The server's clock, in nanoseconds since the Unix epoch.
    pub now: i64,
    /// When what it is running began, by the same clock; 0 when idle.
    pub busy_since: i64,
    pub loop_busy_ns: u64,
    pub loop_total_ns: u64,
    pub cpu_user_ns: u64,
    pub cpu_sys_ns: u64,
    pub rss_bytes: u64,
    pub heap_bytes: u64,
    /// 0 when the server has no limit.
    pub mem_limit_bytes: u64,
    pub conns: u32,
    pub workers: u32,
    pub cores: u32,
    pub query: String,
}

/// The frame that asks a server for its status every `interval_ms`.
pub fn request(version: u8, interval_ms: u32) -> Bytes {
    frame::frame(version, MSG_STATUS, &interval_ms.to_le_bytes())
}

/// Reads a status payload. `None` for anything that is not layout 1 in
/// full: another layout version, a state this code does not know, a payload
/// shorter than it declares. Bytes past the declared text are ignored.
pub fn decode(payload: &[u8]) -> Option<Frame> {
    let fixed: &[u8; FIXED_LEN] = payload.get(..FIXED_LEN)?.try_into().ok()?;
    let u32_at =
        |at: usize| u32::from_le_bytes([fixed[at], fixed[at + 1], fixed[at + 2], fixed[at + 3]]);
    let u64_at = |at: usize| u64::from(u32_at(at)) | u64::from(u32_at(at + 4)) << 32;
    if fixed[0] != LAYOUT {
        return None;
    }
    let state = match fixed[1] {
        0 => State::Idle,
        1 => State::Request,
        2 => State::Other,
        _ => return None,
    };
    let query_len = usize::from(u16::from_le_bytes([fixed[2], fixed[3]]));
    if query_len > QUERY_MAX {
        return None;
    }
    let query = payload.get(FIXED_LEN..FIXED_LEN + query_len)?;
    Some(Frame {
        state,
        pid: u32_at(4),
        now: u64_at(8) as i64,
        busy_since: u64_at(16) as i64,
        loop_busy_ns: u64_at(24),
        loop_total_ns: u64_at(32),
        cpu_user_ns: u64_at(40),
        cpu_sys_ns: u64_at(48),
        rss_bytes: u64_at(56),
        heap_bytes: u64_at(64),
        mem_limit_bytes: u64_at(72),
        conns: u32_at(80),
        workers: u32_at(84),
        cores: u32_at(88),
        query: String::from_utf8_lossy(query).into_owned(),
    })
}

impl Frame {
    /// The payload a server would write for this frame. The bouncer never
    /// sends one; the tests' stand-in server does.
    pub fn encode(&self) -> Vec<u8> {
        let query = &self.query.as_bytes()[..self.query.len().min(QUERY_MAX)];
        let mut out = vec![LAYOUT, self.state as u8];
        out.extend_from_slice(&(query.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.pid.to_le_bytes());
        out.extend_from_slice(&self.now.to_le_bytes());
        out.extend_from_slice(&self.busy_since.to_le_bytes());
        for n in [
            self.loop_busy_ns,
            self.loop_total_ns,
            self.cpu_user_ns,
            self.cpu_sys_ns,
            self.rss_bytes,
            self.heap_bytes,
            self.mem_limit_bytes,
        ] {
            out.extend_from_slice(&n.to_le_bytes());
        }
        for n in [self.conns, self.workers, self.cores, 0] {
            out.extend_from_slice(&n.to_le_bytes());
        }
        out.extend_from_slice(query);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Frame {
        Frame {
            state: State::Request,
            pid: 4242,
            now: 1_800_000_000_000_000_000,
            busy_since: 1_799_999_999_000_000_000,
            loop_busy_ns: 5,
            loop_total_ns: 9,
            cpu_user_ns: 11,
            cpu_sys_ns: 13,
            rss_bytes: 1 << 30,
            heap_bytes: 1 << 29,
            mem_limit_bytes: 1 << 32,
            conns: 3,
            workers: 8,
            cores: 16,
            query: "(select {from: t})".into(),
        }
    }

    #[test]
    fn the_layout_is_the_one_agreed_with_the_server() {
        let raw = sample().encode();
        assert_eq!(raw.len(), FIXED_LEN + 18);
        assert_eq!(&raw[..4], &[1, 1, 18, 0]);
        assert_eq!(raw[4..8], 4242u32.to_le_bytes());
        assert_eq!(raw[16..24], 1_799_999_999_000_000_000i64.to_le_bytes());
        assert_eq!(raw[32..40], 9u64.to_le_bytes());
        assert_eq!(raw[56..64], (1u64 << 30).to_le_bytes());
        assert_eq!(raw[72..80], (1u64 << 32).to_le_bytes());
        assert_eq!(raw[88..92], 16u32.to_le_bytes());
        assert_eq!(&raw[96..], b"(select {from: t})");
        assert_eq!(decode(&raw), Some(sample()));
        assert_eq!(&request(4, 1000)[16..], &[0xe8, 3, 0, 0]);
        assert_eq!(request(4, 1000)[7], MSG_STATUS);
    }

    #[test]
    fn a_frame_that_is_not_layout_one_in_full_is_no_status() {
        let good = sample().encode();
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&good[..FIXED_LEN - 1]), None);
        // The text it declares is not all there.
        assert_eq!(decode(&good[..good.len() - 1]), None);
        let mut other_layout = good.clone();
        other_layout[0] = 2;
        assert_eq!(decode(&other_layout), None);
        let mut unknown_state = good.clone();
        unknown_state[1] = 3;
        assert_eq!(decode(&unknown_state), None);
        let mut long_text = good.clone();
        long_text[2..4].copy_from_slice(&257u16.to_le_bytes());
        long_text.resize(FIXED_LEN + 257, b'x');
        assert_eq!(decode(&long_text), None);
        // Text that is not UTF-8 is shown as far as it can be, not refused.
        let mut binary = good.clone();
        binary[FIXED_LEN] = 0xff;
        assert!(decode(&binary).is_some_and(|f| f.query.starts_with('\u{fffd}')));
        let mut trailing = good;
        trailing.push(0);
        assert_eq!(decode(&trailing), Some(sample()));
    }
}
