//! Rayforce's IPC wire, as far as a proxy needs it: the greeting, the frame
//! header, the cancel byte, and the handful of values the bouncer itself
//! reads or writes. Written against rayforce's `src/core/ipc.c` and
//! `src/store/serde.c`; recheck on every wire version bump.

pub mod compress;
pub mod frame;
pub mod greeting;
pub mod oob;
pub mod value;
