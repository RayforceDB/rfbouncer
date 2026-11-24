//! What several targets share: a way to run the crate's async readers to
//! their end, and a reader that hands bytes out a few at a time.
#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};
use tokio::runtime::{Builder, Runtime};

thread_local! {
    // One runtime for the whole run: building one per input would be most
    // of the time spent.
    static RUNTIME: Runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
}

pub fn block_on<F: Future>(future: F) -> F::Output {
    RUNTIME.with(|rt| rt.block_on(future))
}

/// Gives at most `step` bytes to each read, as a socket would under a
/// sender that writes in pieces. A frame must come out the same however
/// its bytes were cut.
pub struct Chunked<'a> {
    pub bytes: &'a [u8],
    pub step: usize,
}

impl AsyncRead for Chunked<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let n = self.step.max(1).min(self.bytes.len()).min(buf.remaining());
        let (now, later) = self.bytes.split_at(n);
        buf.put_slice(now);
        self.bytes = later;
        Poll::Ready(Ok(()))
    }
}
