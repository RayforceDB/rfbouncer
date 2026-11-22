//! A connection to the Rayforce process that keeps the statistics.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::config::StatsConfig;
use crate::wire::frame::{
    self, FLAG_COMPRESSED, HEADER_LEN, Header, MSG_RESPONSE, MSG_SYNC, PAYLOAD_MAX,
};
use crate::wire::value::{self, Value};
use crate::wire::{compress, greeting};

/// How long the statistics store may take over anything. It is asked small
/// things; past this it is treated as gone.
const BUDGET: Duration = Duration::from_secs(10);

pub struct Conn {
    stream: TcpStream,
    version: u8,
}

impl Conn {
    pub async fn open(cfg: &StatsConfig) -> Result<Self, String> {
        let addr = cfg.rayforce.as_deref().ok_or("statistics are off")?;
        timeout(BUDGET, async {
            let mut stream = TcpStream::connect(addr)
                .await
                .map_err(|e| format!("connect {addr}: {e}"))?;
            let _ = stream.set_nodelay(true);
            greeting::connect(&mut stream, cfg.wire_version, cfg.password.as_deref())
                .await
                .map_err(|e| format!("greeting {addr}: {e}"))?;
            Ok(Self {
                stream,
                version: cfg.wire_version,
            })
        })
        .await
        .map_err(|_| format!("connect {addr}: timed out"))?
    }

    /// Sends source to be parsed and evaluated; an error the server raises
    /// comes back as `Err`.
    pub async fn eval(&mut self, source: &str) -> Result<Value, String> {
        self.call(&value::str_atom(source))
            .await
            .map_err(|e| format!("{e} in {}", clip(source)))
    }

    /// Sends a value to be evaluated.
    pub async fn call(&mut self, payload: &[u8]) -> Result<Value, String> {
        match timeout(BUDGET, self.exchange(payload)).await {
            Ok(Ok(Value::Error(code, msg))) => Err(format!("{code}: {msg}")),
            Ok(answer) => answer,
            Err(_) => Err("the statistics store did not answer in time".into()),
        }
    }

    async fn exchange(&mut self, payload: &[u8]) -> Result<Value, String> {
        let io = |e: std::io::Error| e.to_string();
        self.stream
            .write_all(&frame::frame(self.version, MSG_SYNC, payload))
            .await
            .map_err(io)?;
        let mut raw = [0u8; HEADER_LEN];
        self.stream.read_exact(&mut raw).await.map_err(io)?;
        let header = Header::parse(&raw, self.version, PAYLOAD_MAX).map_err(|e| e.to_string())?;
        if header.msgtype != MSG_RESPONSE {
            return Err("the statistics store sent something that is not an answer".into());
        }
        let mut body = vec![0u8; header.size];
        self.stream.read_exact(&mut body).await.map_err(io)?;
        if header.flags & FLAG_COMPRESSED != 0 {
            body = compress::decompress(&body, PAYLOAD_MAX).map_err(|e| e.to_string())?;
        }
        value::decode(&body).map_err(|e| e.to_string())
    }
}

fn clip(source: &str) -> &str {
    let end = source
        .char_indices()
        .nth(120)
        .map_or(source.len(), |(i, _)| i);
    &source[..end]
}
