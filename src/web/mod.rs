//! The report pages and the metrics, over HTTP on the bouncer's own port.

use std::sync::Arc;

use tokio::net::TcpStream;

use crate::core::Core;

pub async fn serve(_stream: TcpStream, _core: Arc<Core>) {}
