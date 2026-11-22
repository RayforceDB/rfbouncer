//! The admin console: the user `rfbouncer` sends commands as strings and is
//! answered with tables.

use tokio::net::TcpStream;

use crate::core::Core;

pub async fn session(_stream: TcpStream, _version: u8, _core: &Core) {}
