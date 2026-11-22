//! The greeting: two bytes each way, `{wire_version, flags}`, then the
//! credentials when the server raised its auth flag: one length byte and
//! `user:password`, answered by one verdict byte.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const AUTH_REQUIRED: u8 = 0x01;
const AUTH_OK: u8 = 0;
const AUTH_REJECTED: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub enum GreetError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("wire version {got}, expected {expected}")]
    Version { got: u8, expected: u8 },
    #[error("the server wants a password and none is configured")]
    PasswordMissing,
    #[error("the server refused the password")]
    PasswordRefused,
    #[error("credentials too long for the one-byte length")]
    PasswordTooLong,
}

/// The client's side, which is how the bouncer opens a replica. Returns once
/// the server has taken the connection, which it does from its evaluating
/// thread: a greeting that completes means that thread was free.
pub async fn connect<S>(
    stream: &mut S,
    version: u8,
    password: Option<&str>,
) -> Result<(), GreetError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(&[version, 0]).await?;
    let mut hello = [0u8; 2];
    stream.read_exact(&mut hello).await?;
    if hello[0] != version {
        return Err(GreetError::Version {
            got: hello[0],
            expected: version,
        });
    }
    if hello[1] & AUTH_REQUIRED == 0 {
        return Ok(());
    }
    let password = password.ok_or(GreetError::PasswordMissing)?;
    // The NUL is what rayforce's own client sends and counts in the length.
    let text = format!("rfbouncer:{password}\0");
    let len = u8::try_from(text.len()).map_err(|_| GreetError::PasswordTooLong)?;
    stream.write_all(&[len]).await?;
    stream.write_all(text.as_bytes()).await?;
    let mut verdict = [0u8; 1];
    stream.read_exact(&mut verdict).await?;
    if verdict[0] != AUTH_OK {
        return Err(GreetError::PasswordRefused);
    }
    Ok(())
}

/// The client's opening two bytes: its wire version and its capabilities.
pub async fn read_hello<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<u8> {
    let mut hello = [0u8; 2];
    stream.read_exact(&mut hello).await?;
    Ok(hello[0])
}

/// The server's answer. No capability is ever offered, so a client never asks
/// to move the connection off TCP.
pub async fn write_hello<S: AsyncWrite + Unpin>(
    stream: &mut S,
    version: u8,
    auth: bool,
) -> io::Result<()> {
    stream.write_all(&[version, u8::from(auth)]).await
}

pub struct Credentials {
    pub user: String,
    pub password: Vec<u8>,
}

/// Splits at the first colon, as the server does; without one the whole of it
/// is the password.
pub async fn read_credentials<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Credentials> {
    let len = usize::from(stream.read_u8().await?);
    let mut raw = vec![0u8; len];
    stream.read_exact(&mut raw).await?;
    if raw.last() == Some(&0) {
        raw.pop();
    }
    Ok(match raw.iter().position(|b| *b == b':') {
        Some(colon) => Credentials {
            user: String::from_utf8_lossy(&raw[..colon]).into_owned(),
            password: raw[colon + 1..].to_vec(),
        },
        None => Credentials {
            user: String::new(),
            password: raw,
        },
    })
}

pub async fn write_verdict<S: AsyncWrite + Unpin>(stream: &mut S, ok: bool) -> io::Result<()> {
    stream
        .write_all(&[if ok { AUTH_OK } else { AUTH_REJECTED }])
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn credentials_split_at_the_first_colon_and_drop_the_nul() {
        let mut wire: &[u8] = b"\x08app:a:b\0";
        let c = read_credentials(&mut wire).await.unwrap();
        assert_eq!((c.user.as_str(), &c.password[..]), ("app", &b"a:b"[..]));
        let mut wire: &[u8] = b"\x02pw";
        let c = read_credentials(&mut wire).await.unwrap();
        assert_eq!((c.user.as_str(), &c.password[..]), ("", &b"pw"[..]));
    }

    #[tokio::test]
    async fn a_client_sends_what_a_server_reads() {
        let (mut a, mut b) = tokio::io::duplex(64);
        let server = tokio::spawn(async move {
            assert_eq!(read_hello(&mut b).await.unwrap(), 4);
            write_hello(&mut b, 4, true).await.unwrap();
            let c = read_credentials(&mut b).await.unwrap();
            write_verdict(&mut b, c.password == b"pw").await.unwrap();
            c.user
        });
        connect(&mut a, 4, Some("pw")).await.unwrap();
        assert_eq!(server.await.unwrap(), "rfbouncer");
    }

    #[tokio::test]
    async fn another_version_is_refused() {
        let mut wire = tokio_test_stream(&[3, 0]);
        let err = connect(&mut wire, 4, None).await.unwrap_err();
        assert!(matches!(
            err,
            GreetError::Version {
                got: 3,
                expected: 4
            }
        ));
    }

    fn tokio_test_stream(reply: &[u8]) -> tokio::io::DuplexStream {
        let (a, mut b) = tokio::io::duplex(64);
        let reply = reply.to_vec();
        tokio::spawn(async move {
            let mut hello = [0u8; 2];
            b.read_exact(&mut hello).await.unwrap();
            b.write_all(&reply).await.unwrap();
            let mut rest = Vec::new();
            let _ = b.read_to_end(&mut rest).await;
        });
        a
    }
}
