//! The report pages and the metrics, over HTTP on the bouncer's own port.
//!
//! Everything here is read-only and behind the admin password. The page is
//! built into the binary and asks for JSON; nothing is fetched from anywhere
//! else.

pub mod api;

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{
    AUTHORIZATION, CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, WWW_AUTHENTICATE,
};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use tokio::net::TcpStream;

use crate::core::Core;
use crate::metrics;
use api::Filter;

const INDEX: &str = include_str!("../../web/index.html");
const APP: &str = include_str!("../../web/app.js");
const STYLE: &str = include_str!("../../web/style.css");

const HTML: &str = "text/html; charset=utf-8";
const JSON: &str = "application/json";
const TEXT: &str = "text/plain; charset=utf-8";

type Reply = Response<Full<Bytes>>;

pub async fn serve(stream: TcpStream, core: Arc<Core>) {
    let service = service_fn(move |request| {
        let core = core.clone();
        async move { Ok::<_, Infallible>(handle(request, &core).await) }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(10))
        .serve_connection(TokioIo::new(stream), service)
        .await;
}

fn reply(status: StatusCode, kind: &'static str, body: impl Into<Bytes>) -> Reply {
    let mut response = Response::new(Full::new(body.into()));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, hyper::header::HeaderValue::from_static(kind));
    headers.insert(
        CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    // The page shows text that clients wrote; nothing but our own files may
    // run or load, whatever that text contains.
    headers.insert(
        CONTENT_SECURITY_POLICY,
        hyper::header::HeaderValue::from_static(
            "default-src 'self'; style-src 'self' 'unsafe-inline'",
        ),
    );
    response
}

/// Whether the request carries the admin password, as HTTP Basic. Any user
/// name will do.
fn authorized(request: &Request<Incoming>, password: &str) -> bool {
    let given = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
        .and_then(|v| STANDARD.decode(v.trim()).ok());
    let Some(given) = given else { return false };
    let secret = given
        .iter()
        .position(|b| *b == b':')
        .map_or(&given[..], |colon| &given[colon + 1..]);
    secret.ct_eq(password.as_bytes()).into()
}

async fn handle(request: Request<Incoming>, core: &Arc<Core>) -> Reply {
    let Some(password) = core.config().admin_password.clone() else {
        return reply(
            StatusCode::FORBIDDEN,
            TEXT,
            "the reports are off: set admin_password to turn them on\n",
        );
    };
    if !authorized(&request, &password) {
        let mut response = reply(
            StatusCode::UNAUTHORIZED,
            TEXT,
            "the admin password is required\n",
        );
        let challenge = hyper::header::HeaderValue::from_static("Basic realm=\"rfbouncer\"");
        response.headers_mut().insert(WWW_AUTHENTICATE, challenge);
        return response;
    }
    if request.method() != Method::GET {
        return reply(StatusCode::METHOD_NOT_ALLOWED, TEXT, "GET only\n");
    }
    let query = request.uri().query().unwrap_or_default().to_owned();
    match request.uri().path() {
        "/" => reply(StatusCode::OK, HTML, INDEX),
        "/app.js" => reply(StatusCode::OK, "text/javascript; charset=utf-8", APP),
        "/style.css" => reply(StatusCode::OK, "text/css; charset=utf-8", STYLE),
        "/metrics" => reply(
            StatusCode::OK,
            "text/plain; version=0.0.4",
            metrics::render(core).await,
        ),
        "/api/live" => reply(StatusCode::OK, JSON, live(core).await.to_string()),
        path => match path.strip_prefix("/api/") {
            Some(what) => report(core, what.to_owned(), Filter::parse(&query)).await,
            None => reply(StatusCode::NOT_FOUND, TEXT, "not found\n"),
        },
    }
}

/// One of the reports kept in the statistics store.
async fn report(core: &Arc<Core>, what: String, filter: Filter) -> Reply {
    let stats = &core.stats;
    if !stats.enabled() {
        let body =
            json!({ "error": "statistics are off: set rayforce under [stats] to turn them on" });
        return reply(StatusCode::NOT_FOUND, JSON, body.to_string());
    }
    // A report is of everything that has ended, the last seconds included.
    stats.flush().await;
    let answer = match what.as_str() {
        "overview" => api::overview(stats, &filter).await,
        "top" => api::top(stats, &filter).await,
        "query" => api::query(stats, &filter).await,
        "slow" => api::slow(stats, &filter).await,
        "errors" => api::errors(stats, &filter).await,
        "clients" => api::grouped(stats, &filter, "rfb_client").await,
        "backends" => api::grouped(stats, &filter, "rfb_backend").await,
        _ => return reply(StatusCode::NOT_FOUND, TEXT, "not found\n"),
    };
    match answer {
        Ok(body) => reply(StatusCode::OK, JSON, body.to_string()),
        Err(e) => {
            tracing::error!(error = %e, "report failed");
            let body = json!({ "error": format!("the statistics store: {e}") });
            reply(StatusCode::BAD_GATEWAY, JSON, body.to_string())
        }
    }
}

/// What the bouncer is doing this instant: every pool's replicas and queue.
async fn live(core: &Core) -> Value {
    let mut pools = Vec::new();
    for pool in core.pools() {
        let Some(s) = pool.snapshot().await else {
            continue;
        };
        let c = &pool.shared.counters;
        let backends: Vec<Value> = s
            .backends
            .iter()
            .map(|b| {
                json!({
                    "addr": &*b.addr,
                    "state": b.state,
                    "draining": b.draining,
                    "last_error": b.last_error,
                    "served": b.served,
                    "failures": b.failures,
                    "running": b.running.as_ref().map(|(client, since, text)| json!({
                        "client": client.to_string(),
                        "ms": since.as_millis() as u64,
                        "text": text,
                    })),
                })
            })
            .collect();
        let queue: Vec<Value> = s
            .queue
            .iter()
            .map(|q| json!({ "client": q.client.to_string(), "waited_ms": q.waited.as_millis() as u64, "text": q.text }))
            .collect();
        pools.push(json!({
            "name": &*s.name,
            "paused": s.paused,
            "backends": backends,
            "queue": queue,
            "queue_max": pool.shared.queue_max.load(Relaxed),
            "requests": c.requests.load(Relaxed),
        }));
    }
    json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_s": core.started.elapsed().as_secs(),
        "clients": core.client_count(),
        "stats": core.stats.enabled(),
        "stats_dropped": core.stats.dropped.load(Relaxed),
        "pools": pools,
    })
}
