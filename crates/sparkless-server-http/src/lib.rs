//! HTTP/1 connection transport. Route policy and execution remain in the runtime service.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{CONTENT_ENCODING, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, StatusCode};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use tokio::net::TcpStream;

pub type HttpResponse = Response<Full<Bytes>>;

#[derive(Debug)]
pub struct HttpRequestError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

pub fn validate_json_headers(headers: &hyper::HeaderMap) -> Result<(), HttpRequestError> {
    if let Some(content_type) = headers.get(CONTENT_TYPE) {
        let Ok(value) = content_type.to_str() else {
            return Err(HttpRequestError {
                status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
                code: "UnsupportedContentType",
                message: "Content-Type must be application/json".into(),
            });
        };
        if !value
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .eq_ignore_ascii_case("application/json")
        {
            return Err(HttpRequestError {
                status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
                code: "UnsupportedContentType",
                message: "Content-Type must be application/json".into(),
            });
        }
    }
    if headers
        .get(CONTENT_ENCODING)
        .is_some_and(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
    {
        return Err(HttpRequestError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            code: "UnsupportedContentEncoding",
            message: "compressed request bodies are not supported".into(),
        });
    }
    Ok(())
}

pub async fn read_bounded_body_with_timeout(
    mut body: Incoming,
    limit: usize,
    wait: std::time::Duration,
) -> Result<Vec<u8>, HttpRequestError> {
    match tokio::time::timeout(wait, async move {
        let mut bytes = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|error| HttpRequestError {
                status: StatusCode::BAD_REQUEST,
                code: "InvalidBody",
                message: bound_text(&error.to_string(), 1024),
            })?;
            if let Ok(data) = frame.into_data() {
                if bytes.len().saturating_add(data.len()) > limit {
                    return Err(HttpRequestError {
                        status: StatusCode::PAYLOAD_TOO_LARGE,
                        code: "RequestTooLarge",
                        message: "request body exceeds the configured byte limit".into(),
                    });
                }
                bytes.extend_from_slice(&data);
            }
        }
        Ok(bytes)
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(HttpRequestError {
            status: StatusCode::REQUEST_TIMEOUT,
            code: "BodyReadTimeout",
            message: "timed out while reading the request body".into(),
        }),
    }
}

/// Application hooks used by the HTTP transport. Implementations own engine and catalog policy.
pub trait HttpApplication: Clone + Send + Sync + 'static {
    fn connection_permit(&self) -> Option<sparkless_server_runtime::ConnectionPermit>;
    fn ready(&self) -> bool;
    fn capabilities(&self) -> HttpResponse;
    fn statements(
        &self,
        request: Request<Incoming>,
    ) -> impl std::future::Future<Output = HttpResponse> + Send;
    fn close(
        &self,
        request: Request<Incoming>,
    ) -> impl std::future::Future<Output = HttpResponse> + Send;
    fn operation_status(
        &self,
        request: Request<Incoming>,
    ) -> impl std::future::Future<Output = HttpResponse> + Send;
}

/// Serve one accepted connection with a cloneable route handler.
pub async fn serve_connection<A: HttpApplication>(
    stream: TcpStream,
    application: A,
) -> Result<(), hyper::Error> {
    let Some(_connection_permit) = application.connection_permit() else {
        return Ok(());
    };
    let service = service_fn(move |request| {
        let application = application.clone();
        async move { Ok::<_, Infallible>(dispatch(application, request).await) }
    });
    http1::Builder::new()
        .serve_connection(TokioIo::new(stream), service)
        .await
}

async fn dispatch<A: HttpApplication>(application: A, request: Request<Incoming>) -> HttpResponse {
    match (request.method(), request.uri().path()) {
        (&Method::GET, "/health") => {
            let (status, body) = if application.ready() {
                (StatusCode::OK, serde_json::json!({"status":"ready"}))
            } else {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    serde_json::json!({"status":"unavailable"}),
                )
            };
            json_response(status, body)
        }
        (&Method::GET, "/capabilities") => application.capabilities(),
        (&Method::POST, "/statements") => application.statements(request).await,
        (&Method::POST, "/close") => application.close(request).await,
        (&Method::POST, "/operations/status") => application.operation_status(request).await,
        _ => transport_error(StatusCode::NOT_FOUND, "NotFound", "route not found"),
    }
}

fn json_response(status: StatusCode, value: serde_json::Value) -> HttpResponse {
    let bytes = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(bytes)))
        .expect("valid JSON response")
}

fn transport_error(status: StatusCode, name: &str, message: &str) -> HttpResponse {
    json_response(
        status,
        serde_json::json!({"status":"error","error":{"code":name,"message":message}}),
    )
}

fn bound_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}
