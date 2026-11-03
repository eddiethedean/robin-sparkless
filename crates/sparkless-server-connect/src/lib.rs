//! Spark Connect wire server and generated gRPC dispatcher. Product composition supplies RPC policy.

mod replay;
mod runtime;
mod service;
pub use replay::{
    CONNECT_DETACHED_RETENTION, ConnectOutcome, ConnectReplayError, ConnectReplayRecord,
    GoogleRpcErrorInfo, GoogleRpcStatus, MAX_CONNECT_ARROW_BATCH_ROWS,
    MAX_CONNECT_ARROW_RESPONSE_BYTES, MAX_CONNECT_HISTORY_BYTES, MAX_CONNECT_HISTORY_ENTRIES,
    MAX_CONNECT_LIVE_OPERATIONS, MAX_CONNECT_LOCAL_DECODED_BYTES, MAX_CONNECT_LOCAL_IPC_BYTES,
    MAX_CONNECT_LOCAL_ROWS, MAX_CONNECT_LOCAL_SCALAR_BYTES, MAX_CONNECT_REPLAY_OPERATION_BYTES,
    MAX_CONNECT_REPLAY_PROCESS_BYTES, MAX_CONNECT_RESULT_BYTES, MAX_CONNECT_RESULT_ROWS,
    MAX_CONNECT_STREAM_SENDERS, ReplayPoll, status_with_standard_details,
};
pub use runtime::{
    ConnectRuntimeError, ConnectRuntimeState, ConnectTombstoneError, MAX_CONNECT_SESSION_TOMBSTONES,
};
pub use service::{ConnectApplication, ConnectGrpcService, ConnectResponseStream};

use bytes::{Buf, Bytes};
use http::{Request, Response};
use http_body::Body as HttpBody;
use prost::Message;
use prost_reflect::{DescriptorPool, Kind as ReflectKind, MessageDescriptor};
use sparkless_server_core::{
    ConnectExecutionRequest, ConnectExecutionResult, EngineSessionHandle, ExecutionError,
};
use std::future::Future;
use std::sync::OnceLock;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::Stream;
use tonic::Status;
use tonic::body::Body;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder};
use tonic::server::NamedService;
use tonic::transport;
use tonic::transport::Server;
use tonic::transport::server::Connected;
use tower::Service;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Route a decoded Connect plan through the runtime-owned execution port.
pub fn execute_plan<S>(
    runtime: &sparkless_server_runtime::ServerRuntime<S>,
    session: &EngineSessionHandle,
    request: ConnectExecutionRequest,
) -> Result<ConnectExecutionResult, ExecutionError> {
    runtime.execute_connect_plan(session, request)
}

pub const MAX_CONNECT_PROTO_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_CONNECT_PROTO_DEPTH: usize = 64;
pub const MAX_CONNECT_PROTO_NODES: usize = 10_000;
pub const MAX_CONNECT_SCHEMA_FIELDS: usize = 256;

pub mod connect_proto {
    tonic::include_proto!("spark.connect");
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("spark_connect_descriptor");
}

/// Decoder that validates structural limits before Prost allocates generated messages.
pub struct BoundedProstCodec<T, U>(std::marker::PhantomData<(T, U)>);

impl<T, U> Default for BoundedProstCodec<T, U> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T, U> Codec for BoundedProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = tonic_prost::ProstEncoder<T>;
    type Decoder = BoundedProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        tonic_prost::ProstEncoder::default()
    }

    fn decoder(&mut self) -> Self::Decoder {
        BoundedProstDecoder(std::marker::PhantomData)
    }
}

pub struct BoundedProstDecoder<T>(std::marker::PhantomData<T>);

impl<T> Decoder for BoundedProstDecoder<T>
where
    T: Message + Default,
{
    type Item = T;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let message_name = std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .ok_or_else(|| Status::internal("could not name Connect protobuf request"))?;
        preflight_wire_message(&format!("spark.connect.{message_name}"), src.chunk())?;
        Message::decode(src)
            .map(Some)
            .map_err(|error: prost::DecodeError| Status::internal(error.to_string()))
    }

    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(8 * 1024, 32 * 1024)
    }
}

pub fn preflight_wire_message(message_name: &str, bytes: &[u8]) -> Result<(), Status> {
    if bytes.len() > MAX_CONNECT_PROTO_BYTES {
        return Err(Status::resource_exhausted(format!(
            "Connect protobuf exceeds the {MAX_CONNECT_PROTO_BYTES}-byte limit"
        )));
    }
    let descriptor = descriptor_pool()?
        .get_message_by_name(message_name)
        .ok_or_else(|| Status::internal("generated Connect request descriptor is missing"))?;
    scan_wire_message(&descriptor, bytes, 1, &mut ProtoLimits::default())
}

pub fn descriptor_pool() -> Result<&'static DescriptorPool, Status> {
    static POOL: OnceLock<Result<DescriptorPool, String>> = OnceLock::new();
    match POOL.get_or_init(|| {
        DescriptorPool::decode(connect_proto::FILE_DESCRIPTOR_SET)
            .map_err(|error| error.to_string())
    }) {
        Ok(pool) => Ok(pool),
        Err(_) => Err(Status::internal("could not load Spark Connect descriptors")),
    }
}

fn scan_wire_message(
    descriptor: &MessageDescriptor,
    bytes: &[u8],
    depth: usize,
    limits: &mut ProtoLimits,
) -> Result<(), Status> {
    validate_depth(depth)?;
    limits.nodes = limits.nodes.saturating_add(1);
    if limits.nodes > MAX_CONNECT_PROTO_NODES {
        return Err(Status::resource_exhausted(format!(
            "Connect protobuf exceeds {MAX_CONNECT_PROTO_NODES} message nodes"
        )));
    }
    if descriptor.full_name().ends_with(".StructField") {
        limits.schema_fields = limits.schema_fields.saturating_add(1);
        if limits.schema_fields > MAX_CONNECT_SCHEMA_FIELDS {
            return Err(Status::resource_exhausted(format!(
                "Connect schema exceeds {MAX_CONNECT_SCHEMA_FIELDS} fields"
            )));
        }
    }
    let mut cursor = 0;
    while cursor < bytes.len() {
        let key = read_varint(bytes, &mut cursor)?;
        let field_number = u32::try_from(key >> 3)
            .map_err(|_| Status::invalid_argument("protobuf field number is out of range"))?;
        let wire_type = (key & 7) as u8;
        if field_number == 0 || field_number > 536_870_911 {
            return Err(Status::invalid_argument("protobuf field number is invalid"));
        }
        let nested = descriptor
            .get_field(field_number)
            .and_then(|field| match field.kind() {
                ReflectKind::Message(message) => Some(message),
                _ => None,
            });
        if wire_type == 2 {
            let length = usize::try_from(read_varint(bytes, &mut cursor)?)
                .map_err(|_| Status::invalid_argument("protobuf field length is out of range"))?;
            let end = cursor
                .checked_add(length)
                .filter(|end| *end <= bytes.len())
                .ok_or_else(|| {
                    Status::invalid_argument("truncated protobuf length-delimited field")
                })?;
            if let Some(nested) = nested {
                scan_wire_message(&nested, &bytes[cursor..end], depth + 1, limits)?;
            }
            cursor = end;
        } else {
            skip_wire_value(bytes, &mut cursor, wire_type, field_number, depth)?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct ProtoLimits {
    nodes: usize,
    schema_fields: usize,
}

fn validate_depth(depth: usize) -> Result<(), Status> {
    if depth > MAX_CONNECT_PROTO_DEPTH {
        Err(Status::resource_exhausted(format!(
            "Connect protobuf nesting exceeds {MAX_CONNECT_PROTO_DEPTH}"
        )))
    } else {
        Ok(())
    }
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, Status> {
    let mut value = 0_u64;
    for index in 0..10 {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| Status::invalid_argument("truncated protobuf varint"))?;
        *cursor += 1;
        if index == 9 && byte > 1 {
            return Err(Status::invalid_argument(
                "protobuf varint overflows 64 bits",
            ));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Status::invalid_argument("protobuf varint is too long"))
}

fn skip_wire_value(
    bytes: &[u8],
    cursor: &mut usize,
    wire_type: u8,
    field_number: u32,
    depth: usize,
) -> Result<(), Status> {
    match wire_type {
        0 => {
            read_varint(bytes, cursor)?;
        }
        1 => advance_wire_bytes(bytes, cursor, 8)?,
        2 => {
            let length = usize::try_from(read_varint(bytes, cursor)?)
                .map_err(|_| Status::invalid_argument("protobuf field length is out of range"))?;
            advance_wire_bytes(bytes, cursor, length)?;
        }
        3 => skip_group(bytes, cursor, field_number, depth + 1)?,
        4 => {
            return Err(Status::invalid_argument(
                "unexpected protobuf end-group tag",
            ));
        }
        5 => advance_wire_bytes(bytes, cursor, 4)?,
        _ => return Err(Status::invalid_argument("protobuf wire type is invalid")),
    }
    Ok(())
}

fn skip_group(
    bytes: &[u8],
    cursor: &mut usize,
    group_field_number: u32,
    depth: usize,
) -> Result<(), Status> {
    validate_depth(depth)?;
    loop {
        let key = read_varint(bytes, cursor)?;
        let field_number = u32::try_from(key >> 3)
            .map_err(|_| Status::invalid_argument("protobuf field number is out of range"))?;
        let wire_type = (key & 7) as u8;
        if field_number == 0 || field_number > 536_870_911 {
            return Err(Status::invalid_argument("protobuf field number is invalid"));
        }
        if wire_type == 4 {
            if field_number != group_field_number {
                return Err(Status::invalid_argument(
                    "protobuf end-group field does not match",
                ));
            }
            return Ok(());
        }
        skip_wire_value(bytes, cursor, wire_type, field_number, depth)?;
    }
}

fn advance_wire_bytes(bytes: &[u8], cursor: &mut usize, length: usize) -> Result<(), Status> {
    *cursor = cursor
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| Status::invalid_argument("truncated protobuf field"))?;
    Ok(())
}

/// Serve a generated Connect service over a caller-owned, drainable incoming stream.
pub async fn serve_with_incoming_shutdown<S, I, F, IO, IE, ResBody>(
    service: S,
    incoming: I,
    shutdown: F,
) -> Result<(), transport::Error>
where
    S: NamedService
        + Service<Request<Body>, Response = Response<ResBody>, Error = std::convert::Infallible>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
    I: Stream<Item = Result<IO, IE>>,
    IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
    IE: Into<BoxError>,
    F: Future<Output = ()>,
    ResBody: HttpBody<Data = Bytes> + Send + 'static,
    ResBody::Error: Into<BoxError>,
{
    Server::builder()
        .add_service(service)
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await
}
