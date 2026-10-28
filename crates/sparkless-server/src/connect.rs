//! Spark Connect 4.0.5 gRPC adapter and bounded protobuf boundary.
//!
//! This slice establishes the public service and shared identity/admission contracts. Plan
//! translation, response production, and replay are deliberately implemented in later slices.

use crate::{ConnectReserveError, ConnectSessionIdentity, RequestFailure, Server, connect_proto};
use bytes::Buf;
use prost::Message;
use prost_reflect::{DescriptorPool, Kind as ReflectKind, MessageDescriptor};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::OnceLock;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder};
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

pub const MAX_CONNECT_PROTO_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_CONNECT_PROTO_DEPTH: usize = 64;
pub const MAX_CONNECT_PROTO_NODES: usize = 10_000;
pub const MAX_CONNECT_SCHEMA_FIELDS: usize = 256;

/// Tonic codec whose wire scanner rejects structurally excessive protobufs before Prost
/// allocates their generated message trees.
pub(crate) struct BoundedProstCodec<T, U>(PhantomData<(T, U)>);

#[derive(Clone, Debug)]
pub struct BoundedProstDecoder<T>(PhantomData<T>);

impl<T, U> Default for BoundedProstCodec<T, U> {
    fn default() -> Self {
        Self(PhantomData)
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
        BoundedProstDecoder(PhantomData)
    }
}

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

use connect_proto::spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer};

/// Shared native Spark Connect adapter. It owns no engine, catalog, session registry, or
/// admission pool; requests are resolved through the supplied server instance.
#[derive(Clone)]
pub struct ConnectService {
    server: Server,
}

impl Server {
    /// Construct the generated Spark Connect service with Sparkless' inbound and outbound
    /// protobuf message caps. Callers choose the listener address and lifecycle independently.
    pub fn connect_service(&self) -> SparkConnectServiceServer<ConnectService> {
        SparkConnectServiceServer::new(ConnectService {
            server: self.clone(),
        })
        .max_decoding_message_size(MAX_CONNECT_PROTO_BYTES)
        .max_encoding_message_size(MAX_CONNECT_PROTO_BYTES)
    }

    /// Serve the Connect adapter on a caller-owned address. This can run beside the HTTP
    /// listener using the same `Server` instance and therefore the same sessions and admission.
    pub async fn serve_connect_until<F>(
        &self,
        bind: SocketAddr,
        shutdown: F,
    ) -> Result<(), crate::ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tonic::transport::Server::builder()
            .add_service(self.connect_service())
            .serve_with_shutdown(bind, shutdown)
            .await
            .map_err(|error| crate::ServerError {
                message: format!("Spark Connect listener failed: {error}"),
            })
    }
}

type ConnectResponseStream = std::pin::Pin<
    Box<
        dyn tonic::codegen::tokio_stream::Stream<
                Item = Result<connect_proto::ExecutePlanResponse, Status>,
            > + Send
            + 'static,
    >,
>;

#[tonic::async_trait]
impl SparkConnectService for ConnectService {
    type ExecutePlanStream = ConnectResponseStream;
    type ReattachExecuteStream = ConnectResponseStream;

    async fn execute_plan(
        &self,
        request: Request<connect_proto::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let request = request.into_inner();
        validate_request_budget("ExecutePlanRequest", &request)?;
        if request.plan.is_none() {
            return Err(Status::invalid_argument(
                "ExecutePlanRequest.plan is required",
            ));
        }
        let fingerprint = Sha256::digest(request.encode_to_vec()).into();
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let operation_id = request
            .operation_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let operation = match self.server.inner.reserve_connect_operation(
            &identity,
            &operation_id,
            fingerprint,
        ) {
            Ok(operation) => operation,
            Err(ConnectReserveError::Request(error)) => return Err(status_from_failure(error)),
            Err(ConnectReserveError::Duplicate) => {
                return Err(Status::already_exists(
                    "INVALID_HANDLE.OPERATION_ALREADY_EXISTS: operation id is already retained",
                ));
            }
            Err(ConnectReserveError::Conflict) => {
                return Err(Status::invalid_argument(
                    "operation id is already bound to another Connect request or session generation",
                ));
            }
        };
        operation.reject(
            "UNIMPLEMENTED_CONNECT_PLAN",
            "Connect plan translation is implemented in delivery slice 3",
        );
        Err(Status::unimplemented(
            "Connect plan translation is implemented in delivery slice 3",
        ))
    }

    async fn analyze_plan(
        &self,
        request: Request<connect_proto::AnalyzePlanRequest>,
    ) -> Result<Response<connect_proto::AnalyzePlanResponse>, Status> {
        let request = request.into_inner();
        validate_request_budget("AnalyzePlanRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let _session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        Err(Status::unimplemented(
            "Connect analysis is implemented in delivery slice 3",
        ))
    }

    async fn config(
        &self,
        request: Request<connect_proto::ConfigRequest>,
    ) -> Result<Response<connect_proto::ConfigResponse>, Status> {
        let request = request.into_inner();
        validate_request_budget("ConfigRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let _session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        Err(Status::unimplemented(
            "Connect configuration is implemented in delivery slice 3",
        ))
    }

    async fn add_artifacts(
        &self,
        _request: Request<tonic::Streaming<connect_proto::AddArtifactsRequest>>,
    ) -> Result<Response<connect_proto::AddArtifactsResponse>, Status> {
        Err(Status::unimplemented(
            "AddArtifacts and cached local relations are outside the 4.18 Connect subset",
        ))
    }

    async fn artifact_status(
        &self,
        _request: Request<connect_proto::ArtifactStatusesRequest>,
    ) -> Result<Response<connect_proto::ArtifactStatusesResponse>, Status> {
        Err(Status::unimplemented(
            "ArtifactStatus and cached local relations are outside the 4.18 Connect subset",
        ))
    }

    async fn interrupt(
        &self,
        request: Request<connect_proto::InterruptRequest>,
    ) -> Result<Response<connect_proto::InterruptResponse>, Status> {
        let request = request.into_inner();
        validate_request_budget("InterruptRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let _session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        Err(Status::unimplemented(
            "Connect operation interruption is implemented in delivery slice 5",
        ))
    }

    async fn reattach_execute(
        &self,
        request: Request<connect_proto::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        let request = request.into_inner();
        validate_request_budget("ReattachExecuteRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let _session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        validate_uuid("operation_id", &request.operation_id)?;
        Err(Status::unimplemented(
            "Connect response reattachment is implemented in delivery slice 5",
        ))
    }

    async fn release_execute(
        &self,
        request: Request<connect_proto::ReleaseExecuteRequest>,
    ) -> Result<Response<connect_proto::ReleaseExecuteResponse>, Status> {
        let request = request.into_inner();
        validate_request_budget("ReleaseExecuteRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let _session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        validate_uuid("operation_id", &request.operation_id)?;
        Err(Status::unimplemented(
            "Connect operation release is implemented in delivery slice 5",
        ))
    }

    async fn release_session(
        &self,
        request: Request<connect_proto::ReleaseSessionRequest>,
    ) -> Result<Response<connect_proto::ReleaseSessionResponse>, Status> {
        let request = request.into_inner();
        validate_request_budget("ReleaseSessionRequest", &request)?;
        let identity = session_identity(request.user_context, request.session_id, None)?;
        let server_side_session_id = self
            .server
            .inner
            .release_connect_session(&identity, request.allow_reconnect)
            .map_err(status_from_failure)?;
        Ok(Response::new(connect_proto::ReleaseSessionResponse {
            session_id: identity.client_session_id,
            server_side_session_id,
        }))
    }

    async fn fetch_error_details(
        &self,
        request: Request<connect_proto::FetchErrorDetailsRequest>,
    ) -> Result<Response<connect_proto::FetchErrorDetailsResponse>, Status> {
        let request = request.into_inner();
        validate_request_budget("FetchErrorDetailsRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let _session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        Err(Status::unimplemented(
            "Connect error-detail retrieval is implemented in delivery slice 5",
        ))
    }
}

fn session_identity(
    user_context: Option<connect_proto::UserContext>,
    session_id: String,
    observed_server_session_id: Option<String>,
) -> Result<ConnectSessionIdentity, Status> {
    let user_context =
        user_context.ok_or_else(|| Status::invalid_argument("user_context is required"))?;
    validate_user_id(&user_context.user_id)?;
    validate_uuid("session_id", &session_id)?;
    if let Some(observed) = observed_server_session_id.as_deref() {
        validate_uuid("client_observed_server_side_session_id", observed)?;
    }
    Ok(ConnectSessionIdentity {
        user_id: user_context.user_id,
        client_session_id: session_id,
        observed_server_session_id,
    })
}

fn validate_user_id(value: &str) -> Result<(), Status> {
    if value.is_empty() || value.len() > 128 {
        return Err(Status::invalid_argument(
            "user_id must be 1-128 UTF-8 bytes",
        ));
    }
    Ok(())
}

fn validate_uuid(name: &str, value: &str) -> Result<(), Status> {
    Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| Status::invalid_argument(format!("{name} must be a UUID string")))
}

fn status_from_failure(error: RequestFailure) -> Status {
    let code = match error.status.as_u16() {
        400 => Code::InvalidArgument,
        404 => Code::NotFound,
        409 => Code::FailedPrecondition,
        410 => Code::FailedPrecondition,
        429 => Code::ResourceExhausted,
        503 => Code::Unavailable,
        _ => Code::Internal,
    };
    Status::new(code, format!("{}: {}", error.name, error.message))
}

fn validate_request_budget<M: Message>(name: &str, request: &M) -> Result<(), Status> {
    if request.encoded_len() > MAX_CONNECT_PROTO_BYTES {
        return Err(Status::resource_exhausted(format!(
            "{name} exceeds the {MAX_CONNECT_PROTO_BYTES}-byte protobuf limit"
        )));
    }
    let bytes = request.encode_to_vec();
    let message_name = format!("spark.connect.{name}");
    preflight_wire_message(&message_name, &bytes)
}

fn descriptor_pool() -> Result<&'static DescriptorPool, Status> {
    static POOL: OnceLock<Result<DescriptorPool, String>> = OnceLock::new();
    match POOL.get_or_init(|| {
        DescriptorPool::decode(connect_proto::FILE_DESCRIPTOR_SET)
            .map_err(|error| error.to_string())
    }) {
        Ok(pool) => Ok(pool),
        Err(_) => Err(Status::internal("could not load Spark Connect descriptors")),
    }
}

fn preflight_wire_message(message_name: &str, bytes: &[u8]) -> Result<(), Status> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectReserveError, ServerConfig};
    use connect_proto::data_type::{Array, Boolean, Kind, Struct, StructField};
    use std::time::Duration;

    #[test]
    fn descriptor_set_has_the_public_service_and_bounds_nested_requests() {
        let service = descriptor_pool()
            .unwrap()
            .get_service_by_name("spark.connect.SparkConnectService")
            .expect("public Spark service descriptor");
        let methods = service
            .methods()
            .map(|method| method.name().to_string())
            .collect::<std::collections::HashSet<_>>();
        for required in [
            "ExecutePlan",
            "AnalyzePlan",
            "Config",
            "AddArtifacts",
            "ArtifactStatus",
            "Interrupt",
            "ReattachExecute",
            "ReleaseExecute",
            "ReleaseSession",
            "FetchErrorDetails",
        ] {
            assert!(methods.contains(required), "missing RPC {required}");
        }

        let descriptor = descriptor_pool()
            .unwrap()
            .get_message_by_name("spark.connect.ExecutePlanRequest")
            .unwrap();
        assert_eq!(descriptor.full_name(), "spark.connect.ExecutePlanRequest");
    }

    #[test]
    fn captured_public_client_request_decodes_with_the_generated_schema() {
        let bytes = include_bytes!(
            "../../../tests/connect/fixtures/pyspark-client-4.0.5/execute-range-filter-count.pb"
        );
        let request = connect_proto::ExecutePlanRequest::decode(bytes.as_slice())
            .expect("pinned public-client fixture decodes");
        validate_request_budget("ExecutePlanRequest", &request)
            .expect("captured request is within the frozen structural budgets");
        assert_eq!(request.session_id, "11111111-1111-4111-8111-111111111111");
        assert!(request.plan.is_some());
        assert_eq!(
            request.operation_id.as_deref(),
            Some("22222222-2222-4222-8222-222222222222")
        );
        let mut request_with_unknown_field = bytes.to_vec();
        request_with_unknown_field.extend_from_slice(&[0xa0, 0x06, 0x01]);
        preflight_wire_message(
            "spark.connect.ExecutePlanRequest",
            &request_with_unknown_field,
        )
        .expect("unknown public protobuf fields remain skippable");
    }

    #[test]
    fn recursive_schema_depth_and_schema_field_limits_are_rejected() {
        let deep_type = nested_data_type(MAX_CONNECT_PROTO_DEPTH / 2 + 1);
        assert_eq!(
            preflight_wire_message("spark.connect.DataType", &deep_type.encode_to_vec())
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );

        let fields = (0..MAX_CONNECT_SCHEMA_FIELDS + 1)
            .map(|index| StructField {
                name: format!("c{index}"),
                data_type: Some(connect_proto::DataType {
                    kind: Some(Kind::Boolean(Boolean {
                        type_variation_reference: 0,
                    })),
                }),
                nullable: true,
                metadata: None,
            })
            .collect();
        let wide_type = connect_proto::DataType {
            kind: Some(Kind::Struct(Struct {
                fields,
                type_variation_reference: 0,
            })),
        };
        assert_eq!(
            preflight_wire_message("spark.connect.DataType", &wide_type.encode_to_vec())
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
    }

    #[test]
    fn request_byte_and_message_node_limits_are_rejected() {
        let too_large = connect_proto::ExecutePlanRequest {
            client_type: Some("x".repeat(MAX_CONNECT_PROTO_BYTES + 1)),
            ..Default::default()
        };
        assert_eq!(
            validate_request_budget("ExecutePlanRequest", &too_large)
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );

        let many_nodes = connect_proto::ExecutePlanRequest {
            request_options: (0..MAX_CONNECT_PROTO_NODES)
                .map(|_| connect_proto::execute_plan_request::RequestOption {
                    request_option: None,
                })
                .collect(),
            ..Default::default()
        };
        assert_eq!(
            validate_request_budget("ExecutePlanRequest", &many_nodes)
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        let many_nodes_wire = many_nodes.encode_to_vec();
        assert_eq!(
            preflight_wire_message("spark.connect.ExecutePlanRequest", &many_nodes_wire)
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            preflight_wire_message("spark.connect.ExecutePlanRequest", &[0x0a, 0x05, b'x'])
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn connect_session_identity_isolated_and_release_is_tombstoned() {
        let server = Server::new(ServerConfig::default()).unwrap();
        let session_id = Uuid::new_v4().to_string();
        let first_identity = identity("álîce", &session_id, None);
        let lease = server.inner.connect_session_lease(&first_identity).unwrap();
        let original_server_id = lease.0.server_side_session_id.clone();
        let generation = lease.0.generation;
        drop(lease);

        let same_identity = identity("álîce", &session_id, Some(&original_server_id));
        let next_lease = server.inner.connect_session_lease(&same_identity).unwrap();
        assert_eq!(next_lease.0.generation, generation);
        drop(next_lease);

        let separate_user = identity("bob", &session_id, None);
        let other_lease = server.inner.connect_session_lease(&separate_user).unwrap();
        assert_ne!(other_lease.0.generation, generation);
        drop(other_lease);

        let replacement_identity = identity("charlie", &Uuid::new_v4().to_string(), None);
        let old_lease = server
            .inner
            .connect_session_lease(&replacement_identity)
            .unwrap();
        let old_server_id = old_lease.0.server_side_session_id.clone();
        let old_generation = old_lease.0.generation;
        drop(old_lease);
        server
            .inner
            .close_session(&replacement_identity.registry_key())
            .unwrap();
        let replacement = server
            .inner
            .connect_session_lease(&replacement_identity)
            .unwrap();
        assert_ne!(replacement.0.generation, old_generation);
        assert_ne!(replacement.0.server_side_session_id, old_server_id);
        drop(replacement);
        let stale_identity = identity(
            "charlie",
            &replacement_identity.client_session_id,
            Some(&old_server_id),
        );
        assert_eq!(
            server
                .inner
                .connect_session_lease(&stale_identity)
                .err()
                .unwrap()
                .name,
            "StaleSessionIdentity"
        );

        let released_id = server
            .inner
            .release_connect_session(&first_identity, false)
            .unwrap();
        assert_eq!(released_id, original_server_id);
        assert_eq!(
            server
                .inner
                .connect_session_lease(&first_identity)
                .err()
                .unwrap()
                .name,
            "SessionReleased"
        );
        assert_eq!(
            server
                .inner
                .release_connect_session(&first_identity, false)
                .unwrap(),
            original_server_id
        );
        assert_eq!(
            server
                .inner
                .release_connect_session(&first_identity, true)
                .err()
                .unwrap()
                .name,
            "UnsupportedSessionReconnect"
        );
    }

    #[tokio::test]
    async fn connect_operations_share_admission_and_retain_duplicate_protection() {
        let server = Server::new(ServerConfig::default()).unwrap();
        let identity = identity("user", &Uuid::new_v4().to_string(), None);
        let operation_id = Uuid::new_v4().to_string();
        let first = server
            .inner
            .reserve_connect_operation(&identity, &operation_id, [7; 32])
            .unwrap();
        first.reject("UNIMPLEMENTED", "slice boundary");
        assert_eq!(
            server
                .inner
                .admission
                .accepted
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        drop(first);
        assert_eq!(
            server
                .inner
                .admission
                .accepted
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        let tickets = (0..server.inner.admission.max_accepted)
            .map(|_| server.inner.reserve_admission().unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(
            server
                .inner
                .reserve_connect_operation(&identity, &operation_id, [7; 32]),
            Err(ConnectReserveError::Duplicate)
        ));
        drop(tickets);
        assert!(matches!(
            server
                .inner
                .reserve_connect_operation(&identity, &operation_id, [8; 32]),
            Err(ConnectReserveError::Conflict)
        ));
    }

    #[test]
    fn connect_registry_prefixes_are_reserved_from_http_ids() {
        let identity = identity("user", &Uuid::new_v4().to_string(), None);
        assert!(crate::validate_http_session_identifier(&identity.registry_key()).is_err());
        let operation_key = crate::connect_operation_key(&identity, &Uuid::new_v4().to_string());
        assert!(crate::validate_http_operation_identifier(&operation_key).is_err());
        assert!(crate::validate_http_session_identifier("http:session").is_ok());
        assert!(crate::validate_http_operation_identifier("http:operation").is_ok());
    }

    #[test]
    fn concurrent_session_and_operation_admission_cannot_cross_release() {
        let server = Server::new(ServerConfig::default()).unwrap();
        for _ in 0..500 {
            let session_id = Uuid::new_v4().to_string();
            let original_identity = identity("race-user", &session_id, None);
            let session_key = original_identity.registry_key();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));

            let acquire_server = server.clone();
            let acquire_identity = identity("race-user", &session_id, None);
            let acquire_barrier = std::sync::Arc::clone(&barrier);
            let acquire = std::thread::spawn(move || {
                acquire_barrier.wait();
                acquire_server
                    .inner
                    .connect_session_lease(&acquire_identity)
            });

            let release_server = server.clone();
            let release_identity = identity("race-user", &session_id, None);
            let release_barrier = std::sync::Arc::clone(&barrier);
            let release = std::thread::spawn(move || {
                release_barrier.wait();
                release_server
                    .inner
                    .release_connect_session(&release_identity, false)
            });

            barrier.wait();
            let lease = acquire.join().unwrap();
            release.join().unwrap().unwrap();
            match lease {
                Ok(lease) => assert!(lease.0.closing.load(std::sync::atomic::Ordering::Acquire)),
                Err(error) => assert_eq!(error.name, "SessionReleased"),
            }
            assert!(
                !server
                    .inner
                    .sessions
                    .lock()
                    .unwrap()
                    .contains_key(&session_key)
            );

            let operation_identity = identity("race-user", &Uuid::new_v4().to_string(), None);
            let operation_session_key = operation_identity.registry_key();
            let operation_id = Uuid::new_v4().to_string();
            let operation_barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
            let reserve_server = server.clone();
            let reserve_identity =
                identity("race-user", &operation_identity.client_session_id, None);
            let reserve_id = operation_id.clone();
            let reserve_barrier = std::sync::Arc::clone(&operation_barrier);
            let reserve = std::thread::spawn(move || {
                reserve_barrier.wait();
                reserve_server.inner.reserve_connect_operation(
                    &reserve_identity,
                    &reserve_id,
                    [3; 32],
                )
            });
            let release_server = server.clone();
            let release_identity =
                identity("race-user", &operation_identity.client_session_id, None);
            let release_barrier = std::sync::Arc::clone(&operation_barrier);
            let release = std::thread::spawn(move || {
                release_barrier.wait();
                release_server
                    .inner
                    .release_connect_session(&release_identity, false)
            });
            operation_barrier.wait();
            let operation = reserve.join().unwrap();
            release.join().unwrap().unwrap();
            match operation {
                Ok(operation) => {
                    assert!(
                        operation
                            ._session
                            .0
                            .closing
                            .load(std::sync::atomic::Ordering::Acquire)
                    );
                    operation.reject("UNIMPLEMENTED", "test operation");
                }
                Err(ConnectReserveError::Request(error)) => {
                    assert_eq!(error.name, "SessionReleased");
                }
                Err(error) => panic!("unexpected Connect reservation error: {error:?}"),
            }
            assert!(
                !server
                    .inner
                    .sessions
                    .lock()
                    .unwrap()
                    .contains_key(&operation_session_key)
            );
        }
    }

    #[tokio::test]
    async fn generated_rpc_uses_invalid_session_and_duplicate_operation_statuses() {
        let server = Server::new(ServerConfig::default()).unwrap();
        let service = ConnectService { server };
        let request = connect_proto::ExecutePlanRequest {
            session_id: Uuid::new_v4().to_string(),
            user_context: Some(connect_proto::UserContext {
                user_id: "fixture-user".into(),
                ..Default::default()
            }),
            operation_id: Some(Uuid::new_v4().to_string()),
            plan: Some(connect_proto::Plan {
                op_type: Some(connect_proto::plan::OpType::Root(
                    connect_proto::Relation::default(),
                )),
            }),
            ..Default::default()
        };
        let first = service
            .execute_plan(Request::new(request.clone()))
            .await
            .err()
            .unwrap();
        assert_eq!(first.code(), Code::Unimplemented);
        let duplicate = service
            .execute_plan(Request::new(request.clone()))
            .await
            .err()
            .unwrap();
        assert_eq!(duplicate.code(), Code::AlreadyExists);
        assert!(
            duplicate
                .message()
                .contains("INVALID_HANDLE.OPERATION_ALREADY_EXISTS")
        );

        let mut conflicting = request;
        conflicting.client_type = Some("different request bytes".into());
        let conflict = service
            .execute_plan(Request::new(conflicting))
            .await
            .err()
            .unwrap();
        assert_eq!(conflict.code(), Code::InvalidArgument);

        let malformed = connect_proto::ExecutePlanRequest {
            session_id: "not-a-uuid".into(),
            user_context: Some(connect_proto::UserContext {
                user_id: "fixture-user".into(),
                ..Default::default()
            }),
            plan: Some(connect_proto::Plan::default()),
            ..Default::default()
        };
        let malformed_status = service
            .execute_plan(Request::new(malformed))
            .await
            .err()
            .unwrap();
        assert_eq!(malformed_status.code(), Code::InvalidArgument);

        let unsupported = service
            .artifact_status(Request::new(
                connect_proto::ArtifactStatusesRequest::default(),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(unsupported.code(), Code::Unimplemented);
        assert!(
            unsupported
                .message()
                .contains("outside the 4.18 Connect subset")
        );
    }

    #[tokio::test]
    async fn generated_grpc_listener_enforces_the_wire_message_limit() {
        let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);

        let server = Server::new(ServerConfig::default()).unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .serve_connect_until(address, async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(2));
        let channel = endpoint.connect().await.unwrap();
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.unwrap();
        let request = connect_proto::ExecutePlanRequest {
            client_type: Some("x".repeat(MAX_CONNECT_PROTO_BYTES + 1)),
            ..Default::default()
        };
        let status = client
            .server_streaming(
                Request::new(request),
                hyper::http::uri::PathAndQuery::from_static(
                    "/spark.connect.SparkConnectService/ExecutePlan",
                ),
                tonic_prost::ProstCodec::<
                    connect_proto::ExecutePlanRequest,
                    connect_proto::ExecutePlanResponse,
                >::default(),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(
            status.code(),
            Code::OutOfRange,
            "unexpected gRPC size status: {}",
            status.message()
        );

        let _ = shutdown_tx.send(());
        server_task.await.unwrap().unwrap();
    }

    fn identity(
        user_id: &str,
        client_session_id: &str,
        observed_server_session_id: Option<&str>,
    ) -> ConnectSessionIdentity {
        ConnectSessionIdentity {
            user_id: user_id.into(),
            client_session_id: client_session_id.into(),
            observed_server_session_id: observed_server_session_id.map(str::to_string),
        }
    }

    fn nested_data_type(depth: usize) -> connect_proto::DataType {
        let mut data_type = connect_proto::DataType {
            kind: Some(Kind::Boolean(Boolean {
                type_variation_reference: 0,
            })),
        };
        for _ in 0..depth {
            data_type = connect_proto::DataType {
                kind: Some(Kind::Array(Box::new(Array {
                    element_type: Some(Box::new(data_type)),
                    contains_null: true,
                    type_variation_reference: 0,
                }))),
            };
        }
        data_type
    }
}
