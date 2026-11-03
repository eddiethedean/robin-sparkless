//! Product-implemented Spark Connect application hooks and transport-owned gRPC service.

use crate::connect_proto;
use std::pin::Pin;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

pub type ConnectResponseStream = Pin<
    Box<dyn Stream<Item = Result<connect_proto::ExecutePlanResponse, Status>> + Send + 'static>,
>;

/// Application policy supplied by a server product. The transport crate owns the generated
/// Tonic service and delegates each RPC to these product-specific handlers.
#[tonic::async_trait]
pub trait ConnectApplication: Send + Sync + 'static {
    type ExecutePlanStream: Stream<Item = Result<connect_proto::ExecutePlanResponse, Status>>
        + Send
        + 'static;
    type ReattachExecuteStream: Stream<Item = Result<connect_proto::ExecutePlanResponse, Status>>
        + Send
        + 'static;

    async fn execute_plan(
        &self,
        request: Request<connect_proto::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status>;
    async fn analyze_plan(
        &self,
        request: Request<connect_proto::AnalyzePlanRequest>,
    ) -> Result<Response<connect_proto::AnalyzePlanResponse>, Status>;
    async fn config(
        &self,
        request: Request<connect_proto::ConfigRequest>,
    ) -> Result<Response<connect_proto::ConfigResponse>, Status>;
    async fn add_artifacts(
        &self,
        request: Request<tonic::Streaming<connect_proto::AddArtifactsRequest>>,
    ) -> Result<Response<connect_proto::AddArtifactsResponse>, Status>;
    async fn artifact_status(
        &self,
        request: Request<connect_proto::ArtifactStatusesRequest>,
    ) -> Result<Response<connect_proto::ArtifactStatusesResponse>, Status>;
    async fn interrupt(
        &self,
        request: Request<connect_proto::InterruptRequest>,
    ) -> Result<Response<connect_proto::InterruptResponse>, Status>;
    async fn reattach_execute(
        &self,
        request: Request<connect_proto::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status>;
    async fn release_execute(
        &self,
        request: Request<connect_proto::ReleaseExecuteRequest>,
    ) -> Result<Response<connect_proto::ReleaseExecuteResponse>, Status>;
    async fn release_session(
        &self,
        request: Request<connect_proto::ReleaseSessionRequest>,
    ) -> Result<Response<connect_proto::ReleaseSessionResponse>, Status>;
    async fn fetch_error_details(
        &self,
        request: Request<connect_proto::FetchErrorDetailsRequest>,
    ) -> Result<Response<connect_proto::FetchErrorDetailsResponse>, Status>;
}

/// Tonic's generated service wrapper. It keeps wire dispatch in this crate while a product
/// supplies session, catalog, analysis, and execution policy through [`ConnectApplication`].
pub struct ConnectGrpcService<A> {
    application: A,
}

impl<A> ConnectGrpcService<A> {
    pub fn new(application: A) -> Self {
        Self { application }
    }
}

#[tonic::async_trait]
impl<A: ConnectApplication> connect_proto::spark_connect_service_server::SparkConnectService
    for ConnectGrpcService<A>
{
    type ExecutePlanStream = A::ExecutePlanStream;
    type ReattachExecuteStream = A::ReattachExecuteStream;

    async fn execute_plan(
        &self,
        request: Request<connect_proto::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        self.application.execute_plan(request).await
    }

    async fn analyze_plan(
        &self,
        request: Request<connect_proto::AnalyzePlanRequest>,
    ) -> Result<Response<connect_proto::AnalyzePlanResponse>, Status> {
        self.application.analyze_plan(request).await
    }

    async fn config(
        &self,
        request: Request<connect_proto::ConfigRequest>,
    ) -> Result<Response<connect_proto::ConfigResponse>, Status> {
        self.application.config(request).await
    }

    async fn add_artifacts(
        &self,
        request: Request<tonic::Streaming<connect_proto::AddArtifactsRequest>>,
    ) -> Result<Response<connect_proto::AddArtifactsResponse>, Status> {
        self.application.add_artifacts(request).await
    }

    async fn artifact_status(
        &self,
        request: Request<connect_proto::ArtifactStatusesRequest>,
    ) -> Result<Response<connect_proto::ArtifactStatusesResponse>, Status> {
        self.application.artifact_status(request).await
    }

    async fn interrupt(
        &self,
        request: Request<connect_proto::InterruptRequest>,
    ) -> Result<Response<connect_proto::InterruptResponse>, Status> {
        self.application.interrupt(request).await
    }

    async fn reattach_execute(
        &self,
        request: Request<connect_proto::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        self.application.reattach_execute(request).await
    }

    async fn release_execute(
        &self,
        request: Request<connect_proto::ReleaseExecuteRequest>,
    ) -> Result<Response<connect_proto::ReleaseExecuteResponse>, Status> {
        self.application.release_execute(request).await
    }

    async fn release_session(
        &self,
        request: Request<connect_proto::ReleaseSessionRequest>,
    ) -> Result<Response<connect_proto::ReleaseSessionResponse>, Status> {
        self.application.release_session(request).await
    }

    async fn fetch_error_details(
        &self,
        request: Request<connect_proto::FetchErrorDetailsRequest>,
    ) -> Result<Response<connect_proto::FetchErrorDetailsResponse>, Status> {
        self.application.fetch_error_details(request).await
    }
}
