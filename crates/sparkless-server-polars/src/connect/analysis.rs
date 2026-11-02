//! Supported Spark Connect analysis requests for the frozen 4.0.5 subset.

use crate::connect_proto as proto;
use proto::analyze_plan_request::Analyze;
use proto::analyze_plan_response::Result as AnalyzeResult;
use robin_sparkless::SparkSession;
use tonic::Status;

use super::plan::{parse_ddl_schema, schema_to_proto, translate_plan};

pub(super) fn response(
    session_id: &str,
    server_side_session_id: &str,
    session: &SparkSession,
    analyze: Option<Analyze>,
) -> Result<proto::AnalyzePlanResponse, Status> {
    let analyze = analyze
        .ok_or_else(|| Status::invalid_argument("AnalyzePlanRequest.analyze is required"))?;
    let result = match analyze {
        Analyze::Schema(schema) => {
            let plan = schema
                .plan
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("AnalyzePlan.Schema.plan is required"))?;
            let translated = translate_plan(session, plan)?;
            AnalyzeResult::Schema(proto::analyze_plan_response::Schema {
                schema: Some(schema_to_proto(&translated.schema)?),
            })
        }
        Analyze::DdlParse(ddl) => {
            let schema = parse_ddl_schema(&ddl.ddl_string)?;
            AnalyzeResult::DdlParse(proto::analyze_plan_response::DdlParse {
                parsed: Some(schema_to_proto(&schema)?),
            })
        }
        Analyze::SparkVersion(_) => {
            AnalyzeResult::SparkVersion(proto::analyze_plan_response::SparkVersion {
                version: "4.0.5".to_string(),
            })
        }
        Analyze::IsLocal(is_local) => {
            let plan = is_local
                .plan
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("AnalyzePlan.IsLocal.plan is required"))?;
            let translated = translate_plan(session, plan)?;
            translated.dataframe.schema_engine().map_err(|error| {
                Status::failed_precondition(format!("CONNECT_SCHEMA_ANALYSIS_FAILED: {error}"))
            })?;
            AnalyzeResult::IsLocal(proto::analyze_plan_response::IsLocal { is_local: true })
        }
        Analyze::IsStreaming(is_streaming) => {
            let plan = is_streaming.plan.as_ref().ok_or_else(|| {
                Status::invalid_argument("AnalyzePlan.IsStreaming.plan is required")
            })?;
            let translated = translate_plan(session, plan)?;
            translated.dataframe.schema_engine().map_err(|error| {
                Status::failed_precondition(format!("CONNECT_SCHEMA_ANALYSIS_FAILED: {error}"))
            })?;
            AnalyzeResult::IsStreaming(proto::analyze_plan_response::IsStreaming {
                is_streaming: false,
            })
        }
        Analyze::InputFiles(input_files) => {
            let plan = input_files.plan.as_ref().ok_or_else(|| {
                Status::invalid_argument("AnalyzePlan.InputFiles.plan is required")
            })?;
            let translated = translate_plan(session, plan)?;
            translated.dataframe.schema_engine().map_err(|error| {
                Status::failed_precondition(format!("CONNECT_SCHEMA_ANALYSIS_FAILED: {error}"))
            })?;
            AnalyzeResult::InputFiles(proto::analyze_plan_response::InputFiles {
                files: Vec::new(),
            })
        }
        _ => {
            return Err(Status::unimplemented(
                "UNSUPPORTED_CONNECT_ANALYSIS: requested analysis operation is outside the 4.18 subset",
            ));
        }
    };
    Ok(proto::AnalyzePlanResponse {
        session_id: session_id.to_string(),
        server_side_session_id: server_side_session_id.to_string(),
        result: Some(result),
    })
}
