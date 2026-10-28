//! Frozen Spark SQL configuration profile used by the captured 4.0.5 public client.

use crate::connect_proto as proto;
use proto::config_request::{self, operation::OpType};
use robin_sparkless::SparkSession;
use std::collections::BTreeMap;
use tonic::{Code, Status};

const CONFIG_DEFAULTS: &[(&str, &str)] = &[
    ("spark.sql.ansi.enabled", "true"),
    ("spark.sql.caseSensitive", "false"),
    ("spark.sql.execution.arrow.useLargeVarTypes", "false"),
    (
        "spark.sql.execution.pandas.convertToArrowArraySafely",
        "false",
    ),
    ("spark.sql.execution.pandas.inferPandasDictAsMap", "false"),
    ("spark.sql.pyspark.inferNestedDictAsStruct.enabled", "false"),
    (
        "spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled",
        "false",
    ),
    (
        "spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled",
        "false",
    ),
    ("spark.sql.session.localRelationCacheThreshold", "67108864"),
    ("spark.sql.session.timeZone", "UTC"),
    ("spark.sql.timestampType", "TIMESTAMP_LTZ"),
];

pub(super) fn response(
    session_id: &str,
    server_side_session_id: &str,
    session: &SparkSession,
    operation: Option<config_request::Operation>,
) -> Result<proto::ConfigResponse, Status> {
    let operation =
        operation.ok_or_else(|| Status::invalid_argument("ConfigRequest.operation is required"))?;
    let operation = operation
        .op_type
        .ok_or_else(|| Status::invalid_argument("ConfigRequest.operation.op_type is required"))?;
    let profile = effective_profile(session);
    let mut response = proto::ConfigResponse {
        session_id: session_id.to_string(),
        server_side_session_id: server_side_session_id.to_string(),
        pairs: Vec::new(),
        warnings: Vec::new(),
    };
    match operation {
        OpType::Get(get) => {
            response.pairs = get
                .keys
                .into_iter()
                .map(|key| {
                    validate_key(&key)?;
                    let value = profile
                        .get(&key)
                        .cloned()
                        .ok_or_else(|| config_not_found(&key))?;
                    Ok(proto::KeyValue {
                        key,
                        value: Some(value),
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;
        }
        OpType::GetWithDefault(get) => {
            response.pairs = get
                .pairs
                .into_iter()
                .map(|pair| {
                    validate_key(&pair.key)?;
                    let value = profile.get(&pair.key).cloned().or(pair.value);
                    Ok(proto::KeyValue {
                        key: pair.key,
                        value,
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;
        }
        OpType::GetOption(get) => {
            response.pairs = get
                .keys
                .into_iter()
                .map(|key| {
                    validate_key(&key)?;
                    Ok(proto::KeyValue {
                        value: profile.get(&key).cloned(),
                        key,
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;
        }
        OpType::GetAll(get) => {
            let prefix = get.prefix.unwrap_or_default();
            response.pairs = profile
                .into_iter()
                .filter_map(|(key, value)| {
                    key.strip_prefix(&prefix).map(|key| proto::KeyValue {
                        key: key.to_string(),
                        value: Some(value),
                    })
                })
                .collect();
        }
        OpType::IsModifiable(check) => {
            response.pairs = check
                .keys
                .into_iter()
                .map(|key| {
                    validate_key(&key)?;
                    Ok(proto::KeyValue {
                        key,
                        value: Some("false".to_string()),
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;
        }
        OpType::Set(set) => {
            for pair in set.pairs {
                validate_key(&pair.key)?;
                let requested = pair.value.ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "Config.Set requires a value for {:?}",
                        pair.key
                    ))
                })?;
                if profile.get(&pair.key) == Some(&requested) {
                    // Idempotently accepting the frozen effective value does not mutate the
                    // session and lets clients repeat their pinned profile during initialization.
                    continue;
                }
                let message = format!(
                    "configuration {:?} is not modifiable by the Sparkless Connect profile",
                    pair.key
                );
                if set.silent.unwrap_or(false) {
                    response.warnings.push(message);
                } else {
                    return Err(Status::new(Code::FailedPrecondition, message));
                }
            }
        }
        OpType::Unset(unset) => {
            for key in unset.keys {
                validate_key(&key)?;
                // Values exposed by this service are process/session profile defaults and cannot
                // be changed. Unsetting one therefore restores the same effective value.
            }
        }
    }
    Ok(response)
}

fn effective_profile(session: &SparkSession) -> BTreeMap<String, String> {
    CONFIG_DEFAULTS
        .iter()
        .map(|(key, default)| {
            (
                (*key).to_string(),
                session
                    .get_config()
                    .get(*key)
                    .cloned()
                    .unwrap_or_else(|| (*default).to_string()),
            )
        })
        .collect()
}

fn validate_key(key: &str) -> Result<(), Status> {
    if key.is_empty() || key.len() > 256 || key.chars().any(char::is_control) {
        return Err(Status::invalid_argument(
            "Connect config keys must contain 1-256 non-control bytes",
        ));
    }
    Ok(())
}

fn config_not_found(key: &str) -> Status {
    Status::not_found(format!(
        "CONFIG_NOT_FOUND: no supported Spark Connect configuration named {key:?}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect_proto::config_request::operation::OpType;

    fn session() -> SparkSession {
        SparkSession::builder()
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.caseSensitive", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .get_or_create()
    }

    fn operation(op_type: OpType) -> config_request::Operation {
        config_request::Operation {
            op_type: Some(op_type),
        }
    }

    #[test]
    fn captured_config_operations_report_reference_defaults_and_missing_semantics() {
        let session = session();
        let default_values = response(
            "client-session",
            "server-session",
            &session,
            Some(operation(OpType::Get(config_request::Get {
                keys: vec![
                    "spark.sql.timestampType".into(),
                    "spark.sql.session.timeZone".into(),
                    "spark.sql.session.localRelationCacheThreshold".into(),
                    "spark.sql.execution.pandas.convertToArrowArraySafely".into(),
                    "spark.sql.execution.pandas.inferPandasDictAsMap".into(),
                    "spark.sql.pyspark.inferNestedDictAsStruct.enabled".into(),
                    "spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled".into(),
                    "spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled".into(),
                    "spark.sql.execution.arrow.useLargeVarTypes".into(),
                ],
            }))),
        )
        .unwrap();
        assert_eq!(default_values.pairs.len(), 9);
        assert_eq!(
            default_values.pairs[0].value.as_deref(),
            Some("TIMESTAMP_LTZ")
        );
        assert_eq!(default_values.pairs[2].value.as_deref(), Some("67108864"));

        let optional = response(
            "client-session",
            "server-session",
            &session,
            Some(operation(OpType::GetOption(config_request::GetOption {
                keys: vec!["sparkless.contract.missing".into()],
            }))),
        )
        .unwrap();
        assert_eq!(optional.pairs.len(), 1);
        assert_eq!(optional.pairs[0].value, None);

        let fallback = response(
            "client-session",
            "server-session",
            &session,
            Some(operation(OpType::GetWithDefault(
                config_request::GetWithDefault {
                    pairs: vec![proto::KeyValue {
                        key: "sparkless.contract.missing".into(),
                        value: Some("fallback".into()),
                    }],
                },
            ))),
        )
        .unwrap();
        assert_eq!(fallback.pairs[0].value.as_deref(), Some("fallback"));
    }

    #[test]
    fn static_values_are_not_modifiable_but_same_value_set_is_idempotent() {
        let session = session();
        let modifiable = response(
            "client-session",
            "server-session",
            &session,
            Some(operation(OpType::IsModifiable(
                config_request::IsModifiable {
                    keys: vec!["spark.sql.ansi.enabled".into()],
                },
            ))),
        )
        .unwrap();
        assert_eq!(modifiable.pairs[0].value.as_deref(), Some("false"));

        response(
            "client-session",
            "server-session",
            &session,
            Some(operation(OpType::Set(config_request::Set {
                pairs: vec![proto::KeyValue {
                    key: "spark.sql.ansi.enabled".into(),
                    value: Some("true".into()),
                }],
                silent: None,
            }))),
        )
        .expect("same effective value is a no-op");

        let status = response(
            "client-session",
            "server-session",
            &session,
            Some(operation(OpType::Set(config_request::Set {
                pairs: vec![proto::KeyValue {
                    key: "spark.sql.session.timeZone".into(),
                    value: Some("America/Los_Angeles".into()),
                }],
                silent: None,
            }))),
        )
        .unwrap_err();
        assert_eq!(status.code(), Code::FailedPrecondition);
    }
}
