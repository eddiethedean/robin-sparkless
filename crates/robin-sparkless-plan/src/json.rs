use crate::model::{
    BinaryInput, ExpressionKind, GroupInput, LogicalPlan, OperationKind, PlanExpression,
    PlanOperation, PlanPayload,
};
use robin_sparkless_core::{EngineError, ErrorCategory, ErrorDetail, ErrorRetryability};
use serde_json::{Map, Value};

impl LogicalPlan {
    /// Decode and validate the full plan before an executor can access a source.
    pub fn decode(steps: &[Value]) -> Result<Self, EngineError> {
        let operations = steps
            .iter()
            .enumerate()
            .map(|(index, step)| decode_operation(index, step))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { operations })
    }
}

fn invalid(message: impl Into<String>) -> EngineError {
    EngineError::from_detail(
        ErrorDetail::new(ErrorCategory::InvalidInput, "INVALID_PLAN", message)
            .with_retryability(ErrorRetryability::Terminal),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decodes_supported_operations_into_operation_specific_payloads() {
        let steps = vec![
            json!({"op": "stop"}),
            json!({"op": "filter", "payload": {"op": "gt", "left": {"col": "id"}, "right": {"lit": 1}}}),
            json!({"op": "select", "payload": ["id"]}),
            json!({"op": "limit", "payload": {"n": 0}}),
            json!({"op": "offset", "payload": {}}),
            json!({"op": "orderBy", "payload": {"columns": ["id DESC"], "nulls_last": [true]}}),
            json!({"op": "distinct"}),
            json!({"op": "drop", "payload": {"columns": ["unused"]}}),
            json!({"op": "withColumnRenamed", "payload": {"old": "id", "new": "key"}}),
            json!({"op": "withColumn", "payload": {"name": "next", "expr": {"op": "add", "left": {"col": "id"}, "right": {"lit": 1}}}}),
            json!({"op": "groupBy", "payload": {"group_by": ["id"], "aggs": []}}),
            json!({"op": "join", "payload": {"on": "id", "how": "inner", "other_data": [[2]], "other_schema": [{"name": "id", "type": "long"}]}}),
            json!({"op": "union", "payload": {"other_data": [[2]], "other_schema": [{"name": "id", "type": "long"}]}}),
            json!({"op": "unionByName", "payload": {"other_data": [[2]], "other_schema": [{"name": "id", "type": "long"}]}}),
            json!({"op": "crossJoin", "payload": {"other_data": [[2]], "other_schema": [{"name": "id", "type": "long"}]}}),
            json!({"op": "rollup", "payload": {"group_by": ["id"], "aggs": []}}),
            json!({"op": "cube", "payload": {"group_by": ["id"], "aggs": []}}),
        ];

        let plan = LogicalPlan::decode(&steps).expect("all supported operation shapes decode");
        assert_eq!(plan.operations.len(), steps.len());
        assert!(matches!(plan.operations[0].payload(), PlanPayload::Empty));
        assert!(matches!(
            plan.operations[1].payload(),
            PlanPayload::Expression(_)
        ));
        assert!(matches!(
            plan.operations[2].payload(),
            PlanPayload::Select(_)
        ));
        assert!(matches!(
            plan.operations[3].payload(),
            PlanPayload::Limit { n: 0 }
        ));
        assert!(matches!(
            plan.operations[4].payload(),
            PlanPayload::Offset { n: 0 }
        ));
        assert!(matches!(
            plan.operations[5].payload(),
            PlanPayload::OrderBy { .. }
        ));
        assert!(matches!(plan.operations[6].payload(), PlanPayload::Empty));
        assert!(matches!(
            plan.operations[7].payload(),
            PlanPayload::Drop { .. }
        ));
        assert!(matches!(
            plan.operations[8].payload(),
            PlanPayload::Rename { .. }
        ));
        assert!(matches!(
            plan.operations[9].payload(),
            PlanPayload::WithColumn { .. }
        ));
        assert!(matches!(
            plan.operations[10].payload(),
            PlanPayload::GroupBy { .. }
        ));
        assert!(matches!(
            plan.operations[11].payload(),
            PlanPayload::Join(_)
        ));
        assert!(matches!(
            plan.operations[12].payload(),
            PlanPayload::Union(_)
        ));
        assert!(matches!(
            plan.operations[13].payload(),
            PlanPayload::UnionByName(_)
        ));
        assert!(matches!(
            plan.operations[14].payload(),
            PlanPayload::CrossJoin(_)
        ));
        assert!(matches!(
            plan.operations[15].payload(),
            PlanPayload::Rollup(_)
        ));
        assert!(matches!(
            plan.operations[16].payload(),
            PlanPayload::Cube(_)
        ));
    }

    #[test]
    fn rejects_a_malformed_late_operation_before_returning_any_plan() {
        let error = LogicalPlan::decode(&[
            json!({"op": "limit", "payload": {"n": 3}}),
            json!({"op": "filter", "payload": {"op": "gt", "left": {"col": "id"}}}),
        ])
        .expect_err("the second operation is malformed");

        assert_eq!(error.code(), "INVALID_PLAN");
        assert!(error.user_message().contains("requires 'left' and 'right'"));
    }

    #[test]
    fn keeps_unknown_operations_distinct_from_malformed_supported_operations() {
        let error = LogicalPlan::decode(&[json!({"op": "collect"})])
            .expect_err("unknown operations are rejected");

        assert_eq!(error.code(), "UNSUPPORTED_PLAN_OPERATION");
    }
}

fn decode_operation(index: usize, value: &Value) -> Result<PlanOperation, EngineError> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid(format!("plan step {index} must be a JSON object")))?;
    let name = object
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("plan step {index} must have an 'op' string")))?;
    let kind = OperationKind::parse(name).ok_or_else(|| {
        EngineError::from_detail(
            ErrorDetail::new(
                ErrorCategory::InvalidInput,
                "UNSUPPORTED_PLAN_OPERATION",
                format!("unsupported plan operation '{name}' at step {index}"),
            )
            .with_retryability(ErrorRetryability::Terminal),
        )
    })?;
    let payload = object.get("payload").cloned().unwrap_or(Value::Null);
    let payload = merge_join_fields(kind, payload, object);
    let path = format!("plan step {index} ({})", kind.as_str());
    let payload = decode_payload(kind, &payload, &path)?;
    Ok(PlanOperation { kind, payload })
}

fn merge_join_fields(kind: OperationKind, payload: Value, step: &Map<String, Value>) -> Value {
    if !matches!(
        kind,
        OperationKind::Join
            | OperationKind::Union
            | OperationKind::UnionByName
            | OperationKind::CrossJoin
    ) {
        return payload;
    }
    let mut payload = match payload {
        Value::Object(payload) => payload,
        Value::Null => Map::new(),
        other => return other,
    };
    for (snake, camel) in [
        ("other_data", "otherData"),
        ("other_schema", "otherSchema"),
        ("on", "on"),
    ] {
        if !payload.contains_key(snake)
            && !payload.contains_key(camel)
            && let Some(value) = step.get(snake).or_else(|| step.get(camel))
        {
            payload.insert(snake.to_string(), value.clone());
        }
    }
    Value::Object(payload)
}

fn decode_payload(
    kind: OperationKind,
    value: &Value,
    path: &str,
) -> Result<PlanPayload, EngineError> {
    match kind {
        OperationKind::Stop | OperationKind::Distinct => Ok(PlanPayload::Empty),
        OperationKind::Filter => Ok(PlanPayload::Expression(decode_expression(value, path)?)),
        OperationKind::Select => {
            let columns = value
                .as_array()
                .or_else(|| value.get("columns").and_then(Value::as_array))
                .ok_or_else(|| {
                    invalid(format!(
                        "{path}: select payload must contain a columns array"
                    ))
                })?;
            if columns.is_empty() {
                return Err(invalid(format!("{path}: select columns must not be empty")));
            }
            for item in columns {
                validate_select_item(item, path)?;
            }
            Ok(PlanPayload::Select(columns.clone()))
        }
        OperationKind::Limit => {
            let n = value
                .get("n")
                .and_then(Value::as_u64)
                .ok_or_else(|| invalid(format!("{path}: 'n' must be a nonnegative integer")))?;
            usize::try_from(n)
                .map_err(|_| invalid(format!("{path}: 'n' exceeds the platform limit")))?;
            Ok(PlanPayload::Limit { n })
        }
        OperationKind::Offset => {
            let n = match value.get("n") {
                None | Some(Value::Null) => 0,
                Some(value) => value
                    .as_u64()
                    .ok_or_else(|| invalid(format!("{path}: 'n' must be a nonnegative integer")))?,
            };
            usize::try_from(n)
                .map_err(|_| invalid(format!("{path}: 'n' exceeds the platform limit")))?;
            Ok(PlanPayload::Offset { n })
        }
        OperationKind::OrderBy => {
            let object = payload_object(value, path)?;
            let columns = object
                .get("columns")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid(format!("{path}: 'columns' must be an array")))?;
            if columns.is_empty() || !columns.iter().any(valid_order_by_item) {
                return Err(invalid(format!(
                    "{path}: 'columns' must contain a sortable column"
                )));
            }
            if columns.iter().any(|item| !valid_order_by_item(item)) {
                return Err(invalid(format!("{path}: invalid orderBy column")));
            }
            let nulls_last = object
                .get("nulls_last")
                .map(|value| {
                    value
                        .as_array()
                        .filter(|items| items.iter().all(Value::is_boolean))
                        .cloned()
                        .ok_or_else(|| {
                            invalid(format!("{path}: 'nulls_last' must be a boolean array"))
                        })
                })
                .transpose()?;
            Ok(PlanPayload::OrderBy {
                columns: columns.clone(),
                nulls_last,
            })
        }
        OperationKind::Drop => {
            let object = payload_object(value, path)?;
            let columns = object
                .get("columns")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid(format!("{path}: 'columns' must be an array")))?;
            if columns.iter().any(|column| !valid_drop_item(column)) {
                return Err(invalid(format!(
                    "{path}: drop columns must be names or column objects"
                )));
            }
            Ok(PlanPayload::Drop {
                columns: columns.clone(),
            })
        }
        OperationKind::WithColumnRenamed => {
            let object = payload_object(value, path)?;
            let old = required_string(object, "old", path)?;
            let new = required_string(object, "new", path)?;
            Ok(PlanPayload::Rename { old, new })
        }
        OperationKind::WithColumn => {
            let object = payload_object(value, path)?;
            let name = object
                .get("name")
                .or_else(|| object.get("alias"))
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| invalid(format!("{path}: 'name' or 'alias' must be a string")))?
                .to_string();
            let expression = object
                .get("expr")
                .ok_or_else(|| invalid(format!("{path}: 'expr' is required")))?;
            Ok(PlanPayload::WithColumn {
                name,
                expression: decode_expression(expression, path)?,
            })
        }
        OperationKind::GroupBy => {
            let object = payload_object(value, path)?;
            let keys = object
                .get("group_by")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid(format!("{path}: 'group_by' must be an array")))?;
            let aggregations = object
                .get("aggs")
                .or_else(|| object.get("aggregations"))
                .and_then(Value::as_array)
                .ok_or_else(|| invalid(format!("{path}: 'aggs' must be an array")))?;
            validate_group_keys(keys, path)?;
            validate_aggregations(aggregations, path)?;
            Ok(PlanPayload::GroupBy {
                keys: keys.clone(),
                aggregations: aggregations.clone(),
            })
        }
        OperationKind::Join
        | OperationKind::Union
        | OperationKind::UnionByName
        | OperationKind::CrossJoin => {
            let input = decode_binary_input(value, kind, path)?;
            Ok(match kind {
                OperationKind::Join => PlanPayload::Join(input),
                OperationKind::Union => PlanPayload::Union(input),
                OperationKind::UnionByName => PlanPayload::UnionByName(input),
                OperationKind::CrossJoin => PlanPayload::CrossJoin(input),
                _ => unreachable!(),
            })
        }
        OperationKind::Rollup | OperationKind::Cube => {
            let object = payload_object(value, path)?;
            let keys = object
                .get("group_by")
                .or_else(|| object.get("columns"))
                .and_then(Value::as_array)
                .ok_or_else(|| invalid(format!("{path}: 'group_by' must be an array")))?;
            let aggregations = object
                .get("aggs")
                .or_else(|| object.get("aggregations"))
                .and_then(Value::as_array)
                .ok_or_else(|| invalid(format!("{path}: 'aggs' must be an array")))?;
            validate_group_keys(keys, path)?;
            validate_aggregations(aggregations, path)?;
            let input = GroupInput {
                keys: keys.clone(),
                aggregations: aggregations.clone(),
            };
            Ok(if kind == OperationKind::Rollup {
                PlanPayload::Rollup(input)
            } else {
                PlanPayload::Cube(input)
            })
        }
    }
}

fn payload_object<'a>(value: &'a Value, path: &str) -> Result<&'a Map<String, Value>, EngineError> {
    value
        .as_object()
        .ok_or_else(|| invalid(format!("{path}: payload must be an object")))
}

fn required_string(
    object: &Map<String, Value>,
    name: &str,
    path: &str,
) -> Result<String, EngineError> {
    object
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("{path}: '{name}' must be a nonempty string")))
}

fn valid_select_column_item(value: &Value) -> bool {
    value.as_str().is_some_and(|name| !name.trim().is_empty())
        || value.as_object().is_some_and(|object| {
            let column = object
                .get("col")
                .or_else(|| object.get("column"))
                .and_then(Value::as_str);
            column
                .or_else(|| object.get("name").and_then(Value::as_str))
                .is_some_and(|name| !name.trim().is_empty())
        })
}

fn valid_group_key(value: &Value) -> bool {
    value.as_str().is_some_and(|name| !name.trim().is_empty())
        || value.as_object().is_some_and(|object| {
            object
                .get("col")
                .and_then(Value::as_str)
                .or_else(|| object.get("name").and_then(Value::as_str))
                .is_some_and(|name| !name.trim().is_empty())
        })
}

fn valid_drop_item(value: &Value) -> bool {
    value.as_str().is_some_and(|name| !name.trim().is_empty())
        || value.as_object().is_some_and(|object| {
            object
                .get("col")
                .or_else(|| object.get("column"))
                .and_then(Value::as_str)
                .is_some_and(|name| !name.trim().is_empty())
        })
}

fn valid_order_by_item(value: &Value) -> bool {
    if let Some(name) = value.as_str() {
        let name = name.trim();
        if name.is_empty() || name.eq_ignore_ascii_case("asc") || name.eq_ignore_ascii_case("desc")
        {
            return false;
        }
        if name.starts_with('[') && name.ends_with(']') {
            return name[1..name.len() - 1]
                .split(',')
                .any(|part| !part.trim().trim_matches(['\'', '"']).is_empty());
        }
        let uppercase = name.to_ascii_uppercase();
        if let Some(prefix) = uppercase.strip_suffix(" DESC") {
            return !prefix.trim().is_empty();
        }
        if let Some(prefix) = uppercase.strip_suffix(" ASC") {
            return !prefix.trim().is_empty();
        }
        return true;
    }
    value.as_object().is_some_and(|object| {
        object
            .get("col")
            .or_else(|| object.get("name"))
            .and_then(Value::as_str)
            .is_some_and(|name| !name.trim().is_empty())
    })
}

fn validate_select_item(value: &Value, path: &str) -> Result<(), EngineError> {
    if let Some(name) = value.as_str() {
        if name.trim().is_empty() {
            return Err(invalid(format!("{path}: select column must not be empty")));
        }
        return Ok(());
    }
    let object = value
        .as_object()
        .ok_or_else(|| invalid(format!("{path}: select item must be a string or object")))?;
    if let Some(expression) = object.get("expr") {
        if object
            .get("name")
            .is_some_and(|name| !name.as_str().is_some_and(|name| !name.is_empty()))
        {
            return Err(invalid(format!(
                "{path}: select expression 'name' must be a nonempty string"
            )));
        }
        decode_expression(expression, path)?;
        return Ok(());
    }
    if object.contains_key("fn") || object.contains_key("op") || object.contains_key("udf") {
        decode_expression(value, path)?;
        return Ok(());
    }
    if valid_select_column_item(value) {
        return Ok(());
    }
    Err(invalid(format!(
        "{path}: select item is missing a column or expression"
    )))
}

fn validate_group_keys(keys: &[Value], path: &str) -> Result<(), EngineError> {
    for key in keys {
        if valid_group_key(key) {
            continue;
        }
        if let Some(expression) = key.get("expr") {
            decode_expression(expression, path)?;
            continue;
        }
        return Err(invalid(format!(
            "{path}: group keys must be columns or expressions"
        )));
    }
    Ok(())
}

fn validate_aggregations(aggregations: &[Value], path: &str) -> Result<(), EngineError> {
    for aggregation in aggregations {
        let object = aggregation
            .as_object()
            .ok_or_else(|| invalid(format!("{path}: every aggregation must be an object")))?;
        if let Some(expression) = object.get("expr") {
            decode_expression(expression, path)?;
            continue;
        }
        let name = object
            .get("agg")
            .or_else(|| object.get("func"))
            .and_then(Value::as_str)
            .ok_or_else(|| invalid(format!("{path}: aggregation must have an 'agg' string")))?;
        if !matches!(
            name,
            "sum" | "count" | "avg" | "min" | "max" | "first" | "last"
        ) {
            return Err(invalid(format!("{path}: unsupported aggregation '{name}'")));
        }
        if name != "count" && object.get("column").and_then(Value::as_str).is_none() {
            return Err(invalid(format!(
                "{path}: aggregation '{name}' requires 'column'"
            )));
        }
        if object
            .get("column")
            .is_some_and(|column| !column.is_string())
        {
            return Err(invalid(format!(
                "{path}: aggregation 'column' must be a string"
            )));
        }
        if object
            .get("alias")
            .or_else(|| object.get("name"))
            .is_some_and(|alias| !alias.as_str().is_some_and(|alias| !alias.is_empty()))
        {
            return Err(invalid(format!(
                "{path}: aggregation alias must be a nonempty string"
            )));
        }
        if object
            .get("cast")
            .or_else(|| object.get("cast_type"))
            .is_some_and(|cast| !cast.is_string())
        {
            return Err(invalid(format!(
                "{path}: aggregation cast must be a string"
            )));
        }
        if object
            .get("ignorenulls")
            .is_some_and(|ignore_nulls| !ignore_nulls.is_boolean())
        {
            return Err(invalid(format!("{path}: 'ignorenulls' must be a boolean")));
        }
    }
    Ok(())
}

fn decode_binary_input(
    value: &Value,
    kind: OperationKind,
    path: &str,
) -> Result<BinaryInput, EngineError> {
    let object = payload_object(value, path)?;
    let data = object
        .get("other_data")
        .or_else(|| object.get("otherData"))
        .and_then(Value::as_array)
        .ok_or_else(|| invalid(format!("{path}: 'other_data' must be an array")))?;
    let schema = object
        .get("other_schema")
        .or_else(|| object.get("otherSchema"))
        .and_then(Value::as_array)
        .ok_or_else(|| invalid(format!("{path}: 'other_schema' must be an array")))?;
    let schema_names = validate_schema(schema, path)?;
    validate_rows(data, schema_names.len(), path)?;
    let (on, how) = if kind == OperationKind::Join {
        let on = object
            .get("on")
            .ok_or_else(|| invalid(format!("{path}: 'on' is required for join")))?;
        validate_join_condition(on, path)?;
        let how = object
            .get("how")
            .map(|how| {
                how.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| invalid(format!("{path}: 'how' must be a string")))
            })
            .transpose()?;
        if let Some(how) = &how
            && !matches!(
                how.as_str(),
                "inner"
                    | "left"
                    | "right"
                    | "outer"
                    | "full"
                    | "full_outer"
                    | "fullouter"
                    | "left_semi"
                    | "leftsemi"
                    | "semi"
                    | "left_anti"
                    | "leftanti"
                    | "anti"
            )
        {
            return Err(invalid(format!("{path}: unsupported join type '{how}'")));
        }
        (Some(on.clone()), how)
    } else {
        (None, None)
    };
    Ok(BinaryInput {
        data: data.clone(),
        schema: schema.clone(),
        on,
        how,
    })
}

fn validate_schema(schema: &[Value], path: &str) -> Result<Vec<String>, EngineError> {
    let mut names = Vec::with_capacity(schema.len());
    let mut seen_names = std::collections::HashSet::with_capacity(schema.len());
    for field in schema {
        let object = field
            .as_object()
            .ok_or_else(|| invalid(format!("{path}: each schema field must be an object")))?;
        let name = object
            .get("name")
            .or_else(|| object.get("fieldName"))
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| invalid(format!("{path}: schema field needs a nonempty name")))?;
        if !seen_names.insert(name) {
            return Err(invalid(format!("{path}: duplicate schema field '{name}'")));
        }
        let data_type = object
            .get("type")
            .and_then(Value::as_str)
            .or_else(|| object.get("dataType").and_then(Value::as_str))
            .or_else(|| {
                object
                    .get("dataType")?
                    .get("typeName")
                    .or_else(|| object.get("dataType")?.get("type"))
                    .and_then(Value::as_str)
            })
            .filter(|data_type| !data_type.is_empty())
            .ok_or_else(|| invalid(format!("{path}: schema field '{name}' needs a type")))?;
        let _ = data_type;
        names.push(name.to_string());
    }
    Ok(names)
}

fn validate_rows(rows: &[Value], column_count: usize, path: &str) -> Result<(), EngineError> {
    for row in rows {
        match row {
            Value::Array(values) if values.len() == column_count => {}
            Value::Array(values) => {
                return Err(invalid(format!(
                    "{path}: row has {} values but the schema has {column_count} columns",
                    values.len()
                )));
            }
            Value::Object(_) => {}
            _ => {
                return Err(invalid(format!(
                    "{path}: each input row must be an array or object"
                )));
            }
        }
    }
    Ok(())
}

fn validate_join_condition(on: &Value, path: &str) -> Result<(), EngineError> {
    if let Some(name) = on.as_str() {
        if name.trim().is_empty() {
            return Err(invalid(format!(
                "{path}: join key must be a nonempty column name"
            )));
        }
        if name.contains('(') {
            return Err(invalid(format!(
                "{path}: join on expression strings are not supported; use column names only"
            )));
        }
        return Ok(());
    }
    if let Some(items) = on.as_array() {
        if items.is_empty() {
            return Err(invalid(format!(
                "{path}: join 'on' array must not be empty"
            )));
        }
        for item in items {
            if item.as_str().is_some_and(|name| !name.trim().is_empty()) {
                continue;
            }
            if let Some(object) = item.as_object() {
                if object
                    .get("col")
                    .or_else(|| object.get("column"))
                    .and_then(Value::as_str)
                    .is_some_and(|name| !name.trim().is_empty())
                {
                    continue;
                }
                if object.contains_key("op")
                    || object.contains_key("fn")
                    || object.contains_key("udf")
                {
                    decode_expression(item, path)?;
                    continue;
                }
            }
            return Err(invalid(format!("{path}: invalid join key")));
        }
        return Ok(());
    }
    if on.as_object().is_some_and(|object| {
        object.contains_key("op") || object.contains_key("fn") || object.contains_key("udf")
    }) {
        return decode_expression(on, path).map(|_| ());
    }
    Err(invalid(format!(
        "{path}: join 'on' must be a key or expression"
    )))
}

fn decode_expression(value: &Value, path: &str) -> Result<PlanExpression, EngineError> {
    let kind = if let Some(name) = value.as_str() {
        ExpressionKind::Column(name.to_string())
    } else if value.is_null() || value.is_boolean() || value.is_number() {
        ExpressionKind::Literal
    } else {
        let object = value
            .as_object()
            .ok_or_else(|| invalid(format!("{path}: invalid expression value")))?;
        if let Some(name) = object
            .get("col")
            .or_else(|| object.get("column"))
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
        {
            ExpressionKind::Column(name.to_string())
        } else if let Some(literal) = object.get("lit") {
            if !(literal.is_null()
                || literal.is_boolean()
                || literal.is_number()
                || literal.is_string())
            {
                return Err(invalid(format!("{path}: literal must be a scalar value")));
            }
            ExpressionKind::Literal
        } else if let Some(name) = object.get("op").and_then(Value::as_str) {
            validate_operator_operands(name, object, path)?;
            let mut children = Vec::new();
            for key in [
                "left",
                "right",
                "arg",
                "lower",
                "upper",
                "condition",
                "then",
                "else",
                "pattern",
                "replacement",
            ] {
                if name == "isin" && key == "right" {
                    if let Some(right) = object.get(key) {
                        validate_isin_values(right, path)?;
                    }
                    continue;
                }
                if let Some(child) = object.get(key) {
                    children.push(decode_expression(child, path)?);
                }
            }
            if let Some(args) = object.get("args") {
                let args = args
                    .as_array()
                    .ok_or_else(|| invalid(format!("{path}: operator 'args' must be an array")))?;
                for argument in args {
                    children.push(decode_expression(argument, path)?);
                }
            }
            if let Some(values) = object.get("values") {
                let values = values.as_array().ok_or_else(|| {
                    invalid(format!("{path}: operator 'values' must be an array"))
                })?;
                for item in values {
                    children.push(decode_expression(item, path)?);
                }
            }
            if name == "isin"
                && let Some(values) = object.get("values")
                && values.as_array().is_none()
            {
                return Err(invalid(format!(
                    "{path}: operator 'isin' 'values' must be an array"
                )));
            }
            ExpressionKind::Operator {
                name: name.to_string(),
                arguments: children,
            }
        } else if let Some(name) = object.get("udf").and_then(Value::as_str) {
            if name.is_empty() {
                return Err(invalid(format!("{path}: UDF name must not be empty")));
            }
            let arguments = required_expression_args(object, path)?;
            ExpressionKind::Function {
                name: format!("udf:{name}"),
                arguments,
            }
        } else if let Some(name) = object
            .get("fn")
            .or_else(|| object.get("function"))
            .or_else(|| object.get("args").and(object.get("op")))
            .and_then(Value::as_str)
        {
            if name.is_empty() {
                return Err(invalid(format!("{path}: function name must not be empty")));
            }
            validate_window_spec(object.get("window"), path)?;
            let arguments = match object.get("args") {
                Some(Value::Array(arguments)) => arguments
                    .iter()
                    .map(|argument| decode_expression(argument, path))
                    .collect::<Result<Vec<_>, _>>()?,
                Some(_) => {
                    return Err(invalid(format!("{path}: function 'args' must be an array")));
                }
                None if object.contains_key("window") => Vec::new(),
                None => {
                    return Err(invalid(format!("{path}: function 'args' must be an array")));
                }
            };
            ExpressionKind::Function {
                name: name.to_string(),
                arguments,
            }
        } else if object.get("type").and_then(Value::as_str) == Some("window") {
            return Err(invalid(format!(
                "{path}: window expression requires a function name and window object"
            )));
        } else {
            return Err(invalid(format!(
                "{path}: expression object must contain a column, literal, function, or operator"
            )));
        }
    };
    if value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "window")
        && value.get("window").is_none()
    {
        return Err(invalid(format!(
            "{path}: window expression requires a window object"
        )));
    }
    Ok(PlanExpression {
        kind,
        source: value.clone(),
    })
}

fn validate_window_spec(window: Option<&Value>, path: &str) -> Result<(), EngineError> {
    let Some(window) = window else {
        return Ok(());
    };
    let object = window
        .as_object()
        .ok_or_else(|| invalid(format!("{path}: window spec must be an object")))?;
    for key in ["order_by", "partition_by"] {
        let Some(values) = object.get(key) else {
            continue;
        };
        let values = values
            .as_array()
            .ok_or_else(|| invalid(format!("{path}: window '{key}' must be an array")))?;
        for value in values {
            let valid = value.as_str().is_some_and(|name| !name.is_empty())
                || value.as_object().is_some_and(|item| {
                    item.get("col")
                        .and_then(Value::as_str)
                        .is_some_and(|name| !name.is_empty())
                });
            if !valid {
                return Err(invalid(format!(
                    "{path}: window '{key}' entries must be column names"
                )));
            }
        }
    }
    Ok(())
}

fn validate_isin_values(value: &Value, path: &str) -> Result<(), EngineError> {
    let values = if let Some(values) = value.as_array() {
        values
    } else if let Some(values) = value.get("lit").and_then(Value::as_array) {
        values
    } else if value.get("lit").is_some() {
        return Ok(());
    } else {
        return Err(invalid(format!(
            "{path}: 'isin' right must be an array or a literal array"
        )));
    };
    for item in values {
        if item.is_array() {
            return Err(invalid(format!(
                "{path}: 'isin' values must be scalar literals"
            )));
        }
        if let Some(object) = item.as_object()
            && !object.contains_key("lit")
        {
            return Err(invalid(format!(
                "{path}: 'isin' values must be scalar literals"
            )));
        }
    }
    Ok(())
}

fn required_expression_args(
    object: &Map<String, Value>,
    path: &str,
) -> Result<Vec<PlanExpression>, EngineError> {
    let args = object
        .get("args")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid(format!("{path}: UDF 'args' must be an array")))?;
    args.iter()
        .map(|argument| decode_expression(argument, path))
        .collect()
}

fn validate_operator_operands(
    name: &str,
    object: &Map<String, Value>,
    path: &str,
) -> Result<(), EngineError> {
    if !matches!(
        name,
        "eq" | "ne"
            | "gt"
            | "ge"
            | "lt"
            | "le"
            | "eq_null_safe"
            | "and"
            | "or"
            | "add"
            | "+"
            | "sub"
            | "minus"
            | "-"
            | "mul"
            | "*"
            | "div"
            | "/"
            | "divide"
            | "pow"
            | "**"
            | "getItem"
            | "startswith"
            | "not"
            | "bitwise_not"
            | "is_null"
            | "is_not_null"
            | "between"
            | "isin"
            | "regexp_extract"
            | "regexp_replace"
            | "create_map"
            | "createMap"
            | "udf"
            | "coalesce"
            | "nvl"
            | "translate"
            | "substring_index"
            | "substringIndex"
            | "levenshtein"
            | "soundex"
            | "crc32"
            | "xxhash64"
            | "get_json_object"
            | "getJsonObject"
            | "json_tuple"
            | "jsonTuple"
            | "regexp_extract_all"
            | "regexpExtractAll"
            | "date_trunc"
            | "dateTrunc"
            | "to_date"
            | "toDate"
            | "format_string"
            | "formatString"
            | "log"
            | "explode"
            | "explode_outer"
            | "explodeOuter"
            | "concat"
            | "contains"
    ) {
        return Err(invalid(format!(
            "{path}: unsupported expression operator '{name}'"
        )));
    }

    let binary = matches!(
        name,
        "eq" | "ne"
            | "gt"
            | "ge"
            | "lt"
            | "le"
            | "eq_null_safe"
            | "and"
            | "or"
            | "add"
            | "+"
            | "sub"
            | "minus"
            | "-"
            | "mul"
            | "*"
            | "div"
            | "/"
            | "divide"
            | "pow"
            | "**"
            | "getItem"
            | "startswith"
    );
    if binary && (!object.contains_key("left") || !object.contains_key("right")) {
        return Err(invalid(format!(
            "{path}: operator '{name}' requires 'left' and 'right'"
        )));
    }
    if matches!(name, "not" | "bitwise_not" | "is_null" | "is_not_null")
        && !object.contains_key("arg")
    {
        return Err(invalid(format!("{path}: operator '{name}' requires 'arg'")));
    }
    if name == "between"
        && ["left", "lower", "upper"]
            .iter()
            .any(|key| !object.contains_key(*key))
    {
        return Err(invalid(format!(
            "{path}: operator 'between' requires left/lower/upper"
        )));
    }
    if name == "isin"
        && (!object.contains_key("left")
            || (!object.contains_key("right") && !object.contains_key("values")))
    {
        return Err(invalid(format!(
            "{path}: operator 'isin' requires 'left' and 'right' or 'values'"
        )));
    }
    if matches!(name, "regexp_extract" | "regexp_replace") {
        if let Some(args) = object.get("args") {
            if args.as_array().is_none_or(|args| args.len() < 3) {
                return Err(invalid(format!(
                    "{path}: operator '{name}' requires at least three arguments"
                )));
            }
        } else if !object.contains_key("left")
            || !(object.contains_key("pattern") || object.contains_key("right"))
            || (name == "regexp_replace" && !object.contains_key("replacement"))
        {
            return Err(invalid(format!(
                "{path}: operator '{name}' is missing its column or pattern arguments"
            )));
        }
    }
    if matches!(name, "create_map" | "createMap")
        && object
            .get("args")
            .and_then(Value::as_array)
            .is_none_or(|args| args.is_empty() || args.len() % 2 != 0)
    {
        return Err(invalid(format!(
            "{path}: operator '{name}' requires an even, nonempty 'args' array"
        )));
    }
    if matches!(name, "udf") {
        if object
            .get("udf")
            .or_else(|| object.get("name"))
            .and_then(Value::as_str)
            .is_none()
        {
            return Err(invalid(format!(
                "{path}: operator 'udf' requires a UDF name"
            )));
        }
        if object.get("args").and_then(Value::as_array).is_none() {
            return Err(invalid(format!(
                "{path}: operator 'udf' requires an 'args' array"
            )));
        }
    }
    if object.get("args").is_none()
        && ![
            "left",
            "right",
            "arg",
            "lower",
            "upper",
            "condition",
            "then",
            "else",
        ]
        .iter()
        .any(|key| object.contains_key(*key))
        && name != "udf"
    {
        return Err(invalid(format!(
            "{path}: operator '{name}' has no operands"
        )));
    }
    Ok(())
}
