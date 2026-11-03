//! Input schema validation, JSON conversion, and bounded Connect Arrow decoding.

use super::*;

pub(super) fn connect_local_column_name(index: usize) -> String {
    format!("__sparkless_connect_local_{index}")
}

pub(super) fn connect_physical_schema(schema: &StructType) -> Result<StructType, EngineError> {
    schema.validate_metadata().map_err(|error| {
        EngineError::invalid_input(format!("invalid Connect local schema metadata: {error}"))
    })?;
    if schema.logical_fields().is_empty()
        || schema.logical_fields().len() > MAX_CONNECT_ARROW_SCHEMA_FIELDS
    {
        return Err(EngineError::invalid_input(format!(
            "Connect local schema must contain 1-{MAX_CONNECT_ARROW_SCHEMA_FIELDS} scalar fields"
        )));
    }
    let fields = schema
        .logical_fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let supported = matches!(
                &field.data_type,
                CoreDataType::Boolean
                    | CoreDataType::Short
                    | CoreDataType::Integer
                    | CoreDataType::Long
                    | CoreDataType::Double
                    | CoreDataType::String
                    | CoreDataType::Date
                    | CoreDataType::Timestamp
            ) || matches!(&field.data_type, CoreDataType::Decimal(p, s) if (1..=38).contains(p) && s <= p);
            if !supported {
                return Err(EngineError::invalid_input(format!(
                    "Connect local type {:?} is not enabled in the scalar Arrow subset",
                    field.data_type
                )));
            }
            let mut physical = field.clone();
            physical.name = connect_local_column_name(index);
            Ok(physical)
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    Ok(StructType::from_logical_fields(fields))
}

pub(super) fn infer_connect_schema(
    arrow_schema: &polars_arrow::datatypes::ArrowSchema,
    public_names: &[String],
) -> Result<StructType, EngineError> {
    if arrow_schema.len() != public_names.len() {
        return Err(EngineError::invalid_input(
            "Connect Arrow schema field-name count does not match its fields".into(),
        ));
    }
    let fields = arrow_schema
        .iter()
        .enumerate()
        .map(|(index, (_name, field))| {
            let data_type = match &field.dtype {
                ArrowDataType::Boolean => CoreDataType::Boolean,
                ArrowDataType::Int16 => CoreDataType::Short,
                ArrowDataType::Int32 => CoreDataType::Integer,
                ArrowDataType::Int64 => CoreDataType::Long,
                ArrowDataType::Float64 => CoreDataType::Double,
                ArrowDataType::Utf8 => CoreDataType::String,
                ArrowDataType::Date32 => CoreDataType::Date,
                ArrowDataType::Timestamp(
                    polars_arrow::datatypes::TimeUnit::Microsecond,
                    Some(timezone),
                ) if timezone.as_str() == "UTC" => CoreDataType::Timestamp,
                ArrowDataType::Decimal(precision, scale)
                    if (1..=38).contains(precision) && scale <= precision =>
                {
                    CoreDataType::Decimal(*precision as u8, *scale as u8)
                }
                other => {
                    return Err(EngineError::invalid_input(format!(
                        "Connect Arrow type {other:?} is not enabled in the scalar subset"
                    )));
                }
            };
            let metadata = field
                .metadata
                .as_ref()
                .map(|metadata| {
                    metadata
                        .iter()
                        .map(|(key, value)| {
                            (
                                key.to_string(),
                                serde_json::from_str(value.as_str())
                                    .unwrap_or_else(|_| JsonValue::String(value.to_string())),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            Ok(
                StructField::new(public_names[index].clone(), data_type, field.is_nullable)
                    .with_metadata(metadata),
            )
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    Ok(StructType::from_logical_fields(fields))
}

pub(super) fn validate_connect_arrow_source_schema(
    schema: &polars_arrow::datatypes::ArrowSchema,
) -> Result<(), EngineError> {
    if schema.is_empty() || schema.len() > MAX_CONNECT_ARROW_SCHEMA_FIELDS {
        return Err(EngineError::invalid_input(format!(
            "Connect Arrow schema must contain 1-{MAX_CONNECT_ARROW_SCHEMA_FIELDS} scalar fields"
        )));
    }
    for (_, field) in schema.iter() {
        let supported = matches!(
            &field.dtype,
            ArrowDataType::Boolean
                | ArrowDataType::Int16
                | ArrowDataType::Int32
                | ArrowDataType::Int64
                | ArrowDataType::Float64
                | ArrowDataType::Utf8
                | ArrowDataType::Date32
        ) || matches!(
            &field.dtype,
            ArrowDataType::Timestamp(
                polars_arrow::datatypes::TimeUnit::Microsecond,
                Some(timezone)
            ) if timezone.as_str() == "UTC"
        ) || matches!(
            &field.dtype,
            ArrowDataType::Decimal(precision, scale)
                if (1..=38).contains(precision) && scale <= precision
        );
        if !supported {
            return Err(EngineError::invalid_input(format!(
                "Connect Arrow type {:?} is not enabled in the scalar subset",
                field.dtype
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_connect_scalar_values(
    series: &Series,
    max_scalar_bytes: usize,
    name: &str,
) -> Result<(), EngineError> {
    match series.dtype() {
        DataType::String => {
            for value in series
                .str()
                .map_err(polars_to_core_error)?
                .into_iter()
                .flatten()
            {
                if value.len() > max_scalar_bytes {
                    return Err(EngineError::invalid_input(format!(
                        "Connect local value in column {name:?} exceeds {max_scalar_bytes} bytes"
                    )));
                }
            }
        }
        DataType::Float64 => {
            for value in series
                .f64()
                .map_err(polars_to_core_error)?
                .into_iter()
                .flatten()
            {
                if !value.is_finite() {
                    return Err(EngineError::invalid_input(format!(
                        "Connect local column {name:?} contains a non-finite DOUBLE value"
                    )));
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn read_connect_ipc_message_metadata(
    reader: &mut Cursor<&[u8]>,
    data_len: usize,
) -> Result<Option<Vec<u8>>, EngineError> {
    let mut prefix = [0u8; 4];
    reader.read_exact(&mut prefix).map_err(|error| {
        EngineError::invalid_input(format!(
            "malformed Connect Arrow IPC stream framing: {error}"
        ))
    })?;
    if prefix == [0xff; 4] {
        reader.read_exact(&mut prefix).map_err(|error| {
            EngineError::invalid_input(format!("malformed Connect Arrow IPC continuation: {error}"))
        })?;
    }
    let metadata_len = i32::from_le_bytes(prefix);
    if metadata_len < 0 {
        return Err(EngineError::invalid_input(
            "malformed Connect Arrow IPC metadata length".into(),
        ));
    }
    let metadata_len = metadata_len as usize;
    if metadata_len == 0 {
        return Ok(None);
    }
    if metadata_len > data_len.saturating_sub(reader.position() as usize) {
        return Err(EngineError::invalid_input(
            "malformed Connect Arrow IPC metadata extends past the payload".into(),
        ));
    }
    let mut metadata = vec![0; metadata_len];
    reader.read_exact(&mut metadata).map_err(|error| {
        EngineError::invalid_input(format!("malformed Connect Arrow IPC metadata: {error}"))
    })?;
    Ok(Some(metadata))
}

struct ConnectArrowSchemaBudget {
    total_fields: usize,
    schema_bytes: usize,
    metadata_bytes: usize,
    max_schema_bytes: usize,
}

impl ConnectArrowSchemaBudget {
    fn add_schema_bytes(&mut self, bytes: usize) -> Result<(), EngineError> {
        self.schema_bytes = self.schema_bytes.saturating_add(bytes);
        if self.schema_bytes > self.max_schema_bytes {
            return Err(EngineError::invalid_input(format!(
                "Connect Arrow expanded schema exceeds {} bytes",
                self.max_schema_bytes
            )));
        }
        Ok(())
    }

    fn inspect_metadata<'a>(
        &mut self,
        metadata: impl IntoIterator<
            Item = Result<
                polars_arrow::io::ipc::format::ipc::KeyValueRef<'a>,
                polars_arrow::io::ipc::format::ipc::planus::Error,
            >,
        >,
    ) -> Result<(), EngineError> {
        use polars_arrow::io::ipc::format::ipc::KeyValue;
        use robin_sparkless_core::schema::MAX_LOGICAL_SCHEMA_METADATA_BYTES;

        for entry in metadata {
            let entry = entry.map_err(|error| {
                EngineError::invalid_input(format!("invalid Connect Arrow metadata entry: {error}"))
            })?;
            let key = entry.key().map_err(|error| {
                EngineError::invalid_input(format!("invalid Connect Arrow metadata key: {error}"))
            })?;
            let value = entry.value().map_err(|error| {
                EngineError::invalid_input(format!("invalid Connect Arrow metadata value: {error}"))
            })?;
            // Count each reference, including shared/duplicate entries, before owning the
            // FlatBuffer. Count entry storage too, so empty aliased entries are bounded.
            self.metadata_bytes = self
                .metadata_bytes
                .saturating_add(std::mem::size_of::<KeyValue>())
                .saturating_add(key.map(str::len).unwrap_or(0))
                .saturating_add(value.map(str::len).unwrap_or(0));
            if self.metadata_bytes > MAX_LOGICAL_SCHEMA_METADATA_BYTES {
                return Err(EngineError::invalid_input(format!(
                    "Connect Arrow metadata exceeds {MAX_LOGICAL_SCHEMA_METADATA_BYTES} bytes"
                )));
            }
        }
        Ok(())
    }
}

fn inspect_connect_arrow_field_tree(
    field: polars_arrow::io::ipc::format::ipc::FieldRef<'_>,
    depth: usize,
    budget: &mut ConnectArrowSchemaBudget,
) -> Result<bool, EngineError> {
    use polars_arrow::io::ipc::format::ipc::TypeRef;

    if depth > MAX_CONNECT_ARROW_SCHEMA_DEPTH {
        return Err(EngineError::invalid_input(format!(
            "Connect Arrow schema nesting exceeds {MAX_CONNECT_ARROW_SCHEMA_DEPTH}"
        )));
    }
    budget.total_fields = budget.total_fields.saturating_add(1);
    if budget.total_fields > MAX_CONNECT_ARROW_SCHEMA_FIELDS {
        return Err(EngineError::invalid_input(format!(
            "Connect Arrow schema exceeds {MAX_CONNECT_ARROW_SCHEMA_FIELDS} fields"
        )));
    }
    let name = field.name().map_err(|error| {
        EngineError::invalid_input(format!("invalid Connect Arrow field name: {error}"))
    })?;
    budget.add_schema_bytes(name.map(str::len).unwrap_or(0))?;
    if let Some(metadata) = field.custom_metadata().map_err(|error| {
        EngineError::invalid_input(format!("invalid Connect Arrow field metadata: {error}"))
    })? {
        budget.inspect_metadata(metadata)?;
    }
    if field
        .dictionary()
        .map_err(|error| {
            EngineError::invalid_input(format!("invalid Arrow dictionary metadata: {error}"))
        })?
        .is_some()
    {
        return Err(EngineError::invalid_input(
            "dictionary-encoded Connect Arrow input is unsupported".into(),
        ));
    }
    let children = field.children().map_err(|error| {
        EngineError::invalid_input(format!("invalid Arrow nested schema metadata: {error}"))
    })?;
    let mut has_children = false;
    if let Some(children) = children {
        for child in children.iter() {
            has_children = true;
            let child = child.map_err(|error| {
                EngineError::invalid_input(format!("invalid Arrow nested schema field: {error}"))
            })?;
            inspect_connect_arrow_field_tree(child, depth + 1, budget)?;
        }
    }
    match field.type_().map_err(|error| {
        EngineError::invalid_input(format!("invalid Connect Arrow field type: {error}"))
    })? {
        Some(TypeRef::Timestamp(timestamp)) => {
            let timezone = timestamp.timezone().map_err(|error| {
                EngineError::invalid_input(format!("invalid Connect Arrow timezone: {error}"))
            })?;
            budget.add_schema_bytes(timezone.map(str::len).unwrap_or(0))?;
        }
        Some(
            TypeRef::Bool(_)
            | TypeRef::Int(_)
            | TypeRef::FloatingPoint(_)
            | TypeRef::Utf8(_)
            | TypeRef::Date(_)
            | TypeRef::Decimal(_),
        ) => {}
        _ => {
            return Err(EngineError::invalid_input(
                "Connect Arrow field type is not enabled in the scalar subset".into(),
            ));
        }
    }
    Ok(has_children)
}

pub(super) fn preflight_connect_arrow_stream(
    data: &[u8],
    max_rows: usize,
    max_decoded_bytes: usize,
) -> Result<u64, EngineError> {
    if data.is_empty() {
        return Err(EngineError::invalid_input(
            "Connect Arrow IPC payload is empty".into(),
        ));
    }
    let mut reader = Cursor::new(data);
    let first = read_connect_ipc_message_metadata(&mut reader, data.len())?.ok_or_else(|| {
        EngineError::invalid_input("Connect Arrow IPC stream does not begin with a schema".into())
    })?;
    let message = MessageRef::read_as_root(first.as_slice()).map_err(|error| {
        EngineError::invalid_input(format!(
            "malformed Connect Arrow IPC schema message: {error}"
        ))
    })?;
    let header = message
        .header()
        .map_err(|error| {
            EngineError::invalid_input(format!(
                "malformed Connect Arrow IPC schema header: {error}"
            ))
        })?
        .ok_or_else(|| {
            EngineError::invalid_input("Connect Arrow IPC schema header is missing".into())
        })?;
    let schema = match header {
        MessageHeaderRef::Schema(schema) => schema,
        _ => {
            return Err(EngineError::invalid_input(
                "Connect Arrow IPC stream does not begin with a schema".into(),
            ));
        }
    };
    let fields = schema
        .fields()
        .map_err(|error| {
            EngineError::invalid_input(format!("malformed Connect Arrow schema fields: {error}"))
        })?
        .ok_or_else(|| {
            EngineError::invalid_input("Connect Arrow schema fields are missing".into())
        })?;
    let mut budget = ConnectArrowSchemaBudget {
        total_fields: 0,
        schema_bytes: 0,
        metadata_bytes: 0,
        max_schema_bytes: max_decoded_bytes,
    };
    if let Some(metadata) = message.custom_metadata().map_err(|error| {
        EngineError::invalid_input(format!("invalid Connect Arrow message metadata: {error}"))
    })? {
        budget.inspect_metadata(metadata)?;
    }
    if let Some(metadata) = schema.custom_metadata().map_err(|error| {
        EngineError::invalid_input(format!("invalid Connect Arrow schema metadata: {error}"))
    })? {
        budget.inspect_metadata(metadata)?;
    }
    for field in fields.iter() {
        let field = field.map_err(|error| {
            EngineError::invalid_input(format!("malformed Connect Arrow field: {error}"))
        })?;
        if inspect_connect_arrow_field_tree(field, 1, &mut budget)? {
            return Err(EngineError::invalid_input(
                "nested Connect Arrow types are not enabled".into(),
            ));
        }
    }
    let schema_body_len = message.body_length().map_err(|error| {
        EngineError::invalid_input(format!("invalid Connect Arrow schema body length: {error}"))
    })?;
    if schema_body_len != 0 {
        return Err(EngineError::invalid_input(
            "Connect Arrow schema message must not have a body".into(),
        ));
    }

    // The schema tree is now known to be shallow before Polars recursively converts it.
    let _stream_metadata = read_stream_metadata(&mut Cursor::new(data)).map_err(|error| {
        EngineError::invalid_input(format!("invalid Connect Arrow schema: {error}"))
    })?;
    let mut total_rows = 0u64;
    let mut total_buffer_bytes = 0usize;
    loop {
        let Some(metadata) = read_connect_ipc_message_metadata(&mut reader, data.len())? else {
            if reader.position() as usize != data.len() {
                return Err(EngineError::invalid_input(
                    "Connect Arrow IPC stream has trailing bytes".into(),
                ));
            }
            break;
        };
        let message = MessageRef::read_as_root(metadata.as_slice()).map_err(|error| {
            EngineError::invalid_input(format!("malformed Connect Arrow IPC message: {error}"))
        })?;
        let body_len = message.body_length().map_err(|error| {
            EngineError::invalid_input(format!("invalid Connect Arrow IPC body length: {error}"))
        })?;
        let body_len = usize::try_from(body_len).map_err(|_| {
            EngineError::invalid_input("invalid negative Connect Arrow IPC body length".into())
        })?;
        let header = message
            .header()
            .map_err(|error| {
                EngineError::invalid_input(format!(
                    "malformed Connect Arrow IPC message header: {error}"
                ))
            })?
            .ok_or_else(|| {
                EngineError::invalid_input("Connect Arrow IPC message header is missing".into())
            })?;
        match header {
            MessageHeaderRef::RecordBatch(batch) => {
                if batch
                    .compression()
                    .map_err(|error| {
                        EngineError::invalid_input(format!(
                            "invalid Connect Arrow compression metadata: {error}"
                        ))
                    })?
                    .is_some()
                {
                    return Err(EngineError::invalid_input(
                        "compressed Connect Arrow record batches are unsupported".into(),
                    ));
                }
                let rows = batch.length().map_err(|error| {
                    EngineError::invalid_input(format!("invalid Connect Arrow row count: {error}"))
                })?;
                if rows < 0 {
                    return Err(EngineError::invalid_input(
                        "invalid negative Connect Arrow row count".into(),
                    ));
                }
                total_rows = total_rows.saturating_add(rows as u64);
                if total_rows > max_rows as u64 {
                    return Err(EngineError::invalid_input(format!(
                        "Connect local relation exceeds {max_rows} rows"
                    )));
                }
                let buffers = batch
                    .buffers()
                    .map_err(|error| {
                        EngineError::invalid_input(format!(
                            "invalid Connect Arrow buffer table: {error}"
                        ))
                    })?
                    .ok_or_else(|| {
                        EngineError::invalid_input(
                            "Connect Arrow record batch has no buffer table".into(),
                        )
                    })?;
                for buffer in buffers.iter() {
                    let offset = buffer.offset();
                    let length = buffer.length();
                    let offset = usize::try_from(offset).map_err(|_| {
                        EngineError::invalid_input("negative Connect Arrow buffer offset".into())
                    })?;
                    let length = usize::try_from(length).map_err(|_| {
                        EngineError::invalid_input("negative Connect Arrow buffer length".into())
                    })?;
                    let end = offset.checked_add(length).ok_or_else(|| {
                        EngineError::invalid_input("Connect Arrow buffer range overflow".into())
                    })?;
                    if end > body_len {
                        return Err(EngineError::invalid_input(
                            "Connect Arrow buffer extends past its record batch body".into(),
                        ));
                    }
                    total_buffer_bytes =
                        total_buffer_bytes.checked_add(length).ok_or_else(|| {
                            EngineError::invalid_input(
                                "Connect Arrow decoded byte count overflow".into(),
                            )
                        })?;
                    if total_buffer_bytes > max_decoded_bytes {
                        return Err(EngineError::invalid_input(format!(
                            "Connect local Arrow buffers exceed {max_decoded_bytes} decoded bytes"
                        )));
                    }
                }
            }
            MessageHeaderRef::DictionaryBatch(_) => {
                return Err(EngineError::invalid_input(
                    "dictionary-encoded Connect Arrow input is unsupported".into(),
                ));
            }
            _ => {
                return Err(EngineError::invalid_input(
                    "unexpected message in Connect Arrow IPC stream".into(),
                ));
            }
        }
        let next = reader
            .position()
            .checked_add(body_len as u64)
            .ok_or_else(|| {
                EngineError::invalid_input("Connect Arrow IPC body length overflow".into())
            })?;
        if next > data.len() as u64 {
            return Err(EngineError::invalid_input(
                "Connect Arrow IPC body extends past the payload".into(),
            ));
        }
        reader.seek(SeekFrom::Start(next)).map_err(|error| {
            EngineError::invalid_input(format!("invalid Connect Arrow IPC body offset: {error}"))
        })?;
    }
    Ok(total_rows)
}

pub(super) fn rewrite_connect_arrow_input_schema(
    data: &[u8],
) -> Result<(Vec<u8>, Vec<String>), EngineError> {
    use polars_arrow::io::ipc::format::ipc::planus::{Builder, ReadAsRoot};
    use polars_arrow::io::ipc::format::ipc::{Message, MessageHeader, MessageRef};

    let continuation = data.starts_with(&[0xff; 4]);
    let mut reader = Cursor::new(data);
    let metadata =
        read_connect_ipc_message_metadata(&mut reader, data.len())?.ok_or_else(|| {
            EngineError::invalid_input(
                "Connect Arrow IPC stream does not begin with a schema".into(),
            )
        })?;
    let original_message_end = usize::try_from(reader.position()).map_err(|_| {
        EngineError::invalid_input("Connect Arrow schema offset is out of range".into())
    })?;
    let message_ref = MessageRef::read_as_root(&metadata).map_err(|error| {
        EngineError::invalid_input(format!(
            "malformed Connect Arrow IPC schema message: {error}"
        ))
    })?;
    let mut message = Message::try_from(message_ref).map_err(|error| {
        EngineError::invalid_input(format!("invalid Connect Arrow IPC schema message: {error}"))
    })?;
    let Some(MessageHeader::Schema(schema_message)) = message.header.as_mut() else {
        return Err(EngineError::invalid_input(
            "Connect Arrow IPC stream does not begin with a schema".into(),
        ));
    };
    let fields = schema_message.fields.as_mut().ok_or_else(|| {
        EngineError::invalid_input("Connect Arrow IPC schema has no fields".into())
    })?;
    let mut public_names = Vec::with_capacity(fields.len());
    for (index, field) in fields.iter_mut().enumerate() {
        public_names.push(field.name.clone().unwrap_or_default());
        field.name = Some(connect_local_column_name(index));
    }
    let serialized_message = {
        let mut builder = Builder::new();
        builder.finish(&message, None).to_vec()
    };
    let padded_message_len = serialized_message
        .len()
        .checked_add(7)
        .map(|length| length & !7)
        .ok_or_else(|| EngineError::invalid_input("Connect Arrow schema size overflow".into()))?;
    let framed_len = i32::try_from(padded_message_len).map_err(|_| {
        EngineError::invalid_input("Connect Arrow schema exceeds IPC framing range".into())
    })?;
    let prefix_len: usize = if continuation { 8 } else { 4 };
    let mut output = Vec::with_capacity(
        prefix_len
            .saturating_add(padded_message_len)
            .saturating_add(data.len().saturating_sub(original_message_end)),
    );
    if continuation {
        output.extend_from_slice(&[0xff; 4]);
    }
    output.extend_from_slice(&framed_len.to_le_bytes());
    output.extend_from_slice(&serialized_message);
    output.resize(prefix_len + padded_message_len, 0);
    output.extend_from_slice(&data[original_message_end..]);
    Ok((output, public_names))
}

/// Parse "array<element_type>" to get inner type string. Returns None if not array<>.
pub(super) fn parse_array_element_type(type_str: &str) -> Option<String> {
    let s = type_str.trim();
    if !s
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("array<"))
        || !s.ends_with('>')
    {
        return None;
    }
    Some(s[6..s.len() - 1].trim().to_string())
}

/// Split by commas only at top level (not inside < >). Used for struct<...> and map<...>.
pub(super) fn split_at_top_level_commas(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut angle_depth = 0i32;
    let mut paren_depth = 0i32;
    let mut in_backticks = false;
    let mut start = 0;
    let mut chars = s.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c == '`' {
            if in_backticks && chars.peek().is_some_and(|(_, next)| *next == '`') {
                chars.next();
                continue;
            }
            in_backticks = !in_backticks;
            continue;
        }
        if in_backticks {
            continue;
        }
        match c {
            '<' => angle_depth += 1,
            '>' => angle_depth -= 1,
            '(' => paren_depth += 1,
            ')' => paren_depth -= 1,
            ',' if angle_depth == 0 && paren_depth == 0 => {
                out.push(s[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    if start <= s.len() {
        out.push(s[start..].trim().to_string());
    }
    out
}

/// Parse "struct<field:type,...>" to get field (name, type) pairs. Supports nested struct/map/array.
pub(super) fn parse_struct_fields(type_str: &str) -> Option<Vec<(String, String)>> {
    let s = type_str.trim();
    if !s.to_lowercase().starts_with("struct<") || !s.ends_with('>') {
        return None;
    }
    let inner = s[7..s.len() - 1].trim();
    if inner.is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::new();
    for part in split_at_top_level_commas(inner) {
        let part = part.trim();
        let mut in_backticks = false;
        let mut chars = part.char_indices().peekable();
        let mut separator = None;
        while let Some((idx, c)) = chars.next() {
            if c == '`' {
                if in_backticks && chars.peek().is_some_and(|(_, next)| *next == '`') {
                    chars.next();
                    continue;
                }
                in_backticks = !in_backticks;
            } else if c == ':' && !in_backticks {
                separator = Some(idx);
                break;
            }
        }
        if let Some(idx) = separator {
            let encoded_name = part[..idx].trim();
            let name = if let Some(name) = encoded_name
                .strip_prefix('`')
                .and_then(|name| name.strip_suffix('`'))
            {
                name.replace("``", "`")
            } else {
                encoded_name.to_string()
            };
            let typ = part[idx + 1..].trim().to_string();
            out.push((name, typ));
        }
    }
    Some(out)
}

/// Parse "map<key_type,value_type>" to get (key_type, value_type). Returns None if not map<>.
/// PySpark: MapType(StringType(), StringType()) -> "map<string,string>".
pub(super) fn parse_map_key_value_types(type_str: &str) -> Option<(String, String)> {
    let s = type_str.trim();
    if !s
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("map<"))
        || !s.ends_with('>')
    {
        return None;
    }
    let inner = s[4..s.len() - 1].trim();
    let mut parts = split_at_top_level_commas(inner).into_iter();
    let key_type = parts.next()?;
    let value_type = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    Some((key_type, value_type))
}

/// True if type string is Decimal(precision, scale), e.g. "decimal(10,2)".
pub(super) fn is_decimal_type_str(type_str: &str) -> bool {
    let s = type_str.trim().to_lowercase();
    s.starts_with("decimal(") && s.contains(')')
}

pub(super) fn decimal_transport_value(value: &str) -> &str {
    value
        .strip_prefix("__sparkless_decimal__:")
        .unwrap_or(value)
}

pub(super) fn parse_decimal_precision_scale(type_str: &str) -> Option<(usize, usize)> {
    let args = type_str.trim().to_ascii_lowercase();
    let args = args.strip_prefix("decimal(")?.strip_suffix(')')?;
    let (precision, scale) = args.split_once(',')?;
    let precision = precision.trim().parse().ok()?;
    let scale = scale.trim().parse().ok()?;
    (1..=38).contains(&precision).then_some(())?;
    (scale <= precision).then_some(())?;
    Some((precision, scale))
}

pub(super) fn decimal_to_scaled(
    value: &str,
    precision: usize,
    scale: usize,
) -> Result<i128, PolarsError> {
    if !(1..=38).contains(&precision) || scale > precision {
        return Err(PolarsError::ComputeError(
            format!("invalid decimal precision/scale decimal({precision},{scale})").into(),
        ));
    }
    let value = decimal_transport_value(value);
    let (mantissa, exponent) = value
        .split_once(['e', 'E'])
        .map(|(mantissa, exponent)| {
            exponent
                .parse::<i32>()
                .map(|exponent| (mantissa, exponent))
                .map_err(|_| "invalid decimal exponent")
        })
        .transpose()
        .map_err(|message| PolarsError::ComputeError(message.into()))?
        .unwrap_or((value, 0));
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (whole.is_empty() && fraction.is_empty())
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(PolarsError::ComputeError(
            format!("invalid decimal value {value}").into(),
        ));
    }
    let mut digits = format!("{whole}{fraction}");
    let significant = digits.trim_start_matches('0');
    if significant.is_empty() {
        return Ok(0);
    }
    digits = significant.to_string();
    let shift = scale as i64 - fraction.len() as i64 + i64::from(exponent);
    if shift >= 0 {
        let shift = usize::try_from(shift).map_err(|_| {
            PolarsError::ComputeError("decimal value exceeds supported precision".into())
        })?;
        if digits.len().saturating_add(shift) > 39 {
            return Err(PolarsError::ComputeError(
                "decimal value exceeds supported precision".into(),
            ));
        }
        digits.extend(std::iter::repeat_n('0', shift));
    } else {
        let discarded = usize::try_from(shift.unsigned_abs()).map_err(|_| {
            PolarsError::ComputeError("decimal value exceeds supported precision".into())
        })?;
        let retained_len = digits.len().saturating_sub(discarded);
        let round_up = discarded <= digits.len()
            && digits
                .as_bytes()
                .get(retained_len)
                .is_some_and(|digit| *digit >= b'5');
        digits.truncate(retained_len);
        if digits.is_empty() {
            digits.push('0');
        }
        if round_up {
            let mut carry = true;
            let mut digit_bytes = digits.into_bytes();
            for digit in digit_bytes.iter_mut().rev() {
                if *digit == b'9' {
                    *digit = b'0';
                } else {
                    *digit += 1;
                    carry = false;
                    break;
                }
            }
            if carry {
                digit_bytes.insert(0, b'1');
            }
            digits = String::from_utf8(digit_bytes)
                .expect("decimal digit conversion only contains ASCII digits");
        }
    }
    let scaled = digits.parse::<i128>().map_err(|_| {
        PolarsError::ComputeError("decimal value exceeds supported precision".into())
    })?;
    let scaled = if negative { -scaled } else { scaled };
    let max_abs = 10_i128.pow(precision as u32) - 1;
    if scaled.unsigned_abs() > max_abs as u128 {
        return Err(PolarsError::ComputeError(
            format!("decimal value exceeds declared precision {precision}").into(),
        ));
    }
    Ok(scaled)
}

pub(super) fn json_value_to_decimal_scaled(
    value: &JsonValue,
    precision: usize,
    scale: usize,
) -> Result<Option<i128>, PolarsError> {
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::Number(number) => {
            decimal_to_scaled(&number.to_string(), precision, scale).map(Some)
        }
        JsonValue::String(text) => decimal_to_scaled(text, precision, scale).map(Some),
        other => Err(PolarsError::ComputeError(
            format!("cannot convert {other} to decimal").into(),
        )),
    }
}

/// Try to parse a string as Python dict repr (e.g. `"{'E1': 1, 'E2': 'A'}"`) into JSON Value.
/// PR-F / #691: Sparkless can send struct/map values as single-quoted Python repr.
pub(super) fn python_dict_repr_to_json(s: &str) -> Option<JsonValue> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return None;
    }
    // Protect escaped single quotes, then convert single quotes to double, then restore.
    let step1 = s.replace("\\'", "\u{0001}");
    let step2 = step1.replace('\'', "\"");
    let step3 = step2.replace('\u{0001}', "\\\"");
    serde_json::from_str(&step3).ok()
}

/// #971: Parse a string that may be a JSON object, a JSON-encoded string (one extra quote layer),
/// or a Python dict repr, into an optional JSON object for struct/map columns.
pub(super) fn string_to_json_object(s: &str) -> Option<serde_json::Map<String, JsonValue>> {
    let s = s.trim();
    // Direct JSON object
    if let Ok(v) = serde_json::from_str::<JsonValue>(s) {
        if let Some(obj) = v.as_object() {
            return Some(obj.clone());
        }
        // One level of JSON string encoding (e.g. "\"{'E1': 1, 'E2': 'A'}\"")
        if let Some(inner) = v.as_str() {
            return string_to_json_object(inner);
        }
    }
    python_dict_repr_to_json(s).and_then(|v| v.as_object().cloned())
}

/// Map schema type string to Polars DataType (primitives and list for nested use).
/// Decimal(p,s) uses Polars' fixed-precision decimal dtype.
/// PR-G/#710: array<element_type> and nested array<array<...>> supported.
/// #976: Nested array (e.g. array<array<long>>) recurses; elem_type "array<long>" maps to List(Int64).
/// Array element types timestamp, date, struct<...>, map<...> supported for create_dataframe_from_rows parity.
pub(super) fn json_type_str_to_polars(type_str: &str) -> Option<DataType> {
    let trimmed = type_str.trim();
    let s = trimmed.to_lowercase();
    if is_decimal_type_str(&s) {
        let (precision, scale) = parse_decimal_precision_scale(&s)?;
        return Some(DataType::Decimal(precision, scale));
    }
    if let Some(elem_type) = parse_array_element_type(trimmed) {
        // Special case: array<map<key_type,value_type>> – represented as
        // List(List(Struct{key, value})) so each array element is its own map
        // (list-of-{key,value} pairs) and collect() yields a list of dicts.
        if let Some((key_type, value_type)) = parse_map_key_value_types(elem_type.trim()) {
            let key_dtype = json_type_str_to_polars(key_type.trim())?;
            let value_dtype = json_type_str_to_polars(value_type.trim())?;
            let map_struct = DataType::Struct(vec![
                Field::new("key".into(), key_dtype),
                Field::new("value".into(), value_dtype),
            ]);
            return Some(DataType::List(Box::new(DataType::List(Box::new(
                map_struct,
            )))));
        }
        let inner = json_type_str_to_polars(elem_type.trim())?;
        return Some(DataType::List(Box::new(inner)));
    }
    // Preserve struct field name casing (e.g. "Inner", "E1") for subscript resolution (#339, #1066).
    if let Some(fields) = parse_struct_fields(trimmed) {
        let polars_fields: Vec<Field> = fields
            .into_iter()
            .map(|(name, typ)| {
                let inner = json_type_str_to_polars(typ.trim())?;
                Some(Field::new(name.into(), inner))
            })
            .collect::<Option<Vec<_>>>()?;
        return Some(DataType::Struct(polars_fields));
    }
    if let Some((key_type, value_type)) = parse_map_key_value_types(trimmed) {
        let key_dtype = json_type_str_to_polars(key_type.trim())?;
        let value_dtype = json_type_str_to_polars(value_type.trim())?;
        return Some(DataType::List(Box::new(DataType::Struct(vec![
            Field::new("key".into(), key_dtype),
            Field::new("value".into(), value_dtype),
        ]))));
    }
    match s.as_str() {
        // Preserve distinction between IntegerType (32-bit) and LongType (64-bit) for explicit schemas.
        "int" | "integer" | "int32" => Some(DataType::Int32),
        "smallint" | "short" => Some(DataType::Int16),
        "bigint" | "long" => Some(DataType::Int64),
        "float" => Some(DataType::Float32),
        "double" | "double_precision" => Some(DataType::Float64),
        "string" | "str" | "varchar" => Some(DataType::String),
        "boolean" | "bool" => Some(DataType::Boolean),
        "date" => Some(DataType::Date),
        "timestamp" | "datetime" => Some(DataType::Datetime(
            TimeUnit::Microseconds,
            Some(TimeZone::UTC),
        )),
        "timestamp_ntz" => Some(DataType::Datetime(TimeUnit::Microseconds, None)),
        _ => None,
    }
}

pub(super) fn timestamp_data_type(type_str: &str) -> DataType {
    let timezone = if type_str.trim().eq_ignore_ascii_case("timestamp_ntz") {
        None
    } else {
        Some(TimeZone::UTC)
    };
    DataType::Datetime(TimeUnit::Microseconds, timezone)
}

/// #420: Check that a cell value is compatible with schema type (for verify_schema).
/// Returns Err with a short message when the value is not valid for the given type.
pub(super) fn verify_json_value_for_type(v: &JsonValue, type_str: &str) -> Result<(), String> {
    let t = type_str.trim().to_lowercase();
    if v.is_null() {
        return Ok(());
    }
    match t.as_str() {
        "smallint" | "short" => match v {
            JsonValue::Number(n)
                if n.as_i64().is_some_and(|value| i16::try_from(value).is_ok()) =>
            {
                Ok(())
            }
            JsonValue::String(s) if decimal_transport_value(s).parse::<i16>().is_ok() => Ok(()),
            _ => Err(format!("expected smallint, got {}", value_type_name(v))),
        },
        // Preserve the long-standing row-construction behavior for public `int` aliases.
        // Managed SQL DDL uses `int32` so its target schema receives strict 32-bit checks.
        "int" | "integer" => match v {
            JsonValue::Number(_) => Ok(()),
            JsonValue::String(s) if decimal_transport_value(s).parse::<i64>().is_ok() => Ok(()),
            _ => Err(format!("expected integer, got {}", value_type_name(v))),
        },
        "int32" => match v {
            JsonValue::Number(n)
                if n.as_i64().is_some_and(|value| i32::try_from(value).is_ok()) =>
            {
                Ok(())
            }
            JsonValue::String(s) if decimal_transport_value(s).parse::<i32>().is_ok() => Ok(()),
            _ => Err(format!("expected int32, got {}", value_type_name(v))),
        },
        "tinyint" => match v {
            JsonValue::Number(n) if n.as_i64().is_some_and(|value| i8::try_from(value).is_ok()) => {
                Ok(())
            }
            JsonValue::String(s) if decimal_transport_value(s).parse::<i8>().is_ok() => Ok(()),
            _ => Err(format!("expected tinyint, got {}", value_type_name(v))),
        },
        "bigint" | "long" => match v {
            JsonValue::Number(n) if n.as_i64().is_some() => Ok(()),
            JsonValue::String(s) if decimal_transport_value(s).parse::<i64>().is_ok() => Ok(()),
            _ => Err(format!("expected bigint, got {}", value_type_name(v))),
        },
        "double" | "float" | "double_precision" => match v {
            JsonValue::Number(n) if n.as_f64().is_some_and(f64::is_finite) => Ok(()),
            JsonValue::String(s) if decimal_transport_value(s).parse::<f64>().is_ok() => Ok(()),
            _ => Err(format!(
                "expected double/number, got {}",
                value_type_name(v)
            )),
        },
        "string" | "str" | "varchar" => Ok(()), // any value can be string
        "boolean" | "bool" => match v {
            JsonValue::Bool(_) => Ok(()),
            JsonValue::String(s)
                if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") =>
            {
                Ok(())
            }
            _ => Err(format!("expected boolean, got {}", value_type_name(v))),
        },
        "date" => match v {
            JsonValue::String(value)
                if chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok() =>
            {
                Ok(())
            }
            JsonValue::Object(value) if parse_date_from_json_object(value).is_some() => Ok(()),
            _ => Err(format!("expected date, got {}", value_type_name(v))),
        },
        "timestamp" | "datetime" | "timestamp_ntz" => match v {
            JsonValue::String(value) if parse_timestamp_str_to_micros(value).is_ok() => Ok(()),
            JsonValue::Object(value) if parse_datetime_from_json_object(value).is_some() => Ok(()),
            JsonValue::Number(value) if value.as_i64().is_some() => Ok(()),
            _ => Err(format!("expected timestamp, got {}", value_type_name(v))),
        },
        "binary" => match v {
            JsonValue::String(value)
                if base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    value.as_bytes(),
                )
                .is_ok() =>
            {
                Ok(())
            }
            _ => Err(format!(
                "expected base64 binary, got {}",
                value_type_name(v)
            )),
        },
        _ if is_decimal_type_str(&t) => {
            let (precision, scale) = parse_decimal_precision_scale(&t)
                .ok_or_else(|| format!("invalid decimal type {t}"))?;
            let value = match v {
                JsonValue::Number(number) => number.to_string(),
                JsonValue::String(text) => decimal_transport_value(text).to_string(),
                _ => return Err(format!("expected decimal, got {}", value_type_name(v))),
            };
            decimal_to_scaled(&value, precision, scale)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        _ => Ok(()), // date, timestamp, array, struct, etc.: allow and let build fail if needed
    }
}

pub(super) fn value_type_name(v: &JsonValue) -> &'static str {
    match v {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

/// Convert Python repr to JSON-like string for parsing (#1016: "[True, False, True]" -> "[true, false, true]").
pub(super) fn python_repr_to_json_like(s: &str) -> String {
    let mut out = s.to_string();
    // Word-boundary safe: replace Python literals that appear as list elements (comma or bracket before/after).
    out = out
        .replace("True", "true")
        .replace("False", "false")
        .replace("None", "null");
    out
}

/// Normalize a JSON value to an array for array columns (PySpark parity #625).
/// Accepts: Array, Object with "0","1",... keys (Python list serialization), String that parses as JSON array.
/// #1016: String may be Python repr (e.g. "[True, False, True]"); convert to JSON-like and parse.
/// Returns None for null or when value should be treated as single-element list (#611).
pub(super) fn json_value_to_array(v: &JsonValue) -> Option<Vec<JsonValue>> {
    match v {
        JsonValue::Null => None,
        JsonValue::Array(arr) => Some(arr.clone()),
        JsonValue::Object(obj) => {
            // Python/serialization sometimes sends list as {"0": x, "1": y}. Build sorted by index.
            let mut indices: Vec<usize> =
                obj.keys().filter_map(|k| k.parse::<usize>().ok()).collect();
            indices.sort_unstable();
            if indices.is_empty() {
                return None;
            }
            let arr: Vec<JsonValue> = indices
                .iter()
                .filter_map(|i| obj.get(&i.to_string()).cloned())
                .collect();
            Some(arr)
        }
        JsonValue::String(s) => {
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                parsed.as_array().cloned()
            } else {
                // #1016: Python repr e.g. "[True, False, True]" — not valid JSON; normalize and retry.
                let json_like = python_repr_to_json_like(s);
                serde_json::from_str::<JsonValue>(&json_like)
                    .ok()
                    .and_then(|p| p.as_array().cloned())
            }
        }
        _ => None,
    }
}

/// Merge inferred Spark type name strings (PySpark createDataFrame parity).
pub(super) fn merge_spark_type_names(a: &str, b: &str) -> String {
    let a = a.trim().to_ascii_lowercase();
    let b = b.trim().to_ascii_lowercase();
    if a == b {
        return a;
    }
    if let (Some(ea), Some(eb)) = (
        parse_array_element_type_name(&a),
        parse_array_element_type_name(&b),
    ) {
        if ea.is_empty() && !eb.is_empty() {
            return format_array_type_name(&eb);
        }
        if eb.is_empty() && !ea.is_empty() {
            return format_array_type_name(&ea);
        }
        if ea.is_empty() && eb.is_empty() {
            return "array".to_string();
        }
        return format_array_type_name(&merge_spark_type_names(&ea, &eb));
    }
    if let Some(eb) = parse_array_element_type_name(&b) {
        if a == "string" {
            return format_array_type_name(&eb);
        }
    }
    if let Some(ea) = parse_array_element_type_name(&a) {
        if b == "string" {
            return format_array_type_name(&ea);
        }
    }
    if a == "string" || b == "string" {
        return "string".to_string();
    }
    if (a == "long" || a == "bigint" || a == "int") && (b == "double" || b == "float") {
        return "double".to_string();
    }
    if (b == "long" || b == "bigint" || b == "int") && (a == "double" || a == "float") {
        return "double".to_string();
    }
    if (a == "long" || a == "bigint" || a == "int") && (b == "long" || b == "bigint" || b == "int")
    {
        return "long".to_string();
    }
    "string".to_string()
}

pub(super) fn parse_array_element_type_name(s: &str) -> Option<String> {
    let trimmed = s.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower == "array" || lower == "list" {
        return Some(String::new());
    }
    if lower.starts_with("array<") && lower.ends_with('>') {
        return Some(trimmed[6..trimmed.len() - 1].trim().to_string());
    }
    None
}

pub(super) fn format_array_type_name(elem: &str) -> String {
    if elem.trim().is_empty() {
        "array".to_string()
    } else {
        format!("array<{}>", elem.trim())
    }
}

pub(super) fn merge_inferred_types<I>(types: I) -> String
where
    I: IntoIterator<Item = String>,
{
    types.into_iter().fold(String::new(), |acc, t| {
        if acc.is_empty() {
            t
        } else if t.is_empty() {
            acc
        } else {
            merge_spark_type_names(&acc, &t)
        }
    })
}

pub(super) fn infer_array_type_from_json_array(arr: &[JsonValue]) -> String {
    if arr.is_empty() {
        return "array".to_string();
    }
    let elem = merge_inferred_types(
        arr.iter()
            .filter_map(SparkSession::infer_dtype_from_json_value),
    );
    format_array_type_name(&elem)
}

pub(super) fn json_element_type_to_polars(elem_type: &str) -> DataType {
    match elem_type.trim().to_ascii_lowercase().as_str() {
        "boolean" | "bool" => DataType::Boolean,
        "bigint" | "long" | "int" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        _ => DataType::String,
    }
}

/// Infer list element type by merging across all non-empty arrays in the column.
/// #976: When first element is itself an array (nested array), infer inner type and return ("array<inner>", List(inner_dtype)).
pub(super) fn infer_list_element_type(
    rows: &[Vec<JsonValue>],
    col_idx: usize,
) -> Option<(String, DataType)> {
    let mut merged_elem_type = String::new();
    for row in rows {
        let v = row.get(col_idx)?;
        let arr = json_value_to_array(v)?;
        if arr.is_empty() {
            if merged_elem_type.is_empty() {
                merged_elem_type = "array".to_string();
            }
            continue;
        }
        let row_type = infer_array_type_from_json_array(&arr);
        merged_elem_type = if merged_elem_type.is_empty() {
            row_type
        } else {
            merge_spark_type_names(&merged_elem_type, &row_type)
        };
    }
    if merged_elem_type.is_empty() {
        return None;
    }
    if merged_elem_type == "array" {
        return Some(("bigint".to_string(), DataType::Int64));
    }
    if let Some(elem) = parse_array_element_type_name(&merged_elem_type) {
        if elem.to_lowercase().starts_with("array<") {
            if let Some(inner) = json_type_str_to_polars(&elem) {
                return Some((format!("array<{elem}>"), DataType::List(Box::new(inner))));
            }
        }
        // First tuple element is the scalar element type for json_value_to_series_single, not array<...>.
        return Some((
            elem.clone(),
            DataType::List(Box::new(json_element_type_to_polars(&elem))),
        ));
    }
    None
}

/// Build a length-N Series from `Vec<Option<JsonValue>>` for a given type (recursive for struct/array).
pub(super) fn json_values_to_series(
    values: &[Option<JsonValue>],
    type_str: &str,
    name: &str,
) -> Result<Series, PolarsError> {
    use chrono::NaiveDate;
    let epoch = date_utils::epoch_naive_date();
    let type_lower = type_str.trim().to_lowercase();

    if let Some(elem_type) = parse_array_element_type(type_str) {
        let inner_dtype = json_type_str_to_polars(&elem_type).ok_or_else(|| {
            PolarsError::ComputeError(
                format!("array element type '{elem_type}' not supported").into(),
            )
        })?;
        let mut builder = get_list_builder(&inner_dtype, 64, values.len(), name.into());
        for v in values.iter() {
            if v.as_ref().is_none_or(|x| matches!(x, JsonValue::Null)) {
                builder.append_null();
            } else if let Some(arr) = v.as_ref().and_then(json_value_to_array) {
                // #625: Array, Object with "0","1",..., or string that parses as JSON array (PySpark list parity).
                // #710/PR-G: Nested array (elem_type is array<...>): one list series per element, then vals + from_any_values.
                let elem_series: Vec<Series> = if parse_array_element_type(&elem_type).is_some() {
                    arr.iter()
                        .map(|e| json_values_to_series(&[Some(e.clone())], &elem_type, "elem"))
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    arr.iter()
                        .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                        .collect::<Result<Vec<_>, _>>()?
                };
                let vals: Vec<_> = elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                let s = Series::from_any_values_and_dtype(
                    PlSmallStr::EMPTY,
                    &vals,
                    &inner_dtype,
                    false,
                )
                .map_err(|e| PolarsError::ComputeError(format!("array elem: {e}").into()))?;
                builder.append_series(&s)?;
            } else {
                // #611: PySpark accepts single value as one-element list for array columns.
                let single_arr = [v.clone().unwrap_or(JsonValue::Null)];
                let elem_series: Vec<Series> = if parse_array_element_type(&elem_type).is_some() {
                    single_arr
                        .iter()
                        .map(|e| json_values_to_series(&[Some(e.clone())], &elem_type, "elem"))
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    single_arr
                        .iter()
                        .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                        .collect::<Result<Vec<_>, _>>()?
                };
                let vals: Vec<_> = elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                let arr_series = Series::from_any_values_and_dtype(
                    PlSmallStr::EMPTY,
                    &vals,
                    &inner_dtype,
                    false,
                )
                .map_err(|e| PolarsError::ComputeError(format!("array elem: {e}").into()))?;
                builder.append_series(&arr_series)?;
            }
        }
        return Ok(builder.finish().into_series());
    }

    if parse_map_key_value_types(type_str).is_some() {
        let dtype = json_type_str_to_polars(type_str).ok_or_else(|| {
            PolarsError::ComputeError(format!("map type '{type_str}' not supported").into())
        })?;
        let null_value = JsonValue::Null;
        let values = values
            .iter()
            .map(
                |value| -> Result<polars::prelude::AnyValue<'static>, PolarsError> {
                    let value = value.as_ref().unwrap_or(&null_value);
                    let series = json_value_to_series_single(value, type_str, name)?;
                    Ok(series.get(0)?.into_static())
                },
            )
            .collect::<Result<Vec<_>, PolarsError>>()?;
        return Series::from_any_values_and_dtype(name.into(), &values, &dtype, true);
    }

    // Use type_str (not type_lower) so struct field names preserve case (e.g. E1, E2); col("StructValue.E1") then resolves when case_sensitive.
    if let Some(fields) = parse_struct_fields(type_str.trim()) {
        let mut field_series_vec: Vec<Vec<Option<JsonValue>>> = (0..fields.len())
            .map(|_| Vec::with_capacity(values.len()))
            .collect();
        let mut outer_validity = Vec::with_capacity(values.len());
        for v in values.iter() {
            // #610: Accept string that parses as JSON object or array (e.g. Python tuple serialized as "[1, \"y\"]").
            // #691/PR-F: Accept Python dict repr string (e.g. "{'a': 1, 'b': 'x'}").
            // #610/#691/#971: Accept string that parses as JSON object/array or Python dict repr; allow one level of JSON encoding.
            let effective: Option<JsonValue> = match v.as_ref() {
                Some(JsonValue::String(s)) => {
                    if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                        if parsed.is_object() || parsed.is_array() {
                            Some(parsed)
                        } else if let Some(inner) = parsed.as_str() {
                            string_to_json_object(inner)
                                .map(JsonValue::Object)
                                .or_else(|| v.clone())
                        } else {
                            v.clone()
                        }
                    } else if let Some(obj) = string_to_json_object(s) {
                        Some(JsonValue::Object(obj))
                    } else {
                        v.clone()
                    }
                }
                _ => v.clone(),
            };
            // StructChunked::from_series only sees the child arrays. When every child is
            // null it cannot tell a null parent struct from a valid struct whose children
            // are null, so preserve the parent's validity bitmap separately.
            outer_validity.push(
                effective
                    .as_ref()
                    .is_some_and(|value| !matches!(value, JsonValue::Null)),
            );
            if effective
                .as_ref()
                .is_none_or(|x| matches!(x, JsonValue::Null))
            {
                for fc in &mut field_series_vec {
                    fc.push(None);
                }
            } else if let Some(arr) = effective
                .as_ref()
                .and_then(|x| x.as_array().cloned())
                .or_else(|| effective.as_ref().and_then(json_value_to_array))
            {
                // #634: Array or object with "0","1",... keys (Python tuple serialization) — positional.
                for (fi, _) in fields.iter().enumerate() {
                    field_series_vec[fi].push(arr.get(fi).cloned());
                }
            } else if let Some(obj) = effective.as_ref().and_then(|x| x.as_object()) {
                // #971: case-insensitive field lookup (Python dict may have "E1"/"E2", schema "e1"/"e2").
                // #1015: Sparkless may send struct as {"E1": v1, "E2": v2}; fallback by position so we build with schema names (a, b) and collect() yields row["nested"]["a"].
                for (fi, (fname, _)) in fields.iter().enumerate() {
                    let val = obj
                        .get(fname)
                        .or_else(|| {
                            obj.keys()
                                .find(|k| k.eq_ignore_ascii_case(fname))
                                .and_then(|k| obj.get(k))
                        })
                        .or_else(|| obj.get(&format!("E{}", fi + 1)));
                    field_series_vec[fi].push(val.cloned());
                }
            } else if let Some(JsonValue::String(s)) = v.as_ref() {
                // #973: final attempt — parse string as object or array (tuple/list serialization).
                let trimmed = s.trim();
                let parsed_obj = string_to_json_object(trimmed);
                let parsed_arr: Option<Vec<JsonValue>> = serde_json::from_str(trimmed)
                    .ok()
                    .and_then(|j: JsonValue| j.as_array().cloned());
                if let Some(obj) = parsed_obj {
                    for (fi, (fname, _)) in fields.iter().enumerate() {
                        let val = obj
                            .get(fname)
                            .or_else(|| {
                                obj.keys()
                                    .find(|k| k.eq_ignore_ascii_case(fname))
                                    .and_then(|k| obj.get(k))
                            })
                            .or_else(|| obj.get(&format!("E{}", fi + 1)));
                        field_series_vec[fi].push(val.cloned());
                    }
                } else if let Some(arr) = parsed_arr {
                    for (fi, _) in fields.iter().enumerate() {
                        field_series_vec[fi].push(arr.get(fi).cloned());
                    }
                } else {
                    return Err(PolarsError::ComputeError(
                        "struct value must be object (by field name) or array (by position). \
                         PySpark accepts dict or tuple/list for struct columns."
                            .into(),
                    ));
                }
            } else {
                return Err(PolarsError::ComputeError(
                    "struct value must be object (by field name) or array (by position). \
                     PySpark accepts dict or tuple/list for struct columns."
                        .into(),
                ));
            }
        }
        let series_per_field: Vec<Series> = fields
            .iter()
            .enumerate()
            .map(|(fi, (fname, ftype))| json_values_to_series(&field_series_vec[fi], ftype, fname))
            .collect::<Result<Vec<_>, _>>()?;
        let field_refs: Vec<&Series> = series_per_field.iter().collect();
        let st = StructChunked::from_series(name.into(), values.len(), field_refs.iter().copied())
            .map_err(|e| PolarsError::ComputeError(format!("struct column: {e}").into()))?
            .with_outer_validity(Some(Bitmap::from_iter(outer_validity)))
            .into_series();
        return Ok(st);
    }

    // #978: Support integer/smallint/tinyint, double_precision, and decimal(...,...) so json_value_to_series doesn't hit "unsupported type".
    if is_decimal_type_str(&type_lower) {
        let (precision, scale) = parse_decimal_precision_scale(&type_lower).ok_or_else(|| {
            PolarsError::ComputeError(format!("invalid decimal type {type_lower}").into())
        })?;
        let vals: Vec<Option<i128>> = values
            .iter()
            .map(|value| {
                value.as_ref().map_or(Ok(None), |value| {
                    json_value_to_decimal_scaled(value, precision, scale)
                })
            })
            .collect::<Result<_, PolarsError>>()?;
        return Series::new(name.into(), vals)
            .into_decimal(precision, scale)
            .map(|series| series.into_series());
    }

    match type_lower.as_str() {
        "int32" => {
            let vals: Vec<Option<i32>> = values
                .iter()
                .map(|value| {
                    value.as_ref().and_then(|value| match value {
                        JsonValue::Number(number) => {
                            number.as_i64().and_then(|value| i32::try_from(value).ok())
                        }
                        JsonValue::String(value) => {
                            decimal_transport_value(value).parse::<i32>().ok()
                        }
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        "smallint" | "short" => {
            let vals: Vec<Option<i16>> = values
                .iter()
                .map(|value| {
                    value.as_ref().and_then(|value| match value {
                        JsonValue::Number(number) => {
                            number.as_i64().and_then(|value| i16::try_from(value).ok())
                        }
                        JsonValue::String(value) => {
                            decimal_transport_value(value).parse::<i16>().ok()
                        }
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        "int" | "integer" | "bigint" | "long" | "tinyint" => {
            let vals: Vec<Option<i64>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::Number(n) => n.as_i64(),
                        JsonValue::String(s) => decimal_transport_value(s).parse::<i64>().ok(),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        "double" | "double_precision" => {
            let vals: Vec<Option<f64>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::Number(n) => n.as_f64(),
                        JsonValue::String(s) => decimal_transport_value(s).parse::<f64>().ok(),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        "float" => {
            let vals: Vec<Option<f32>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::Number(n) => n.as_f64().map(|value| value as f32),
                        JsonValue::String(s) => decimal_transport_value(s).parse::<f32>().ok(),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        "string" | "str" | "varchar" => {
            let vals: Vec<Option<&str>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::String(s) => Some(decimal_transport_value(s)),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            let owned: Vec<Option<String>> =
                vals.into_iter().map(|o| o.map(|s| s.to_string())).collect();
            Ok(Series::new(name.into(), owned))
        }
        "boolean" | "bool" => {
            // #978: Accept Bool, Number(0/1), and String("true"/"false"/"1"/"0") for boolean column.
            let vals: Vec<Option<bool>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::Bool(b) => Some(*b),
                        JsonValue::Number(n) => n
                            .as_i64()
                            .map(|i| i != 0)
                            .or_else(|| n.as_f64().map(|f| f != 0.0)),
                        JsonValue::String(s) => {
                            let s = s.trim().to_lowercase();
                            match s.as_str() {
                                "true" | "1" => Some(true),
                                "false" | "0" => Some(false),
                                _ => None,
                            }
                        }
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        "date" => {
            let vals: Vec<Option<i32>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::String(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .ok()
                            .map(|d| (d - epoch).num_days() as i32),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            let s = Series::new(name.into(), vals);
            s.cast(&DataType::Date)
                .map_err(|e| PolarsError::ComputeError(format!("date cast: {e}").into()))
        }
        "timestamp" | "datetime" | "timestamp_ntz" => {
            let vals: Vec<Option<i64>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::String(s) => parse_timestamp_str_to_micros(s).ok(),
                        JsonValue::Object(obj) => parse_datetime_from_json_object(obj),
                        JsonValue::Number(n) => n.as_i64(),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            let s = Series::new(name.into(), vals);
            s.cast(&timestamp_data_type(type_str))
                .map_err(|e| PolarsError::ComputeError(format!("datetime cast: {e}").into()))
        }
        "binary" => {
            let vals: Vec<Option<Vec<u8>>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::String(s) => base64::engine::general_purpose::STANDARD
                            .decode(s.as_bytes())
                            .ok(),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        _ => Err(PolarsError::ComputeError(
            format!("json_values_to_series: unsupported type '{type_str}'").into(),
        )),
    }
}

/// Build a single Series from a JsonValue for use as list element or struct field.
pub(super) fn json_value_to_series_single(
    value: &JsonValue,
    type_str: &str,
    name: &str,
) -> Result<Series, PolarsError> {
    use chrono::NaiveDate;
    use polars::datatypes::ListChunked;
    use polars::prelude::{DataType, Field};
    let epoch = date_utils::epoch_naive_date();
    let type_trimmed = type_str.trim();
    let type_lower = type_trimmed.to_lowercase();
    // #1066: Nested array (e.g. createDataFrame with {"matrix": [[1,2,3],[4,5,6]]}) when type is array<...>.
    if let (JsonValue::Array(arr), Some(elem_type)) =
        (value, parse_array_element_type(type_trimmed))
    {
        let inner_dtype = json_type_str_to_polars(&elem_type).ok_or_else(|| {
            PolarsError::ComputeError(
                format!("array element type '{elem_type}' not supported").into(),
            )
        })?;
        let elem_series: Vec<Series> = if parse_array_element_type(&elem_type).is_some() {
            arr.iter()
                .map(|e| json_values_to_series(&[Some(e.clone())], &elem_type, "elem"))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            arr.iter()
                .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                .collect::<Result<Vec<_>, _>>()?
        };
        let vals: Vec<_> = elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
        let s = Series::from_any_values_and_dtype(PlSmallStr::EMPTY, &vals, &inner_dtype, false)
            .map_err(|e| PolarsError::ComputeError(format!("array elem: {e}").into()))?;
        let mut builder = get_list_builder(&inner_dtype, 64, 1, name.into());
        builder.append_series(&s)?;
        return Ok(builder.finish().into_series());
    }
    // Struct element type (e.g. array<struct<name:string,age:int>>)
    if let Some(fields) = parse_struct_fields(type_trimmed) {
        if let Some(st) = json_object_or_array_to_struct_series(value, &fields, name)? {
            return Ok(st);
        }
        // Null value: build a single null struct of the appropriate dtype.
        if let Some(dtype) = json_type_str_to_polars(type_trimmed) {
            let s = Series::new_null(name.into(), 1).cast(&dtype)?;
            return Ok(s);
        }
    }
    // Map element type (e.g. array<map<string,int>> where elem_type is "map<string,int>")
    if let Some((key_type, value_type)) = parse_map_key_value_types(type_trimmed) {
        let key_dtype = json_type_str_to_polars(key_type.trim()).ok_or_else(|| {
            PolarsError::ComputeError(
                format!("array element key type '{key_type}' not supported").into(),
            )
        })?;
        let value_dtype = json_type_str_to_polars(value_type.trim()).ok_or_else(|| {
            PolarsError::ComputeError(
                format!("array element value type '{value_type}' not supported").into(),
            )
        })?;
        // Null map: represent as a null list-of-struct (one element, null).
        if matches!(value, JsonValue::Null) {
            let struct_dtype = DataType::Struct(vec![
                Field::new("key".into(), key_dtype.clone()),
                Field::new("value".into(), value_dtype.clone()),
            ]);
            let lc = ListChunked::full_null_with_dtype(name.into(), 1, &struct_dtype);
            return Ok(lc.into_series());
        }
        // Object map or string that parses to JSON/Python dict repr.
        let obj_opt = match value {
            JsonValue::Object(obj) => Some(obj.clone()),
            JsonValue::String(s) => string_to_json_object(s),
            _ => None,
        };
        if let Some(obj) = obj_opt {
            let st = json_object_to_map_struct_series(
                &obj,
                &key_type,
                &value_type,
                &key_dtype,
                &value_dtype,
                name,
            )?;
            let struct_dtype = st.dtype().clone();
            let mut builder = get_list_builder(&struct_dtype, st.len(), 1, name.into());
            builder.append_series(&st)?;
            return Ok(builder.finish().into_series());
        }
        return Err(PolarsError::ComputeError(
            format!("json_value_to_series: unsupported {type_str} for {value:?}").into(),
        ));
    }
    match (value, type_lower.as_str()) {
        (JsonValue::Null, _) => Ok(Series::new_null(name.into(), 1)),
        (JsonValue::Number(n), "int32") => Ok(Series::new(
            name.into(),
            vec![n.as_i64().and_then(|value| i32::try_from(value).ok())],
        )),
        (JsonValue::String(s), "int32") => {
            let value = decimal_transport_value(s)
                .trim()
                .parse::<i32>()
                .map_err(|error| PolarsError::ComputeError(format!("int parse: {error}").into()))?;
            Ok(Series::new(name.into(), vec![Some(value)]))
        }
        (JsonValue::Number(n), "smallint" | "short") => Ok(Series::new(
            name.into(),
            vec![n.as_i64().and_then(|value| i16::try_from(value).ok())],
        )),
        (JsonValue::String(s), "smallint" | "short") => {
            let value = decimal_transport_value(s)
                .trim()
                .parse::<i16>()
                .map_err(|error| {
                    PolarsError::ComputeError(format!("smallint parse: {error}").into())
                })?;
            Ok(Series::new(name.into(), vec![Some(value)]))
        }
        (JsonValue::Number(n), "int" | "integer" | "bigint" | "long" | "tinyint") => {
            Ok(Series::new(name.into(), vec![n.as_i64()]))
        }
        (JsonValue::String(s), "int" | "integer" | "bigint" | "long" | "tinyint") => {
            let i = decimal_transport_value(s)
                .trim()
                .parse::<i64>()
                .map_err(|e| PolarsError::ComputeError(format!("int parse: {e}").into()))?;
            Ok(Series::new(name.into(), vec![Some(i)]))
        }
        (JsonValue::Number(n), "double" | "double_precision") => {
            Ok(Series::new(name.into(), vec![n.as_f64()]))
        }
        (JsonValue::String(s), "double" | "double_precision") => {
            let f = decimal_transport_value(s)
                .trim()
                .parse::<f64>()
                .map_err(|e| PolarsError::ComputeError(format!("float parse: {e}").into()))?;
            Ok(Series::new(name.into(), vec![Some(f)]))
        }
        (JsonValue::Number(n), "float") => Ok(Series::new(
            name.into(),
            vec![n.as_f64().map(|value| value as f32)],
        )),
        (JsonValue::String(s), "float") => {
            let value = decimal_transport_value(s)
                .trim()
                .parse::<f32>()
                .map_err(|error| {
                    PolarsError::ComputeError(format!("float parse: {error}").into())
                })?;
            Ok(Series::new(name.into(), vec![Some(value)]))
        }
        (JsonValue::Number(n), t) if is_decimal_type_str(t) => {
            let (precision, scale) = parse_decimal_precision_scale(t).ok_or_else(|| {
                PolarsError::ComputeError(format!("invalid decimal type {t}").into())
            })?;
            Series::new(
                name.into(),
                vec![Some(decimal_to_scaled(&n.to_string(), precision, scale)?)],
            )
            .into_decimal(precision, scale)
            .map(|series| series.into_series())
        }
        (JsonValue::String(s), t) if is_decimal_type_str(t) => {
            let (precision, scale) = parse_decimal_precision_scale(t).ok_or_else(|| {
                PolarsError::ComputeError(format!("invalid decimal type {t}").into())
            })?;
            Series::new(
                name.into(),
                vec![Some(decimal_to_scaled(s, precision, scale)?)],
            )
            .into_decimal(precision, scale)
            .map(|series| series.into_series())
        }
        (JsonValue::String(s), "string" | "str" | "varchar") => {
            Ok(Series::new(name.into(), vec![decimal_transport_value(s)]))
        }
        (JsonValue::Number(n), "string" | "str" | "varchar") => {
            // #971: map<string,*> values may be numbers when Python sends dict with mixed types; coerce to string.
            let s = n
                .as_f64()
                .map(|f| f.to_string())
                .or_else(|| n.as_i64().map(|i| i.to_string()))
                .unwrap_or_else(|| "null".to_string());
            Ok(Series::new(name.into(), vec![s.as_str()]))
        }
        (JsonValue::Bool(b), "boolean" | "bool") => Ok(Series::new(name.into(), vec![*b])),
        (JsonValue::Number(n), "boolean" | "bool") => {
            let b = n
                .as_i64()
                .map(|i| i != 0)
                .or_else(|| n.as_f64().map(|f| f != 0.0))
                .unwrap_or(false);
            Ok(Series::new(name.into(), vec![b]))
        }
        (JsonValue::String(s), "boolean" | "bool") => {
            let b = match s.trim().to_lowercase().as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => {
                    return Err(PolarsError::ComputeError(
                        format!("boolean parse: expected true/false/1/0, got {:?}", s).into(),
                    ));
                }
            };
            Ok(Series::new(name.into(), vec![b]))
        }
        (JsonValue::String(s), "date") => {
            let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| PolarsError::ComputeError(format!("date parse: {e}").into()))?;
            let days = (d - epoch).num_days() as i32;
            let s = Series::new(name.into(), vec![days]).cast(&DataType::Date)?;
            Ok(s)
        }
        (JsonValue::Object(obj), "date") => {
            let days = parse_date_from_json_object(obj).ok_or_else(|| {
                PolarsError::ComputeError("date object: missing year/month/day".into())
            })?;
            let s = Series::new(name.into(), vec![days]).cast(&DataType::Date)?;
            Ok(s)
        }
        (JsonValue::String(s), "timestamp" | "datetime" | "timestamp_ntz") => {
            let micros = parse_timestamp_str_to_micros(s)?;
            let s = Series::new(name.into(), vec![Some(micros)])
                .cast(&timestamp_data_type(type_str))?;
            Ok(s)
        }
        (JsonValue::Number(n), "timestamp" | "datetime" | "timestamp_ntz") => {
            let micros = n.as_i64();
            let s = Series::new(name.into(), vec![micros]).cast(&timestamp_data_type(type_str))?;
            Ok(s)
        }
        (JsonValue::Object(obj), "timestamp" | "datetime" | "timestamp_ntz") => {
            let micros = parse_datetime_from_json_object(obj).ok_or_else(|| {
                PolarsError::ComputeError("datetime object: missing year/month/day".into())
            })?;
            let s = Series::new(name.into(), vec![Some(micros)])
                .cast(&timestamp_data_type(type_str))?;
            Ok(s)
        }
        _ => Err(PolarsError::ComputeError(
            format!("json_value_to_series: unsupported {type_str} for {value:?}").into(),
        )),
    }
}

/// Parse date from JSON object with year, month, day (e.g. Python datetime.date serialization). Returns days since epoch.
pub(super) fn parse_date_from_json_object(obj: &serde_json::Map<String, JsonValue>) -> Option<i32> {
    let year = obj.get("year").and_then(|v| v.as_i64())? as i32;
    let month = obj.get("month").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
    let day = obj.get("day").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
    let epoch = date_utils::epoch_naive_date();
    chrono::NaiveDate::from_ymd_opt(year, month, day).map(|d| (d - epoch).num_days() as i32)
}

/// Parse datetime from JSON object (year, month, day, hour?, minute?, second?). Returns microseconds since epoch.
pub(super) fn parse_datetime_from_json_object(
    obj: &serde_json::Map<String, JsonValue>,
) -> Option<i64> {
    let year = obj.get("year").and_then(|v| v.as_i64())? as i32;
    let month = obj.get("month").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
    let day = obj.get("day").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
    let hour = obj.get("hour").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let min = obj.get("minute").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let sec = obj.get("second").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let d = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
    let dt = d.and_hms_opt(hour, min, sec)?;
    Some(dt.and_utc().timestamp_micros())
}

/// Parse a single timestamp string to microseconds since epoch (for json_value_to_series_single).
pub(super) fn parse_timestamp_str_to_micros(s: &str) -> Result<i64, PolarsError> {
    use chrono::{DateTime, NaiveDate, NaiveDateTime};
    let s = s.trim().trim_end_matches('Z');
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(s) {
        return Ok(timestamp.timestamp_micros());
    }
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        })
        .or_else(|_| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
                .and_then(|d| {
                    d.and_hms_opt(0, 0, 0)
                        .ok_or_else(|| PolarsError::ComputeError("date to datetime (0:0:0)".into()))
                })
        })
        .map(|dt| dt.and_utc().timestamp_micros())
}

/// Build a struct Series from JsonValue::Object or JsonValue::Array (field-order) or Null.
#[allow(dead_code)]
pub(super) fn json_object_or_array_to_struct_series(
    value: &JsonValue,
    fields: &[(String, String)],
    name: &str,
) -> Result<Option<Series>, PolarsError> {
    use polars::prelude::StructChunked;
    if matches!(value, JsonValue::Null) {
        return Ok(None);
    }
    // #610: Accept string that parses as JSON object or array. #691/PR-F: Python dict repr.
    let effective = match value {
        JsonValue::String(s) => {
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                if parsed.is_object() || parsed.is_array() {
                    parsed
                } else {
                    value.clone()
                }
            } else if let Some(parsed) = python_dict_repr_to_json(s) {
                parsed
            } else {
                value.clone()
            }
        }
        _ => value.clone(),
    };
    let mut field_series: Vec<Series> = Vec::with_capacity(fields.len());
    // #634: Positional struct from array or from object with "0","1",... keys (Python tuple).
    let pos_arr_owned: Option<Vec<JsonValue>> = effective
        .as_array()
        .cloned()
        .or_else(|| json_value_to_array(&effective));
    let pos_arr = pos_arr_owned.as_deref();
    for (idx, (fname, ftype)) in fields.iter().enumerate() {
        let fval = if let Some(arr) = pos_arr {
            arr.get(idx).unwrap_or(&JsonValue::Null)
        } else if let Some(obj) = effective.as_object() {
            // #1015: object may have E1,E2 keys (Sparkless); use schema name then positional so struct has names (a, b).
            obj.get(fname)
                .or_else(|| obj.get(&format!("E{}", idx + 1)))
                .unwrap_or(&JsonValue::Null)
        } else {
            return Err(PolarsError::ComputeError(
                "struct value must be object (by field name) or array (by position). \
                 PySpark accepts dict or tuple/list for struct columns."
                    .into(),
            ));
        };
        let s = json_value_to_series_single(fval, ftype, fname)?;
        field_series.push(s);
    }
    let field_refs: Vec<&Series> = field_series.iter().collect();
    let st = StructChunked::from_series(name.into(), 1, field_refs.iter().copied())
        .map_err(|e| PolarsError::ComputeError(format!("struct from value: {e}").into()))?
        .into_series();
    Ok(Some(st))
}

/// Build a single row's map column value as List(Struct{key, value}) element from a JSON object.
/// PySpark parity #627: create_dataframe_from_rows accepts dict for map columns.
pub(super) fn json_object_to_map_struct_series(
    obj: &serde_json::Map<String, JsonValue>,
    key_type: &str,
    value_type: &str,
    key_dtype: &DataType,
    value_dtype: &DataType,
    _name: &str,
) -> Result<Series, PolarsError> {
    if obj.is_empty() {
        let key_series = Series::new("key".into(), Vec::<String>::new());
        let value_series = Series::new_empty(PlSmallStr::EMPTY, value_dtype);
        let st = StructChunked::from_series(
            PlSmallStr::EMPTY,
            0,
            [&key_series, &value_series].iter().copied(),
        )
        .map_err(|e| PolarsError::ComputeError(format!("map struct empty: {e}").into()))?
        .into_series();
        return Ok(st);
    }
    let keys: Vec<String> = obj.keys().cloned().collect();
    let mut value_series = None::<Series>;
    for v in obj.values() {
        let s = json_value_to_series_single(v, value_type, "value")?;
        value_series = Some(match value_series.take() {
            None => s,
            Some(mut acc) => {
                acc.extend(&s).map_err(|e| {
                    PolarsError::ComputeError(format!("map value extend: {e}").into())
                })?;
                acc
            }
        });
    }
    let value_series =
        value_series.unwrap_or_else(|| Series::new_empty(PlSmallStr::EMPTY, value_dtype));
    let key_series = Series::new("key".into(), keys.clone());
    let key_series = if key_type.trim().to_lowercase().as_str() == "string"
        || key_type.trim().to_lowercase().as_str() == "str"
        || key_type.trim().to_lowercase().as_str() == "varchar"
    {
        key_series
    } else {
        key_series
            .cast(key_dtype)
            .map_err(|e| PolarsError::ComputeError(format!("map key cast: {e}").into()))?
    };
    let st = StructChunked::from_series(
        PlSmallStr::EMPTY,
        key_series.len(),
        [&key_series, &value_series].iter().copied(),
    )
    .map_err(|e| PolarsError::ComputeError(format!("map struct: {e}").into()))?
    .into_series();
    Ok(st)
}
