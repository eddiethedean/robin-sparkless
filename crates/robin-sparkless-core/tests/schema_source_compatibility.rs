use robin_sparkless_core::{DataType, LogicalField, StructField, StructType};

#[test]
fn legacy_three_field_literals_and_typed_field_slices_still_compile() {
    let field = StructField {
        name: "id".into(),
        data_type: DataType::Long,
        nullable: false,
    };
    let schema = StructType::new(vec![StructField {
        name: "payload".into(),
        data_type: DataType::Struct(vec![field]),
        nullable: true,
    }]);
    let fields: &[StructField] = schema.fields();
    let DataType::Struct(children) = &fields[0].data_type else {
        panic!("legacy struct type changed");
    };
    assert_eq!(children[0].name, "id");
    assert_eq!(
        schema.to_json().unwrap(),
        r#"{"fields":[{"name":"payload","data_type":{"Struct":[{"name":"id","data_type":"Long","nullable":false}]},"nullable":true}]}"#
    );
}

#[test]
fn additive_descriptors_preserve_nested_metadata_and_the_existing_wire_format() {
    let schema = StructType::from_logical_fields(vec![
        LogicalField::new(
            "payload".into(),
            DataType::LogicalStruct(vec![
                LogicalField::new("id".into(), DataType::Long, false)
                    .with_metadata([("origin".into(), serde_json::json!("nested"))].into()),
            ]),
            true,
        )
        .with_metadata([("owner".into(), serde_json::json!("catalog"))].into()),
    ]);
    let wire = serde_json::to_value(&schema).unwrap();
    assert_eq!(
        wire["fields"][0]["data_type"]["Struct"][0]["metadata"]["origin"],
        "nested"
    );
    let decoded: StructType = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), wire);
    assert_eq!(decoded.logical_fields()[0].metadata["owner"], "catalog");
    let DataType::LogicalStruct(children) = &decoded.logical_fields()[0].data_type else {
        panic!("logical struct type lost");
    };
    assert_eq!(children[0].metadata["origin"], "nested");
    assert!(matches!(decoded.fields()[0].data_type, DataType::Struct(_)));
}
