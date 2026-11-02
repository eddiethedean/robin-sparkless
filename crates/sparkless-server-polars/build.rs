fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_CONNECT");
    #[cfg(feature = "connect")]
    generate_connect_bindings()?;
    Ok(())
}

#[cfg(feature = "connect")]
fn generate_connect_bindings() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "proto";
    let protoc_include = protoc_bin_vendored::include_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    let protos = [
        "base.proto",
        "catalog.proto",
        "commands.proto",
        "common.proto",
        "example_plugins.proto",
        "expressions.proto",
        "ml.proto",
        "ml_common.proto",
        "relations.proto",
        "types.proto",
    ]
    .map(|name| format!("{proto_root}/spark/connect/{name}"));

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .codec_path("crate::connect::BoundedProstCodec")
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?)
                .join("spark_connect_descriptor.bin"),
        )
        .compile_protos(
            &protos,
            &[
                proto_root.to_string(),
                protoc_include
                    .to_str()
                    .ok_or("invalid protoc include path")?
                    .to_string(),
            ],
        )?;

    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    Ok(())
}
