fn main() {
    let mut prost_config = prost_build::Config::new();
    prost_config.bytes([".messagepb.InstallSnapshotRequest"]);
    tonic_build::configure()
        .type_attribute(
            ".message",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .compile_with_config(
            prost_config,
            &["./proto/message.proto", "./proto/error.proto"],
            &["./proto/"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile proto, error is {:?}", e));
}
