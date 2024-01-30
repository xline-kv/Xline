fn main() {
    tonic_build::configure()
        .type_attribute(
            "ProposeConfChangeRequest.ConfChange",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .compile(
            &["./proto/common/src/curp-command.proto"],
            &["./proto/common/src"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile proto, error is {:?}", e));

    let mut prost_config = prost_build::Config::new();
    prost_config.bytes([".inner_messagepb.InstallSnapshotRequest"]);
    tonic_build::configure()
        .compile_with_config(
            prost_config,
            &["./proto/inner_message.proto"],
            &["./proto/"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile proto, error is {:?}", e));
}
