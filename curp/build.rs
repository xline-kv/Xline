fn main() {
    let mut prost_config = prost_build::Config::new();
    prost_config.bytes([".messagepb.InstallSnapshotRequest"]);
    tonic_build::configure()
        .compile_with_config(prost_config, &["./proto/message.proto"], &["./proto/"])
        .unwrap_or_else(|e| panic!("Failed to compile proto, error is {:?}", e));
}
