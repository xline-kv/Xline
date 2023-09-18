fn main() {
    let mut prost_config = prost_build::Config::new();
    prost_config.bytes([".messagepb.InstallSnapshotRequest"]);
    tonic_build::configure()
        .compile_with_config(
            prost_config,
            &[
                "./proto/src/message.proto",
                "./proto/src/error.proto",
                "./proto/src/command.proto",
            ],
            &["./proto/src/"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile proto, error is {:?}", e));
}
