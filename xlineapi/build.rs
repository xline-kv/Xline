fn main() {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Deserialize, serde::Serialize)]")
        .compile(
            &[
                "old-proto/kv.proto",
                "old-proto/rpc.proto",
                "old-proto/auth.proto",
                "old-proto/v3lock.proto",
                "old-proto/lease.proto",
                "old-proto/command.proto",
                "old-proto/error.proto",
            ],
            &["old-proto"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile proto, error is {:?}", e));
}
