fn main() {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Deserialize, serde::Serialize)]")
        .compile(
            &[
                "proto/src/kv.proto",
                "proto/src/rpc.proto",
                "proto/src/auth.proto",
                "proto/src/v3lock.proto",
                "proto/src/lease.proto",
                "proto/src/xline-command.proto",
                "proto/src/xline-error.proto",
            ],
            &["./proto/src"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile proto, error is {:?}", e));
}
