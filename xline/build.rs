fn main() {
    tonic_build::configure()
        .compile(
            &[
                "proto/kv.proto",
                "proto/rpc.proto",
                "proto/auth.proto",
                "proto/v3lock.proto",
                "proto/lease.proto",
            ],
            &["proto"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile proto, error is {:?}", e));
}
