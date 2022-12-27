fn main() {
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["./proto/message.proto"], &["./proto/"])
        .unwrap_or_else(|e| panic!("Failed to compile proto, error is {:?}", e));
}
