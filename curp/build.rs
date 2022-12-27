fn main() {
    tonic_build::configure()
        .compile(&["./proto/message.proto"], &["./proto/"])
        .unwrap_or_else(|e| panic!("Failed to compile proto, error is {:?}", e));
}
