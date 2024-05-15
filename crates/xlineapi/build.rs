fn main() {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Deserialize, serde::Serialize)]")
        .field_attribute(
            "etcdserverpb.RangeRequest.key",
            "#[serde(with = \"serde_bytes\")]",
        )
        .field_attribute(
            "etcdserverpb.RangeRequest.range_end",
            "#[serde(with = \"serde_bytes\")]",
        )
        .field_attribute(
            "etcdserverpb.PutRequest.key",
            "#[serde(with = \"serde_bytes\")]",
        )
        .field_attribute(
            "etcdserverpb.PutRequest.value",
            "#[serde(with = \"serde_bytes\")]",
        )
        .field_attribute(
            "etcdserverpb.DeleteRangeRequest.key",
            "#[serde(with = \"serde_bytes\")]",
        )
        .field_attribute(
            "etcdserverpb.DeleteRangeRequest.range_end",
            "#[serde(with = \"serde_bytes\")]",
        )
        .field_attribute(
            "etcdserverpb.Compare.key",
            "#[serde(with = \"serde_bytes\")]",
        )
        .field_attribute(
            "etcdserverpb.Compare.range_end",
            "#[serde(with = \"serde_bytes\")]",
        )
        .field_attribute(
            "etcdserverpb.Compare.target_union.value",
            "#[serde(with = \"serde_bytes\")]",
        )
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
