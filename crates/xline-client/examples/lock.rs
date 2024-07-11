use anyhow::Result;
use xline_client::{
    clients::Xutex,
    types::kv::{Compare, CompareResult, PutOptions, TxnOp},
    Client, ClientOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let client = Client::connect(curp_members, ClientOptions::default()).await?;

    let lock_client = client.lock_client();
    let kv_client = client.kv_client();

    let mut xutex = Xutex::new(lock_client, "lock-test", None, None).await?;
    // when the `xutex_guard` drop, the lock will be unlocked.
    let xutex_guard = xutex.lock_unsafe().await?;

    let txn_req = xutex_guard
        .txn_check_locked_key()
        .when([Compare::value("key2", CompareResult::Equal, "value2")])
        .and_then([TxnOp::put(
            "key2",
            "value3",
            Some(PutOptions::default().with_prev_kv(true)),
        )])
        .or_else(&[]);

    let _resp = kv_client.txn(txn_req).await?;

    Ok(())
}
