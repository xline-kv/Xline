use anyhow::Result;
use xline_client::{
    types::kv::{Compare, CompareResult, DeleteRangeOptions, PutOptions, TxnOp, TxnRequest},
    Client, ClientOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let client = Client::connect(curp_members, ClientOptions::default())
        .await?
        .kv_client();

    // put
    client.put("key1", "value1", None).await?;
    client.put("key2", "value2", None).await?;

    // range
    let resp = client.range("key1", None).await?;

    if let Some(kv) = resp.kvs.first() {
        println!(
            "got key: {}, value: {}",
            String::from_utf8_lossy(&kv.key),
            String::from_utf8_lossy(&kv.value)
        );
    }

    // delete
    let resp = client
        .delete(
            "key1",
            Some(DeleteRangeOptions::default().with_prev_kv(true)),
        )
        .await?;

    for kv in resp.prev_kvs {
        println!(
            "deleted key: {}, value: {}",
            String::from_utf8_lossy(&kv.key),
            String::from_utf8_lossy(&kv.value)
        );
    }

    // txn
    let txn_req = TxnRequest::new()
        .when(&[Compare::value("key2", CompareResult::Equal, "value2")][..])
        .and_then(
            &[TxnOp::put(
                "key2",
                "value3",
                Some(PutOptions::default().with_prev_kv(true)),
            )][..],
        )
        .or_else(&[TxnOp::range("key2", None)][..]);

    let _resp = client.txn(txn_req).await?;
    let resp = client.range("key2", None).await?;
    // should print "value3"
    if let Some(kv) = resp.kvs.first() {
        println!(
            "got key: {}, value: {}",
            String::from_utf8_lossy(&kv.key),
            String::from_utf8_lossy(&kv.value)
        );
    }

    // compact
    let rev = resp.header.unwrap().revision;
    let _resp = client.compact(rev, false).await?;

    Ok(())
}
