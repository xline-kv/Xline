//! The following tests are originally from `etcd-client`

use test_macros::abort_on_panic;
use xline_client::{
    error::Result,
    types::kv::{
        Compare, CompareResult, DeleteRangeOptions, PutOptions, RangeOptions, TxnOp, TxnRequest,
    },
};

use super::common::get_cluster_client;

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn put_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.kv_client();

    client.put("put", "123", None).await?;

    // overwrite with prev key
    {
        let resp = client
            .put("put", "456", Some(PutOptions::default().with_prev_kv(true)))
            .await?;
        let prev_kv = resp.prev_kv;
        assert!(prev_kv.is_some());
        let prev_kv = prev_kv.unwrap();
        assert_eq!(prev_kv.key, b"put");
        assert_eq!(prev_kv.value, b"123");
    }

    // overwrite again with prev key
    {
        let resp = client
            .put("put", "456", Some(PutOptions::default().with_prev_kv(true)))
            .await?;
        let prev_kv = resp.prev_kv;
        assert!(prev_kv.is_some());
        let prev_kv = prev_kv.unwrap();
        assert_eq!(prev_kv.key, b"put");
        assert_eq!(prev_kv.value, b"456");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn range_should_fetches_previously_put_keys() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.kv_client();

    client.put("get10", "10", None).await?;
    client.put("get11", "11", None).await?;
    client.put("get20", "20", None).await?;
    client.put("get21", "21", None).await?;

    // get key
    {
        let resp = client.range("get11", None).await?;
        assert_eq!(resp.count, 1);
        assert!(!resp.more);
        assert_eq!(resp.kvs.len(), 1);
        assert_eq!(resp.kvs[0].key, b"get11");
        assert_eq!(resp.kvs[0].value, b"11");
    }

    // get from key
    {
        let resp = client
            .range(
                "get11",
                Some(RangeOptions::default().with_from_key().with_limit(2)),
            )
            .await?;
        assert!(resp.more);
        assert_eq!(resp.kvs.len(), 2);
        assert_eq!(resp.kvs[0].key, b"get11");
        assert_eq!(resp.kvs[0].value, b"11");
        assert_eq!(resp.kvs[1].key, b"get20");
        assert_eq!(resp.kvs[1].value, b"20");
    }

    // get prefix keys
    {
        let resp = client
            .range("get1", Some(RangeOptions::default().with_prefix()))
            .await?;
        assert_eq!(resp.count, 2);
        assert!(!resp.more);
        assert_eq!(resp.kvs.len(), 2);
        assert_eq!(resp.kvs[0].key, b"get10");
        assert_eq!(resp.kvs[0].value, b"10");
        assert_eq!(resp.kvs[1].key, b"get11");
        assert_eq!(resp.kvs[1].value, b"11");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn delete_should_remove_previously_put_kvs() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.kv_client();

    client.put("del10", "10", None).await?;
    client.put("del11", "11", None).await?;
    client.put("del20", "20", None).await?;
    client.put("del21", "21", None).await?;
    client.put("del31", "31", None).await?;
    client.put("del32", "32", None).await?;

    // delete key
    {
        let resp = client
            .delete(
                "del11",
                Some(DeleteRangeOptions::default().with_prev_kv(true)),
            )
            .await?;
        assert_eq!(resp.deleted, 1);
        assert_eq!(&resp.prev_kvs[0].key, "del11".as_bytes());
        assert_eq!(&resp.prev_kvs[0].value, "11".as_bytes());
        let resp = client
            .range("del11", Some(RangeOptions::default().with_count_only(true)))
            .await?;
        assert_eq!(resp.count, 0);
    }

    // delete a range of keys
    {
        let resp = client
            .delete(
                "del11",
                Some(
                    DeleteRangeOptions::default()
                        .with_range_end("del22")
                        .with_prev_kv(true),
                ),
            )
            .await?;
        assert_eq!(resp.deleted, 2);
        assert_eq!(&resp.prev_kvs[0].key, "del20".as_bytes());
        assert_eq!(&resp.prev_kvs[0].value, "20".as_bytes());
        assert_eq!(&resp.prev_kvs[1].key, "del21".as_bytes());
        assert_eq!(&resp.prev_kvs[1].value, "21".as_bytes());
        let resp = client
            .range(
                "del11",
                Some(
                    RangeOptions::default()
                        .with_range_end("del22")
                        .with_count_only(true),
                ),
            )
            .await?;
        assert_eq!(resp.count, 0);
    }

    // delete key with prefix
    {
        let resp = client
            .delete(
                "del3",
                Some(
                    DeleteRangeOptions::default()
                        .with_prefix()
                        .with_prev_kv(true),
                ),
            )
            .await?;
        assert_eq!(resp.deleted, 2);
        assert_eq!(&resp.prev_kvs[0].key, "del31".as_bytes());
        assert_eq!(&resp.prev_kvs[0].value, "31".as_bytes());
        assert_eq!(&resp.prev_kvs[1].key, "del32".as_bytes());
        assert_eq!(&resp.prev_kvs[1].value, "32".as_bytes());
        let resp = client
            .range("del32", Some(RangeOptions::default().with_count_only(true)))
            .await?;
        assert_eq!(resp.count, 0);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn txn_should_execute_as_expected() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.kv_client();

    client.put("txn01", "01", None).await?;

    // transaction 1
    {
        let resp = client
            .txn(
                TxnRequest::new()
                    .when(&[Compare::value("txn01", CompareResult::Equal, "01")][..])
                    .and_then(
                        &[TxnOp::put(
                            "txn01",
                            "02",
                            Some(PutOptions::default().with_prev_kv(true)),
                        )][..],
                    )
                    .or_else(&[TxnOp::range("txn01", None)][..]),
            )
            .await?;

        assert!(resp.succeeded);
        let op_responses = resp.responses;
        assert_eq!(op_responses.len(), 1);

        match op_responses[0].response.as_ref().unwrap() {
            xlineapi::Response::ResponsePut(resp) => {
                assert_eq!(resp.prev_kv.as_ref().unwrap().value, b"01")
            }
            _ => panic!("expect put response)"),
        }

        let resp = client.range("txn01", None).await?;
        assert_eq!(resp.kvs[0].key, b"txn01");
        assert_eq!(resp.kvs[0].value, b"02");
    }

    // transaction 2
    {
        let resp = client
            .txn(
                TxnRequest::new()
                    .when(&[Compare::value("txn01", CompareResult::Equal, "01")][..])
                    .and_then(&[TxnOp::put("txn01", "02", None)][..])
                    .or_else(&[TxnOp::range("txn01", None)][..]),
            )
            .await?;

        assert!(!resp.succeeded);
        let op_responses = resp.responses;
        assert_eq!(op_responses.len(), 1);

        match op_responses[0].response.as_ref().unwrap() {
            xlineapi::Response::ResponseRange(resp) => {
                assert_eq!(resp.kvs[0].value, b"02")
            }
            _ => panic!("expect range response)"),
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn compact_should_remove_previous_revision() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.kv_client();

    client.put("compact", "0", None).await?;
    client.put("compact", "1", None).await?;

    // before compacting
    let rev0_resp = client
        .range("compact", Some(RangeOptions::default().with_revision(2)))
        .await?;
    assert_eq!(rev0_resp.kvs[0].value, b"0");
    let rev1_resp = client
        .range("compact", Some(RangeOptions::default().with_revision(3)))
        .await?;
    assert_eq!(rev1_resp.kvs[0].value, b"1");

    client.compact(3, false).await?;

    // after compacting
    let rev0_resp = client
        .range("compact", Some(RangeOptions::default().with_revision(2)))
        .await;
    assert!(
        rev0_resp.is_err(),
        "client.range should receive an err after compaction, but it receives: {rev0_resp:?}"
    );
    let rev1_resp = client
        .range("compact", Some(RangeOptions::default().with_revision(3)))
        .await?;
    assert_eq!(rev1_resp.kvs[0].value, b"1");

    Ok(())
}
