//! The following tests are originally from `etcd-client`
use test_macros::abort_on_panic;
use xline_client::{
    error::Result,
    types::kv::{
        CompactionRequest, Compare, CompareResult, DeleteRangeRequest, PutRequest, RangeRequest,
        TxnOp, TxnRequest,
    },
};

use super::common::get_cluster_client;

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn put_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.kv_client();

    let request = PutRequest::new("put", "123");
    client.put(request).await?;

    // overwrite with prev key
    {
        let request = PutRequest::new("put", "456").with_prev_kv(true);
        let resp = client.put(request).await?;
        let prev_kv = resp.prev_kv;
        assert!(prev_kv.is_some());
        let prev_kv = prev_kv.unwrap();
        assert_eq!(prev_kv.key, b"put");
        assert_eq!(prev_kv.value, b"123");
    }

    // overwrite again with prev key
    {
        let request = PutRequest::new("put", "456").with_prev_kv(true);
        let resp = client.put(request).await?;
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

    client.put(PutRequest::new("get10", "10")).await?;
    client.put(PutRequest::new("get11", "11")).await?;
    client.put(PutRequest::new("get20", "20")).await?;
    client.put(PutRequest::new("get21", "21")).await?;

    // get key
    {
        let resp = client.range(RangeRequest::new("get11")).await?;
        assert_eq!(resp.count, 1);
        assert!(!resp.more);
        assert_eq!(resp.kvs.len(), 1);
        assert_eq!(resp.kvs[0].key, b"get11");
        assert_eq!(resp.kvs[0].value, b"11");
    }

    // get from key
    {
        let resp = client
            .range(RangeRequest::new("get11").with_from_key().with_limit(2))
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
            .range(RangeRequest::new("get1").with_prefix())
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

    client.put(PutRequest::new("del10", "10")).await?;
    client.put(PutRequest::new("del11", "11")).await?;
    client.put(PutRequest::new("del20", "20")).await?;
    client.put(PutRequest::new("del21", "21")).await?;
    client.put(PutRequest::new("del31", "31")).await?;
    client.put(PutRequest::new("del32", "32")).await?;

    // delete key
    {
        let resp = client
            .delete(DeleteRangeRequest::new("del11").with_prev_kv(true))
            .await?;
        assert_eq!(resp.deleted, 1);
        assert_eq!(&resp.prev_kvs[0].key, "del11".as_bytes());
        assert_eq!(&resp.prev_kvs[0].value, "11".as_bytes());
        let resp = client
            .range(RangeRequest::new("del11").with_count_only(true))
            .await?;
        assert_eq!(resp.count, 0);
    }

    // delete a range of keys
    {
        let resp = client
            .delete(
                DeleteRangeRequest::new("del11")
                    .with_range_end("del22")
                    .with_prev_kv(true),
            )
            .await?;
        assert_eq!(resp.deleted, 2);
        assert_eq!(&resp.prev_kvs[0].key, "del20".as_bytes());
        assert_eq!(&resp.prev_kvs[0].value, "20".as_bytes());
        assert_eq!(&resp.prev_kvs[1].key, "del21".as_bytes());
        assert_eq!(&resp.prev_kvs[1].value, "21".as_bytes());
        let resp = client
            .range(
                RangeRequest::new("del11")
                    .with_range_end("del22")
                    .with_count_only(true),
            )
            .await?;
        assert_eq!(resp.count, 0);
    }

    // delete key with prefix
    {
        let resp = client
            .delete(
                DeleteRangeRequest::new("del3")
                    .with_prefix()
                    .with_prev_kv(true),
            )
            .await?;
        assert_eq!(resp.deleted, 2);
        assert_eq!(&resp.prev_kvs[0].key, "del31".as_bytes());
        assert_eq!(&resp.prev_kvs[0].value, "31".as_bytes());
        assert_eq!(&resp.prev_kvs[1].key, "del32".as_bytes());
        assert_eq!(&resp.prev_kvs[1].value, "32".as_bytes());
        let resp = client
            .range(RangeRequest::new("del32").with_count_only(true))
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

    client.put(PutRequest::new("txn01", "01")).await?;

    // transaction 1
    {
        let resp = client
            .txn(
                TxnRequest::new()
                    .when(&[Compare::value("txn01", CompareResult::Equal, "01")][..])
                    .and_then(
                        &[TxnOp::put(
                            PutRequest::new("txn01", "02").with_prev_kv(true),
                        )][..],
                    )
                    .or_else(&[TxnOp::range(RangeRequest::new("txn01"))][..]),
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

        let resp = client.range(RangeRequest::new("txn01")).await?;
        assert_eq!(resp.kvs[0].key, b"txn01");
        assert_eq!(resp.kvs[0].value, b"02");
    }

    // transaction 2
    {
        let resp = client
            .txn(
                TxnRequest::new()
                    .when(&[Compare::value("txn01", CompareResult::Equal, "01")][..])
                    .and_then(&[TxnOp::put(PutRequest::new("txn01", "02"))][..])
                    .or_else(&[TxnOp::range(RangeRequest::new("txn01"))][..]),
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

    client.put(PutRequest::new("compact", "0")).await?;
    client.put(PutRequest::new("compact", "1")).await?;

    // before compacting
    let rev0_resp = client
        .range(RangeRequest::new("compact").with_revision(2))
        .await?;
    assert_eq!(rev0_resp.kvs[0].value, b"0");
    let rev1_resp = client
        .range(RangeRequest::new("compact").with_revision(3))
        .await?;
    assert_eq!(rev1_resp.kvs[0].value, b"1");

    client.compact(CompactionRequest::new(3)).await?;

    // after compacting
    let rev0_resp = client
        .range(RangeRequest::new("compact").with_revision(2))
        .await;
    assert!(
        rev0_resp.is_err(),
        "client.range should receive an err after compaction, but it receives: {rev0_resp:?}"
    );
    let rev1_resp = client
        .range(RangeRequest::new("compact").with_revision(3))
        .await?;
    assert_eq!(rev1_resp.kvs[0].value, b"1");

    Ok(())
}
