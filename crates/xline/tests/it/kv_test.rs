use std::{error::Error, time::Duration};

use test_macros::abort_on_panic;
use xline_test_utils::{
    types::kv::{
        Compare, CompareResult, DeleteRangeOptions, PutOptions, RangeOptions, Response, SortOrder,
        SortTarget, TxnOp, TxnRequest,
    },
    Client, ClientOptions, Cluster,
};

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_kv_put() -> Result<(), Box<dyn Error>> {
    struct TestCase {
        key: &'static str,
        value: &'static str,
        option: Option<PutOptions>,
        want_err: bool,
    }

    let tests = [
        TestCase {
            key: "foo",
            value: "",
            option: Some(PutOptions::default().with_ignore_value(true)),
            want_err: true,
        },
        TestCase {
            key: "foo",
            value: "bar",
            option: None,
            want_err: false,
        },
        TestCase {
            key: "foo",
            value: "",
            option: Some(PutOptions::default().with_ignore_value(true)),
            want_err: false,
        },
        TestCase {
            key: "foo",
            value: "",
            option: Some(PutOptions::default().with_lease(12345)),
            want_err: true,
        },
    ];

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await.kv_client();

    for test in tests {
        let res = client.put(test.key, test.value, test.option).await;
        assert_eq!(res.is_err(), test.want_err);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_kv_get() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        key: Vec<u8>,
        opt: Option<RangeOptions>,
        want_kvs: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await.kv_client();

    let kvs = ["a", "b", "c", "c", "c", "foo", "foo/abc", "fop"];
    let want_kvs = ["a", "b", "c", "foo", "foo/abc", "fop"];
    let kvs_by_version = ["a", "b", "foo", "foo/abc", "fop", "c"];
    let reversed_kvs = ["fop", "foo/abc", "foo", "c", "b", "a"];

    let tests = [
        TestCase {
            key: "a".into(),
            opt: None,
            want_kvs: &want_kvs[..1],
        },
        TestCase {
            key: "a".into(),
            opt: Some(RangeOptions::default().with_serializable(true)),
            want_kvs: &want_kvs[..1],
        },
        TestCase {
            key: "a".into(),
            opt: Some(RangeOptions::default().with_range_end("c")),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            key: "".into(),
            opt: Some(RangeOptions::default().with_prefix()),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "".into(),
            opt: Some(RangeOptions::default().with_from_key()),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "a".into(),
            opt: Some(RangeOptions::default().with_range_end("x")),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "".into(),
            opt: Some(RangeOptions::default().with_prefix().with_revision(4)),
            want_kvs: &want_kvs[..3],
        },
        TestCase {
            key: "a".into(),
            opt: Some(RangeOptions::default().with_count_only(true)),
            want_kvs: &[],
        },
        TestCase {
            key: "foo".into(),
            opt: Some(RangeOptions::default().with_prefix()),
            want_kvs: &["foo", "foo/abc"],
        },
        TestCase {
            key: "foo".into(),
            opt: Some(RangeOptions::default().with_from_key()),
            want_kvs: &["foo", "foo/abc", "fop"],
        },
        TestCase {
            key: "".into(),
            opt: Some(RangeOptions::default().with_prefix().with_limit(2)),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            key: "".into(),
            opt: Some(
                RangeOptions::default()
                    .with_prefix()
                    .with_sort_order(SortOrder::Descend)
                    .with_sort_order(SortOrder::Ascend),
            ),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "".into(),
            opt: Some(
                RangeOptions::default()
                    .with_prefix()
                    .with_sort_target(SortTarget::Version)
                    .with_sort_order(SortOrder::Ascend),
            ),

            want_kvs: &kvs_by_version[..],
        },
        TestCase {
            key: "".into(),
            opt: Some(
                RangeOptions::default()
                    .with_prefix()
                    .with_sort_target(SortTarget::Create)
                    .with_sort_order(SortOrder::None),
            ),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "".into(),
            opt: Some(
                RangeOptions::default()
                    .with_prefix()
                    .with_sort_target(SortTarget::Create)
                    .with_sort_order(SortOrder::Descend),
            ),
            want_kvs: &reversed_kvs[..],
        },
        TestCase {
            key: "".into(),
            opt: Some(
                RangeOptions::default()
                    .with_prefix()
                    .with_sort_target(SortTarget::Key)
                    .with_sort_order(SortOrder::Descend),
            ),
            want_kvs: &reversed_kvs[..],
        },
    ];

    for key in kvs {
        client.put(key, "bar", None).await?;
    }

    for test in tests {
        let res = client.range(test.key, test.opt).await?;
        assert_eq!(res.kvs.len(), test.want_kvs.len());
        let is_identical = res
            .kvs
            .iter()
            .zip(test.want_kvs.iter())
            .all(|(kv, want)| kv.key == want.as_bytes());
        assert!(is_identical);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_range_redirect() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;

    let addr = cluster.get_client_url(1);
    let kv_client = Client::connect([addr], ClientOptions::default())
        .await?
        .kv_client();
    let _ignore = kv_client.put("foo", "bar", None).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let res = kv_client.range("foo", None).await?;
    assert_eq!(res.kvs.len(), 1);
    assert_eq!(res.kvs[0].value, b"bar");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_kv_delete() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        key: Vec<u8>,
        opt: Option<DeleteRangeOptions>,
        want_deleted: i64,
        want_keys: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await.kv_client();

    let keys = ["a", "b", "c", "c/abc", "d"];

    let tests = [
        TestCase {
            key: "".into(),
            opt: Some(DeleteRangeOptions::default().with_prefix()),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            key: "".into(),
            opt: Some(DeleteRangeOptions::default().with_from_key()),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            key: "a".into(),
            opt: Some(DeleteRangeOptions::default().with_range_end("c")),
            want_deleted: 2,
            want_keys: &["c", "c/abc", "d"],
        },
        TestCase {
            key: "c".into(),
            opt: None,
            want_deleted: 1,
            want_keys: &["a", "b", "c/abc", "d"],
        },
        TestCase {
            key: "c".into(),
            opt: Some(DeleteRangeOptions::default().with_prefix()),
            want_deleted: 2,
            want_keys: &["a", "b", "d"],
        },
        TestCase {
            key: "c".into(),
            opt: Some(DeleteRangeOptions::default().with_from_key()),
            want_deleted: 3,
            want_keys: &["a", "b"],
        },
        TestCase {
            key: "e".into(),
            opt: None,
            want_deleted: 0,
            want_keys: &keys,
        },
    ];

    for test in tests {
        for key in keys {
            client.put(key, "bar", None).await?;
        }

        let res = client.delete(test.key, test.opt).await?;
        assert_eq!(res.deleted, test.want_deleted);

        let res = client
            .range("", Some(RangeOptions::default().with_prefix()))
            .await?;
        let is_identical = res
            .kvs
            .iter()
            .zip(test.want_keys.iter())
            .all(|(kv, want)| kv.key == want.as_bytes());
        assert!(is_identical);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_txn() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await.kv_client();

    let kvs = ["a", "b", "c", "d", "e"];
    for key in kvs {
        client.put(key, "bar", None).await?;
    }

    let read_write_txn_req = TxnRequest::new()
        .when(&[Compare::value("b", CompareResult::Equal, "bar")][..])
        .and_then(&[TxnOp::put("f", "foo", None)][..])
        .or_else(&[TxnOp::range("a", None)][..]);

    let res = client.txn(read_write_txn_req).await?;
    assert!(res.succeeded);
    assert_eq!(res.responses.len(), 1);
    assert!(matches!(
        res.responses[0].response,
        Some(Response::ResponsePut(_))
    ));

    let read_only_txn = TxnRequest::new()
        .when(&[Compare::version("b", CompareResult::Greater, 10)][..])
        .and_then(&[TxnOp::range("a", None)][..])
        .or_else(&[TxnOp::range("b", None)][..]);
    let mut res = client.txn(read_only_txn).await?;
    assert!(!res.succeeded);
    assert_eq!(res.responses.len(), 1);

    if let Some(resp_op) = res.responses.pop() {
        if let Response::ResponseRange(get_res) = resp_op
            .response
            .expect("the inner response should not be None")
        {
            assert_eq!(get_res.kvs.len(), 1);
            assert_eq!(get_res.kvs[0].key, b"b");
            assert_eq!(get_res.kvs[0].value, b"bar");
        } else {
            unreachable!("receive unexpected op response in a read-only transaction");
        }
    } else {
        unreachable!("response in a read_only_txn should not be None");
    };

    let serializable_txn = TxnRequest::new()
        .when([])
        .and_then(
            &[TxnOp::range(
                "c",
                Some(RangeOptions::default().with_serializable(true)),
            )][..],
        )
        .or_else(
            &[TxnOp::range(
                "d",
                Some(RangeOptions::default().with_serializable(true)),
            )][..],
        );
    let mut res = client.txn(serializable_txn).await?;
    assert!(res.succeeded);
    assert_eq!(res.responses.len(), 1);

    if let Some(resp_op) = res.responses.pop() {
        if let Response::ResponseRange(get_res) = resp_op
            .response
            .expect("the inner response should not be None")
        {
            assert_eq!(get_res.kvs.len(), 1);
            assert_eq!(get_res.kvs[0].key, b"c");
            assert_eq!(get_res.kvs[0].value, b"bar");
        } else {
            unreachable!("receive unexpected op response in a read-only transaction");
        }
    } else {
        unreachable!("response in a read_only_txn should not be None");
    };

    Ok(())
}
