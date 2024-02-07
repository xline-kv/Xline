use std::{error::Error, time::Duration};

use test_macros::abort_on_panic;
use xline_test_utils::{
    types::kv::{
        Compare, CompareResult, DeleteRangeRequest, PutRequest, RangeRequest, Response, SortOrder,
        SortTarget, TxnOp, TxnRequest,
    },
    Client, ClientOptions, Cluster,
};

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_kv_put() -> Result<(), Box<dyn Error>> {
    struct TestCase {
        req: PutRequest,
        want_err: bool,
    }

    let tests = [
        TestCase {
            req: PutRequest::new("foo", "").with_ignore_value(true),
            want_err: true,
        },
        TestCase {
            req: PutRequest::new("foo", "bar"),
            want_err: false,
        },
        TestCase {
            req: PutRequest::new("foo", "").with_ignore_value(true),
            want_err: false,
        },
        TestCase {
            req: PutRequest::new("foo", "").with_lease(12345),
            want_err: true,
        },
    ];

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await.kv_client();

    for test in tests {
        let res = client.put(test.req).await;
        assert_eq!(res.is_err(), test.want_err);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_kv_get() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        req: RangeRequest,
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
            req: RangeRequest::new("a"),
            want_kvs: &want_kvs[..1],
        },
        TestCase {
            req: RangeRequest::new("a").with_serializable(true),
            want_kvs: &want_kvs[..1],
        },
        TestCase {
            req: RangeRequest::new("a").with_range_end("c"),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            req: RangeRequest::new("").with_prefix(),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("").with_from_key(),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("a").with_range_end("x"),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("").with_prefix().with_revision(4),
            want_kvs: &want_kvs[..3],
        },
        TestCase {
            req: RangeRequest::new("a").with_count_only(true),
            want_kvs: &[],
        },
        TestCase {
            req: RangeRequest::new("foo").with_prefix(),
            want_kvs: &["foo", "foo/abc"],
        },
        TestCase {
            req: RangeRequest::new("foo").with_from_key(),
            want_kvs: &["foo", "foo/abc", "fop"],
        },
        TestCase {
            req: RangeRequest::new("").with_prefix().with_limit(2),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Mod)
                .with_sort_order(SortOrder::Ascend),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Version)
                .with_sort_order(SortOrder::Ascend),
            want_kvs: &kvs_by_version[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Create)
                .with_sort_order(SortOrder::None),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Create)
                .with_sort_order(SortOrder::Descend),
            want_kvs: &reversed_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Key)
                .with_sort_order(SortOrder::Descend),
            want_kvs: &reversed_kvs[..],
        },
    ];

    for key in kvs {
        client.put(PutRequest::new(key, "bar")).await?;
    }

    for test in tests {
        let res = client.range(test.req).await?;
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
    let _ignore = kv_client.put(PutRequest::new("foo", "bar")).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let res = kv_client.range(RangeRequest::new("foo")).await?;
    assert_eq!(res.kvs.len(), 1);
    assert_eq!(res.kvs[0].value, b"bar");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_kv_delete() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        req: DeleteRangeRequest,
        want_deleted: i64,
        want_keys: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await.kv_client();

    let keys = ["a", "b", "c", "c/abc", "d"];

    let tests = [
        TestCase {
            req: DeleteRangeRequest::new("").with_prefix(),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            req: DeleteRangeRequest::new("").with_from_key(),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            req: DeleteRangeRequest::new("a").with_range_end("c"),
            want_deleted: 2,
            want_keys: &["c", "c/abc", "d"],
        },
        TestCase {
            req: DeleteRangeRequest::new("c"),
            want_deleted: 1,
            want_keys: &["a", "b", "c/abc", "d"],
        },
        TestCase {
            req: DeleteRangeRequest::new("c").with_prefix(),
            want_deleted: 2,
            want_keys: &["a", "b", "d"],
        },
        TestCase {
            req: DeleteRangeRequest::new("c").with_from_key(),
            want_deleted: 3,
            want_keys: &["a", "b"],
        },
        TestCase {
            req: DeleteRangeRequest::new("e"),
            want_deleted: 0,
            want_keys: &keys,
        },
    ];

    for test in tests {
        for key in keys {
            client.put(PutRequest::new(key, "bar")).await?;
        }

        let res = client.delete(test.req).await?;
        assert_eq!(res.deleted, test.want_deleted);

        let res = client.range(RangeRequest::new("").with_prefix()).await?;
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
        client.put(PutRequest::new(key, "bar")).await?;
    }

    let read_write_txn_req = TxnRequest::new()
        .when(&[Compare::value("b", CompareResult::Equal, "bar")][..])
        .and_then(&[TxnOp::put(PutRequest::new("f", "foo"))][..])
        .or_else(&[TxnOp::range(RangeRequest::new("a"))][..]);

    let res = client.txn(read_write_txn_req).await?;
    assert!(res.succeeded);
    assert_eq!(res.responses.len(), 1);
    assert!(matches!(
        res.responses[0].response,
        Some(Response::ResponsePut(_))
    ));

    let read_only_txn = TxnRequest::new()
        .when(&[Compare::version("b", CompareResult::Greater, 10)][..])
        .and_then(&[TxnOp::range(RangeRequest::new("a"))][..])
        .or_else(&[TxnOp::range(RangeRequest::new("b"))][..]);
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
        .and_then(&[TxnOp::range(RangeRequest::new("c").with_serializable(true))][..])
        .or_else(&[TxnOp::range(RangeRequest::new("d").with_serializable(true))][..]);
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
