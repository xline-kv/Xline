mod common;

use std::error::Error;

use etcd_client::{DeleteOptions, GetOptions, SortOrder, SortTarget};

use crate::common::Cluster;

#[tokio::test]
async fn test_kv_put() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client(0).await;

    client.put("foo", "bar", None).await?;
    let res = client.get("foo", None).await?;
    assert_eq!(res.kvs().len(), 1);
    assert_eq!(res.kvs()[0].key(), b"foo");
    assert_eq!(res.kvs()[0].value(), b"bar");

    Ok(())
}

#[tokio::test]
async fn test_kv_get() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        key: &'a str,
        opts: Option<GetOptions>,
        want_kvs: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client(0).await;

    let kvs = ["a", "b", "c", "c", "c", "foo", "foo/abc", "fop"];
    let want_kvs = ["a", "b", "c", "foo", "foo/abc", "fop"];
    let kvs_by_version = ["a", "b", "foo", "foo/abc", "fop", "c"];
    let reversed_kvs = ["fop", "foo/abc", "foo", "c", "b", "a"];

    let tests = [
        TestCase {
            key: "a",
            opts: None,
            want_kvs: &want_kvs[..1],
        },
        // TestCase {
        //     key: "a",
        //     opts: Some(GetOptions::new().with_serializable()),
        //     want_kvs: &want_kvs[..1],
        // },
        TestCase {
            key: "a",
            opts: Some(GetOptions::new().with_range("c")),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            key: "",
            opts: Some(GetOptions::new().with_prefix()),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "",
            opts: Some(GetOptions::new().with_from_key()),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "a",
            opts: Some(GetOptions::new().with_range("x")),
            want_kvs: &want_kvs[..],
        },
        // TODO: Range with revision
        // TestCase {
        //     key: "",
        //     opts: Some(GetOptions::new().with_prefix().with_revision(4)),
        //     want_kvs: &want_kvs[..3],
        // },
        TestCase {
            key: "a",
            opts: Some(GetOptions::new().with_count_only()),
            want_kvs: &[],
        },
        TestCase {
            key: "foo",
            opts: Some(GetOptions::new().with_prefix()),
            want_kvs: &["foo", "foo/abc"],
        },
        TestCase {
            key: "foo",
            opts: Some(GetOptions::new().with_from_key()),
            want_kvs: &["foo", "foo/abc", "fop"],
        },
        TestCase {
            key: "",
            opts: Some(GetOptions::new().with_prefix().with_limit(2)),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            key: "",
            opts: Some(
                GetOptions::new()
                    .with_prefix()
                    .with_sort(SortTarget::Mod, SortOrder::Ascend),
            ),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "",
            opts: Some(
                GetOptions::new()
                    .with_prefix()
                    .with_sort(SortTarget::Version, SortOrder::Ascend),
            ),
            want_kvs: &kvs_by_version[..],
        },
        TestCase {
            key: "",
            opts: Some(
                GetOptions::new()
                    .with_prefix()
                    .with_sort(SortTarget::Create, SortOrder::None),
            ),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "",
            opts: Some(
                GetOptions::new()
                    .with_prefix()
                    .with_sort(SortTarget::Create, SortOrder::Descend),
            ),

            want_kvs: &reversed_kvs[..],
        },
        TestCase {
            key: "",
            opts: Some(
                GetOptions::new()
                    .with_prefix()
                    .with_sort(SortTarget::Key, SortOrder::Descend),
            ),
            want_kvs: &reversed_kvs[..],
        },
    ];

    for key in kvs {
        client.put(key, "bar", None).await?;
    }

    for test in tests {
        let res = client.get(test.key, test.opts).await?;
        assert_eq!(res.kvs().len(), test.want_kvs.len());
        let is_identical = res
            .kvs()
            .iter()
            .zip(test.want_kvs.iter())
            .all(|(kv, want)| kv.key() == want.as_bytes());
        assert!(is_identical);
    }

    Ok(())
}

#[tokio::test]
async fn test_kv_delete() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        key: &'a str,
        opts: Option<DeleteOptions>,
        want_deleted: i64,
        want_keys: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client(0).await;

    let keys = ["a", "b", "c", "c/abc", "d"];

    let tests = [
        TestCase {
            key: "",
            opts: Some(DeleteOptions::new().with_prefix()),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            key: "",
            opts: Some(DeleteOptions::new().with_from_key()),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            key: "a",
            opts: Some(DeleteOptions::new().with_range("c")),
            want_deleted: 2,
            want_keys: &["c", "c/abc", "d"],
        },
        TestCase {
            key: "c",
            opts: None,
            want_deleted: 1,
            want_keys: &["a", "b", "c/abc", "d"],
        },
        TestCase {
            key: "c",
            opts: Some(DeleteOptions::new().with_prefix()),
            want_deleted: 2,
            want_keys: &["a", "b", "d"],
        },
        TestCase {
            key: "c",
            opts: Some(DeleteOptions::new().with_from_key()),
            want_deleted: 3,
            want_keys: &["a", "b"],
        },
        TestCase {
            key: "e",
            opts: None,
            want_deleted: 0,
            want_keys: &keys,
        },
    ];

    for test in tests {
        for key in keys {
            client.put(key, "bar", None).await?;
        }

        let res = client.delete(test.key, test.opts).await?;
        assert_eq!(res.deleted(), test.want_deleted);

        let res = client
            .get("", Some(GetOptions::new().with_all_keys()))
            .await?;
        let is_identical = res
            .kvs()
            .iter()
            .zip(test.want_keys.iter())
            .all(|(kv, want)| kv.key() == want.as_bytes());
        assert!(is_identical);
    }

    Ok(())
}
