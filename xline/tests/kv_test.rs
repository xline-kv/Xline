mod common;

use common::Cluster;
use etcd_client::{DeleteOptions, GetOptions, PutOptions, SortOrder, SortTarget};

#[tokio::test]
async fn test_kv_put() {
    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    let result = client.put("foo", "bar", None).await;
    assert!(result.is_ok());

    let result = client.get("foo", None).await;
    assert!(result.is_ok());

    let res = result.unwrap();
    assert_eq!(res.kvs().len(), 1);
    assert_eq!(res.kvs()[0].key(), b"foo");
    assert_eq!(res.kvs()[0].value(), b"bar");
}

#[tokio::test]
async fn test_kv_get() {
    struct TestCase<'a> {
        key: &'a str,
        opts: Option<GetOptions>,
        want_kvs: &'a [&'a str],
    }
    let mut cluster = Cluster::new(3);
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
        TestCase {
            key: "a",
            opts: Some(GetOptions::new().with_serializable()),
            want_kvs: &want_kvs[..1],
        },
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
        // TestCase { // TODO: Range with revision
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
        let result = client.put(key, "bar", None).await;
        assert!(result.is_ok());
    }

    for test in tests {
        let result = client.get(test.key, test.opts).await;
        assert!(result.is_ok());
        let res = result.unwrap();
        assert_eq!(res.kvs().len(), test.want_kvs.len());
        let is_identical = res
            .kvs()
            .iter()
            .zip(test.want_kvs.iter())
            .all(|(kv, want)| kv.key() == want.as_bytes());
        assert!(is_identical);
    }
}

#[tokio::test]
async fn test_kv_delete() {
    struct TestCase<'a> {
        key: &'a str,
        opts: Option<DeleteOptions>,

        want_deleted: i64,
        want_keys: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3);
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
            let result = client.put(key, "bar", None).await;
            assert!(result.is_ok());
        }

        let result = client.delete(test.key, test.opts).await;
        assert!(result.is_ok());

        let res = result.unwrap();
        assert_eq!(res.deleted(), test.want_deleted);

        let result = client
            .get("", Some(GetOptions::new().with_all_keys()))
            .await;
        assert!(result.is_ok());
        let is_identical = result
            .unwrap()
            .kvs()
            .iter()
            .zip(test.want_keys.iter())
            .all(|(kv, want)| kv.key() == want.as_bytes());

        assert!(is_identical);
    }
}

// TODO: Fix this test when server implements stop method
#[tokio::test]
async fn test_kv_get_no_quorum() {
    struct TestCase {
        opts: Option<GetOptions>,
        want_error: bool,
    }

    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    // cluster.server(1).stop().await;
    // cluster.server(2).stop().await;

    let tests = [
        TestCase {
            opts: Some(GetOptions::new().with_serializable()),
            want_error: false,
        },
        // TestCase {
        //     opts: None,
        //     want_error: true,
        // },
    ];

    for test in tests {
        let result = client.get("foo", test.opts).await;
        assert_eq!(result.is_err(), test.want_error);
    }
}

#[tokio::test]
async fn test_kv_with_empty_key() {
    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    let result = client.put("my-namespace/foobar", "data", None).await;
    assert!(result.is_ok());

    let result = client.put("my-namespace/foobar1", "data", None).await;
    assert!(result.is_ok());

    let result = client.put("namespace/foobar1", "data", None).await;
    assert!(result.is_ok());

    let result = client
        .get("n", Some(GetOptions::new().with_from_key()))
        .await;
    assert!(result.is_ok());

    let res = result.unwrap();
    assert_eq!(res.kvs().len(), 1);

    let result = client.get("", Some(GetOptions::new().with_prefix())).await;
    assert!(result.is_ok());

    let res = result.unwrap();
    assert_eq!(res.kvs().len(), 3);

    let result = client.delete("", None).await;
    assert!(result.is_err());

    let result = client
        .delete("", Some(DeleteOptions::new().with_from_key()))
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_kv_put_error() {
    // TODO: initialize the cluster with quota
    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    let result = client.put("", "data", None).await;
    assert!(result.is_err());

    // TODO: request with a too large value.
    // let result = client.put("key","a".repeat(MAX_REQUEST_SIZE + 100), None).await;
    // assert!(result.is_err());

    // TODO: request when quota is not enough.
    // let result = client.put("key","a".repeat(MAX_REQUEST_SIZE - 50), None).await;
    // assert!(result.is_ok());

    // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // let result = client.put("key","a".repeat(MAX_REQUEST_SIZE - 50), None).await;
    // assert!(result.is_err());
}

#[tokio::test]
async fn test_kv_put_with_lease() {
    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    let result = client.lease_grant(0, None).await;
    assert!(result.is_ok());
    let lease = result.unwrap();

    let result = client
        .put(
            "key",
            "data",
            Some(PutOptions::new().with_lease(lease.id())),
        )
        .await;
    assert!(result.is_ok());

    let result = client.get("key", None).await;
    assert!(result.is_ok());

    let res = result.unwrap();
    assert_eq!(res.kvs().len(), 1);
    assert_eq!(res.kvs()[0].lease(), lease.id());
    assert_eq!(res.kvs()[0].value(), b"data");
}

#[tokio::test]
async fn test_kv_put_with_ignore_value() {
    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    // FIXME: it should return an error, because the key is not exist.
    // let result = client
    //     .put("foo", "", Some(PutOptions::new().with_ignore_value()))
    //     .await;
    // assert!(result.is_err());

    let result = client.put("foo", "bar", None).await;
    assert!(result.is_ok());

    // let result = client
    //     .put("foo", "", Some(PutOptions::new().with_ignore_value()))
    //     .await;
    // assert!(result.is_ok());

    let result = client.get("foo", None).await;
    assert!(result.is_ok());

    let res = result.unwrap();
    assert_eq!(res.kvs().len(), 1);
    assert_eq!(res.kvs()[0].value(), b"bar");
}

#[tokio::test]
async fn test_kv_put_with_ignore_lease() {
    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    let result = client.lease_grant(10, None).await;
    assert!(result.is_ok());
    let lease = result.unwrap();

    // FIXME: it should return an error, because the key is not exist.
    // let result = client
    //     .put("foo", "bar", Some(PutOptions::new().with_ignore_lease()))
    //     .await;
    // assert!(result.is_err());

    let result = client
        .put("foo", "bar", Some(PutOptions::new().with_lease(lease.id())))
        .await;
    assert!(result.is_ok());

    let result = client
        .put("foo", "bar", Some(PutOptions::new().with_ignore_lease()))
        .await;
    assert!(result.is_ok());

    let result = client.get("foo", None).await;
    assert!(result.is_ok());

    let res = result.unwrap();
    assert_eq!(res.kvs().len(), 1);
    assert_eq!(res.kvs()[0].lease(), lease.id());
}

#[tokio::test]
async fn test_kv_range() {
    struct KeyValue {
        key: Vec<u8>,
        create_revision: i64,
        mod_revision: i64,
        version: i64,
        value: Vec<u8>,
        // lease: i64,
    }
    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    let key_set = ["a", "b", "c", "c", "c", "foo", "foo/abc", "fop"];
    for key in key_set {
        let result = client.put(key, "", None).await;
        assert!(result.is_ok());
    }

    let result = client.get(key_set[0], None).await;
    assert!(result.is_ok());
    let wheader = result.unwrap().take_header().unwrap();

    let opts = Some(
        GetOptions::new()
            .with_from_key()
            .with_sort(SortTarget::Key, SortOrder::Ascend),
    );
    let wantset = vec![
        KeyValue {
            key: Vec::from("a"),
            value: Vec::from(""),
            create_revision: 2,
            mod_revision: 2,
            version: 1,
        },
        KeyValue {
            key: Vec::from("b"),
            value: Vec::from(""),
            create_revision: 3,
            mod_revision: 3,
            version: 1,
        },
        KeyValue {
            key: Vec::from("c"),
            value: Vec::from(""),
            create_revision: 4,
            mod_revision: 6,
            version: 3,
        },
        KeyValue {
            key: Vec::from("foo"),
            value: Vec::from(""),
            create_revision: 7,
            mod_revision: 7,
            version: 1,
        },
        KeyValue {
            key: Vec::from("foo/abc"),
            value: Vec::from(""),
            create_revision: 8,
            mod_revision: 8,
            version: 1,
        },
        KeyValue {
            key: Vec::from("fop"),
            value: Vec::from(""),
            create_revision: 9,
            mod_revision: 9,
            version: 1,
        },
    ];

    let result = client.get("", opts).await;
    assert!(result.is_ok());
    let res = result.unwrap();
    let header = res.header().unwrap();
    assert_eq!(header.revision(), wheader.revision());
    let is_identical = res.kvs().iter().zip(wantset.iter()).all(|(kv, want)| {
        kv.key() == want.key
            && kv.value() == want.value
            && kv.create_revision() == want.create_revision
            && kv.mod_revision() == want.mod_revision
            && kv.version() == want.version
    });
    assert!(is_identical);
}

#[tokio::test]
async fn test_kv_delete_range() {
    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    let key_set = ["a", "b", "c", "c/abc", "d"];
    for key in key_set {
        let result = client.put(key, "", None).await;
        assert!(result.is_ok());
    }

    let result = client
        .delete("", Some(DeleteOptions::new().with_from_key()))
        .await;
    assert!(result.is_ok());

    let result = client
        .get("", Some(GetOptions::new().with_all_keys()))
        .await;
    assert!(result.is_ok());
    let res = result.unwrap();
    assert_eq!(res.count(), 0);
}
