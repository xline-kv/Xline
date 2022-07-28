mod common;

use common::Cluster;
use etcd_rs::{DeleteRequest, KeyRange, KeyValueOp, PutRequest, RangeRequest};

#[tokio::test]
async fn test_kv_put() {
    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    let put_req = PutRequest::new("foo", "bar");
    let result = client.put(put_req).await;
    println!("{:?}", result);

    assert!(result.is_ok());

    let key_range = KeyRange::key("foo");
    let get_req = RangeRequest::new(key_range);
    let result = client.get(get_req).await;
    assert!(result.is_ok());

    let kvs = result.unwrap().kvs;
    assert_eq!(kvs.len(), 1);
    assert_eq!(kvs[0].key_str(), "foo");
    assert_eq!(kvs[0].value_str(), "bar");
}

// #[tokio::test]
// async fn test_kv_get() {
//     #[derive(Default)]
//     struct TestCase<'a> {
//         key: &'a str,
//         end: Option<&'a str>,
//         serialized: bool,
//         prefix: bool,
//         from_key: bool,
//         count_only: bool,
//         limit: Option<usize>,
//         order: Option<u8>,
//         sort_by: Option<u8>,
//         revision: Option<u64>,

//         want_kvs: &'a [&'a str],
//     }
//     let mut cluster = Cluster::run(3).await;
//     let client = cluster.client(0).await;
//     let kvs = ["a", "b", "c", "c", "c", "foo", "foo/abc", "fop"];
//     let want_kvs = ["a", "b", "c", "foo", "foo/abc", "fop"];
//     // let kvs_by_version = ["a", "b", "foo", "foo/abc", "fop", "c"];
//     // let reversed_kvs = ["fop", "foo/abc", "foo", "c", "b", "a"];

//     let tests = [
//         TestCase {
//             key: "a",
//             want_kvs: &want_kvs[..1],
//             ..Default::default()
//         },
//         // TestCase {
//         //     key: "a",
//         //     serialized: true,
//         //     want_kvs: &want_kvs[..1],
//         //     ..Default::default()
//         // },
//         TestCase {
//             key: "a",
//             end: Some("c"),
//             want_kvs: &want_kvs[..2],
//             ..Default::default()
//         },
//         TestCase {
//             key: "",
//             prefix: true,
//             want_kvs: &want_kvs[..],
//             ..Default::default()
//         },
//         // TestCase {
//         //     key: "",
//         //     from_key: true,
//         //     want_kvs: &want_kvs[..],
//         //     ..Default::default()
//         // },
//         TestCase {
//             key: "a",
//             end: Some("x"),
//             want_kvs: &want_kvs[..],
//             ..Default::default()
//         },
//         TestCase {
//             key: "",
//             prefix: true,
//             revision: Some(4),
//             want_kvs: &want_kvs[..3],
//             ..Default::default()
//         },
//         TestCase {
//             key: "a",
//             count_only: true,
//             want_kvs: &[],
//             ..Default::default()
//         },
//         TestCase {
//             key: "foo",
//             prefix: true,
//             want_kvs: &["foo", "foo/abc"],
//             ..Default::default()
//         },
//         // TestCase {
//         //     key: "foo",
//         //     from_key: true,
//         //     want_kvs: &["foo", "foo/abc", "fop"],
//         //     ..Default::default()
//         // },
//         TestCase {
//             key: "",
//             prefix: true,
//             limit: Some(2),
//             want_kvs: &want_kvs[..2],
//             ..Default::default()
//         },
//         // TestCase {
//         //     key: "",
//         //     prefix: true,
//         //     order: Some(SortAscend),
//         //     sort_by: Some(SortByModRevision),
//         //     want_kvs: &want_kvs[..],
//         //     ..Default::default()
//         // },
//         // TestCase {
//         //     key: "",
//         //     prefix: true,
//         //     order: Some(SortAscend),
//         //     sort_by: Some(SortByVersion),
//         //     want_kvs: &kvs_by_version[..],
//         //     ..Default::default()
//         // },
//         // TestCase {
//         //     key: "",
//         //     prefix: true,
//         //     order: Some(SortNone),
//         //     sort_by: Some(SortByCreateRevision),
//         //     want_kvs: &want_kvs[..],
//         //     ..Default::default()
//         // },
//         // TestCase {
//         //     key: "",
//         //     prefix: true,
//         //     order: Some(SortDescend),
//         //     sort_by: Some(SortByCreateRevision),
//         //     want_kvs: &reversed_kvs[..],
//         //     ..Default::default()
//         // },
//         // TestCase {
//         //     key: "",
//         //     prefix: true,
//         //     order: Some(SortDescend),
//         //     sort_by: Some(SortByKey),
//         //     want_kvs: &reversed_kvs[..],
//         //     ..Default::default()
//         // },
//     ];

//     for key in kvs {
//         let put_req = PutRequest::new(key, "bar");
//         let result = client.put(put_req).await;
//         assert!(result.is_ok());
//     }

//     for test in tests {
//         let range = if test.prefix {
//             KeyRange::prefix(test.key)
//         } else if test.from_key {
//             KeyRange::range(test.key, vec![0])
//         } else if let Some(end) = test.end {
//             KeyRange::range(test.key, end)
//         } else {
//             KeyRange::key(test.key)
//         };

//         let mut _range_req = RangeRequest::new(range);
//     }
// }

#[tokio::test]
async fn test_kv_delete() {
    #[derive(Default)]
    struct TestCase<'a> {
        key: &'a str,
        prefix: bool,
        from_key: bool,
        end: Option<&'a str>,

        want_deleted: u64,
        want_keys: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    let keys = ["a", "b", "c", "c/abc", "d"];

    // FIXME: this test is not working. because `from_key` is not implemented yet.
    let tests = [
        TestCase {
            key: "",
            prefix: true,
            want_deleted: 5,
            ..Default::default()
        },
        // TestCase {
        //     key: "",
        //     from_key: true,
        //     want_deleted: 5,
        //     ..Default::default()
        // },
        TestCase {
            key: "a",
            end: Some("c"),
            want_deleted: 2,
            want_keys: &["c", "c/abc", "d"],
            ..Default::default()
        },
        TestCase {
            key: "c",
            want_deleted: 1,
            want_keys: &["a", "b", "c/abc", "d"],
            ..Default::default()
        },
        TestCase {
            key: "c",
            prefix: true,
            want_deleted: 2,
            want_keys: &["a", "b", "d"],
            ..Default::default()
        },
        // TestCase {
        //     key: "c",
        //     from_key: true,
        //     want_deleted: 3,
        //     want_keys: &["a", "b"],
        //     ..Default::default()
        // },
        TestCase {
            key: "e",
            want_deleted: 0,
            want_keys: &keys,
            ..Default::default()
        },
    ];
    for test in tests {
        for key in keys {
            let put_req = PutRequest::new(key, "bar");
            let result = client.put(put_req).await;
            assert!(result.is_ok());
        }

        let range = if test.prefix {
            KeyRange::prefix(test.key)
        } else if test.from_key {
            KeyRange::range(test.key, vec![0])
        } else if let Some(end) = test.end {
            KeyRange::range(test.key, end)
        } else {
            KeyRange::key(test.key)
        };
        let del_req = DeleteRequest::new(range);
        let result = client.delete(del_req).await;
        assert!(result.is_ok());

        let res = result.unwrap();
        assert_eq!(res.deleted, test.want_deleted);

        let get_req = RangeRequest::new(KeyRange::all());
        let result = client.get(get_req).await;
        assert!(result.is_ok());
        let is_identical = result
            .unwrap()
            .kvs
            .iter()
            .zip(test.want_keys.iter())
            .all(|(kv, want)| kv.key_str() == *want);

        assert!(is_identical);
    }
}

#[tokio::test]
async fn test_kv_with_empty_key() {
    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    let put_req = PutRequest::new("my-namespace/foobar", "data");
    let result = client.put(put_req).await;
    assert!(result.is_ok());

    let put_req = PutRequest::new("my-namespace/foobar1", "data");
    let result = client.put(put_req).await;
    assert!(result.is_ok());

    let put_req = PutRequest::new("namespace/foobar1", "data");
    let result = client.put(put_req).await;
    assert!(result.is_ok());

    // FIXME: from-key returns all keys, it should return only keys greater than the start key

    // let keyrange_fromkey = KeyRange::range("n", vec![0]);
    // let range_req = EtcdRangeRequest::new(keyrange_fromkey);
    // let resp = client.range(range_req).await.unwrap().get_kvs();
    // assert_eq!(resp.len(), 1);

    let key_range = KeyRange::prefix("");
    let get_req = RangeRequest::new(key_range);
    let result = client.get(get_req).await;
    assert!(result.is_ok());
    let kvs = result.unwrap().kvs;
    assert_eq!(kvs.len(), 3);
}

#[tokio::test]
async fn test_kv_put_error() {
    // TODO: initialize the cluster with quota
    let mut cluster = Cluster::new(3);
    cluster.start().await;
    let client = cluster.client(0).await;

    let put_req = PutRequest::new("", "data");
    let result = client.put(put_req).await;
    assert!(result.is_err());

    // TODO: request with a too large value.
    // let put_req = PutRequest::new("key", "a".repeat(MAX_REQUEST_SIZE+100));
    // let result = client.put(put_req).await;
    // println!("result: {:?}", result);
    // assert!(result.is_err());

    // TODO: request when quota is not enough.
    // let put_req = PutRequest::new("key", "a".repeat(MAX_REQUEST_SIZE - 50));
    // let result = client.put(put_req).await;
    // assert!(result.is_ok());
    // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    // let put_req = PutRequest::new("key", "a".repeat(MAX_REQUEST_SIZE - 50));
    // let result = client.put(put_req).await;
    // assert!(result.is_err());
}
