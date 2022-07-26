mod common;

use common::Cluster;
use etcd_client::{EtcdDeleteRequest, EtcdGetRequest, EtcdPutRequest, EtcdRangeRequest, KeyRange};

#[tokio::test]
async fn test_kv_put() {
    let mut cluster = Cluster::run(3).await;
    let client = cluster.client(0).await;

    let put_req = EtcdPutRequest::new("foo", "bar");
    let result = client.kv().put(put_req).await;
    println!("result: {:?}", result);
    assert!(result.is_ok());

    let get_req = EtcdGetRequest::new("foo");
    let result = client.kv().get(get_req).await;

    assert!(result.is_ok());
    let kvs = result.unwrap().take_kvs();
    assert_eq!(kvs.len(), 1);
    assert_eq!(kvs[0].key_str(), "foo");
    assert_eq!(kvs[0].value_str(), "bar");
}

// #[tokio::test]
// async fn test_kv_get() {
//     // struct RangeOptions {
//     //     revision: u64,
//     //     prefix: bool,
//     //     // sort_order: RangeRequest_SortOrder,
//     // }
//     // struct TestCase<'a> {
//     //     start_key: &'a str,
//     //     end_key: &'a str,
//     //     want_kvs: &'a [&'a str],
//     // }
//     let mut cluster = Cluster::run(3).await;
//     let client = cluster.client(0).await;
//     let kvs = ["a", "b", "c", "c", "c", "foo", "foo/abc", "fop"];
//     // let want_kvs = ["a", "b", "c", "foo", "foo/abc", "fop"];
//     // let kvs_by_version = vec!["a", "b", "foo", "foo/abc", "fop", "c"];
//     // let reversed_kvs = vec!["fop", "foo/abc", "foo", "c", "b", "a"];
//     for key in kvs {
//         let put_req = EtcdPutRequest::new(key, "bar");
//         let result = client.kv().put(put_req).await;
//         assert!(result.is_ok());
//     }
// }

#[tokio::test]
async fn test_kv_delete() {
    #[derive(Default)]
    struct TestCase<'a> {
        key: &'a str,
        prefix: bool,
        from_key: bool,
        end: &'a str,

        want_deleted: usize,
        want_keys: &'a [&'a str],
    }

    let mut cluster = Cluster::run(3).await;
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
            end: "c",
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
            let put_req = EtcdPutRequest::new(key, "bar");
            let result = client.kv().put(put_req).await;
            assert!(result.is_ok());
        }

        let range = if test.prefix {
            KeyRange::prefix(test.key)
        } else if test.from_key {
            KeyRange::range(test.key, vec![0])
        } else {
            KeyRange::range(test.key, test.end)
        };
        let del_req = EtcdDeleteRequest::new(range);
        let result = client.kv().delete(del_req).await;
        assert!(result.is_ok());

        let res = result.unwrap();
        assert_eq!(res.count_deleted(), test.want_deleted);

        let get_req = EtcdRangeRequest::new(KeyRange::all());
        let result = client.kv().range(get_req).await;
        assert!(result.is_ok());
        let is_identical = result
            .unwrap()
            .take_kvs()
            .iter()
            .zip(test.want_keys.iter())
            .all(|(kv, want)| kv.key_str() == *want);

        assert!(is_identical);
    }
}

#[tokio::test]
async fn test_kv_with_empty_key() {
    let mut cluster = Cluster::run(3).await;
    let client = cluster.client(0).await;

    let put_req = EtcdPutRequest::new("my-namespace/foobar", "data");
    let result = client.kv().put(put_req).await;
    assert!(result.is_ok());

    let put_req = EtcdPutRequest::new("my-namespace/foobar1", "data");
    let result = client.kv().put(put_req).await;
    assert!(result.is_ok());

    let put_req = EtcdPutRequest::new("namespace/foobar1", "data");
    let result = client.kv().put(put_req).await;
    assert!(result.is_ok());

    // FIXME: from-key returns all keys, it should return only keys greater than the start key

    // let keyrange_fromkey = KeyRange::range("n", vec![0]);
    // let range_req = EtcdRangeRequest::new(keyrange_fromkey);
    // let resp = client.kv().range(range_req).await.unwrap().get_kvs();
    // assert_eq!(resp.len(), 1);

    let keyrange_prefix = KeyRange::prefix("");
    let range_req = EtcdRangeRequest::new(keyrange_prefix);
    let result = client.kv().range(range_req).await;
    assert!(result.is_ok());
    let kvs = result.unwrap().take_kvs();
    assert_eq!(kvs.len(), 3);
}

#[tokio::test]
async fn test_kv_put_error() {
    // TODO: initialize the cluster with quota
    let mut cluster = Cluster::run(3).await;
    let client = cluster.client(0).await;

    let put_req = EtcdPutRequest::new("", "data");
    let result = client.kv().put(put_req).await;
    println!("result: {:?}", result);

    // FIXME: this result should be an error.
    // assert!(result.is_err());

    // TODO: request with a too large value.
    // let put_req = PutRequest::new("key", "a".repeat(MAX_REQUEST_SIZE+100));
    // let result = client.kv().put(put_req).await;
    // println!("result: {:?}", result);
    // assert!(result.is_err());

    // TODO: request when quota is not enough.
    // let put_req = PutRequest::new("key", "a".repeat(MAX_REQUEST_SIZE - 50));
    // let result = client.kv().put(put_req).await;
    // assert!(result.is_ok());
    // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    // let put_req = PutRequest::new("key", "a".repeat(MAX_REQUEST_SIZE - 50));
    // let result = client.kv().put(put_req).await;
    // assert!(result.is_err());
}
