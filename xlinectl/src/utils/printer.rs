use xlineapi::{KeyValue, ResponseHeader};

/// Printer of common response types
pub(crate) struct Printer;

impl Printer {
    /// Response header printer
    pub(crate) fn header(header: Option<&ResponseHeader>) {
        let Some(header) = header else { return };
        println!("header:");
        println!(
            "cluster_id: {}, member_id: {}, revision: {}, raft_term: {}",
            header.cluster_id, header.member_id, header.revision, header.raft_term
        );
    }

    /// Response key-value printer
    pub(crate) fn kv(kv: &KeyValue) {
        println!(
            "key: {}, value: {}",
            String::from_utf8_lossy(&kv.key),
            String::from_utf8_lossy(&kv.value)
        );
    }
}
