use std::sync::OnceLock;

use xlineapi::{
    DeleteRangeResponse, KeyValue, LeaseGrantResponse, LeaseKeepAliveResponse, LeaseLeasesResponse,
    LeaseRevokeResponse, LeaseTimeToLiveResponse, PutResponse, RangeResponse, ResponseHeader,
};

/// The global printer type config
static PRINTER_TYPE: OnceLock<PrinterType> = OnceLock::new();

/// The type of the Printer
pub(crate) enum PrinterType {
    /// Simple printer, which print simplified result
    Simple,
    /// Filed printer, which print every fields of the result
    Field,
}

/// Set the type of the printer
pub(crate) fn set_printer_type(printer_type: PrinterType) {
    let _ignore = PRINTER_TYPE.get_or_init(|| printer_type);
}

/// The printer implementation trait
pub(crate) trait Printer {
    /// Print the simplified result
    fn simple(&self);
    /// Print every fields of the result
    fn field(&self);
    /// Print according to the config set
    fn print(&self) {
        match *PRINTER_TYPE
            .get()
            .unwrap_or_else(|| unreachable!("the printer type should be initialized"))
        {
            PrinterType::Simple => self.simple(),
            PrinterType::Field => self.field(),
        }
    }
}

impl Printer for PutResponse {
    fn simple(&self) {
        println!("OK");
    }

    fn field(&self) {
        FieldPrinter::header(self.header.as_ref());
        if let Some(pre_kv) = self.prev_kv.as_ref() {
            FieldPrinter::kv(pre_kv);
        }
    }
}

impl Printer for RangeResponse {
    fn simple(&self) {
        for kv in &self.kvs {
            SimplePrinter::utf8(&kv.key);
            SimplePrinter::utf8(&kv.value);
        }
    }

    fn field(&self) {
        FieldPrinter::header(self.header.as_ref());
        println!("kvs:");
        for kv in &self.kvs {
            FieldPrinter::kv(kv);
        }
        println!("more: {}, count: {}", self.more, self.count);
    }
}

impl Printer for DeleteRangeResponse {
    fn simple(&self) {
        println!("{}", self.deleted);
        for kv in &self.prev_kvs {
            SimplePrinter::utf8(&kv.key);
            SimplePrinter::utf8(&kv.value);
        }
    }

    fn field(&self) {
        FieldPrinter::header(self.header.as_ref());
        println!("kvs:");
        for kv in &self.prev_kvs {
            FieldPrinter::kv(kv);
        }
        println!("deleted: {}", self.deleted);
    }
}

impl Printer for LeaseGrantResponse {
    fn simple(&self) {
        println!("{}", self.id);
    }

    fn field(&self) {
        FieldPrinter::header(self.header.as_ref());
        println!("lease id: {}, granted ttl: {}", self.id, self.ttl);
    }
}

impl Printer for LeaseKeepAliveResponse {
    fn simple(&self) {
        println!("{}", self.ttl);
    }

    fn field(&self) {
        FieldPrinter::header(self.header.as_ref());
        println!("lease id: {} keepalived with TTL: {}", self.id, self.ttl);
    }
}

impl Printer for LeaseLeasesResponse {
    fn simple(&self) {
        for lease in &self.leases {
            println!("{}", lease.id);
        }
    }

    fn field(&self) {
        FieldPrinter::header(self.header.as_ref());
        for lease in &self.leases {
            println!("lease: {}", lease.id);
        }
    }
}

impl Printer for LeaseRevokeResponse {
    fn simple(&self) {
        println!("Revoked");
    }

    fn field(&self) {
        FieldPrinter::header(self.header.as_ref());
        println!("Revoked");
    }
}

impl Printer for LeaseTimeToLiveResponse {
    fn simple(&self) {
        println!("{}", self.ttl);
    }

    fn field(&self) {
        FieldPrinter::header(self.header.as_ref());
        println!(
            "lease id: {}, ttl: {}, granted_ttl: {}",
            self.id, self.ttl, self.granted_ttl
        );

        for key in &self.keys {
            FieldPrinter::key(key);
        }
    }
}

/// Simple Printer of common response types
struct SimplePrinter;

impl SimplePrinter {
    /// Print utf8 bytes as string
    fn utf8(vec: &[u8]) {
        println!("{}", String::from_utf8_lossy(vec));
    }
}

/// Field Printer of common response types
struct FieldPrinter;

#[allow(dead_code)]
impl FieldPrinter {
    /// Response header printer
    fn header(header: Option<&ResponseHeader>) {
        let Some(header) = header else { return };
        println!("header:");
        println!(
            "cluster_id: {}, member_id: {}, revision: {}, raft_term: {}",
            header.cluster_id, header.member_id, header.revision, header.raft_term
        );
    }

    /// Response key printer
    pub(crate) fn key(key: &[u8]) {
        println!("key: {}", String::from_utf8_lossy(key));
    }

    /// Response value printer
    pub(crate) fn value(value: &[u8]) {
        println!("value: {}", String::from_utf8_lossy(value));
    }

    /// Response key-value printer
    fn kv(kv: &KeyValue) {
        println!(
            "key: {}, value: {}",
            String::from_utf8_lossy(&kv.key),
            String::from_utf8_lossy(&kv.value)
        );
    }
}
