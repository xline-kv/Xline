use std::sync::OnceLock;

use xlineapi::{KeyValue, PutResponse, RangeResponse, ResponseHeader};

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

    /// Response key-value printer
    fn kv(kv: &KeyValue) {
        println!(
            "key: {}, value: {}",
            String::from_utf8_lossy(&kv.key),
            String::from_utf8_lossy(&kv.value)
        );
    }
}
