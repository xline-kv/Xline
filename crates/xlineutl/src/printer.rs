use std::sync::OnceLock;

use serde::Serialize;

/// The global printer type config
static PRINTER_TYPE: OnceLock<PrinterType> = OnceLock::new();

/// The type of the Printer
pub(crate) enum PrinterType {
    /// Simple printer, which print simplified result
    Simple,
    /// Filed printer, which print every fields of the result
    Field,
    /// JSON printer, which print in JSON format
    Json,
}

/// Set the type of the printer
pub(crate) fn set_printer_type(printer_type: PrinterType) {
    let _ignore = PRINTER_TYPE.get_or_init(|| printer_type);
}

/// The printer implementation trait
pub(crate) trait Printer: Serialize {
    /// Print the simplified result
    fn simple(&self);
    /// Print every fields of the result
    fn field(&self);
    /// Print the result in JSON format
    fn json(&self) {
        println!(
            "{}",
            serde_json::to_string_pretty(self).expect("failed to serialize result")
        );
    }
    /// Print according to the config set
    fn print(&self) {
        match *PRINTER_TYPE
            .get()
            .unwrap_or_else(|| unreachable!("the printer type should be initialized"))
        {
            PrinterType::Simple => self.simple(),
            PrinterType::Field => self.field(),
            PrinterType::Json => self.json(),
        }
    }
}
