use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[non_exhaustive]
#[clap(author, version, about, long_about = None)]
/// Args of Benchmark
pub struct Benchmark {
    /// The address of the server
    #[clap(long, value_delimiter = ',', default_value = "127.0.0.1:2379")]
    pub endpoints: Vec<String>,
    /// Clients number
    #[clap(long, default_value_t = 1)]
    pub clients: usize,
    /// Use curp or not
    #[clap(long)]
    pub use_curp: bool,
    /// Output to stdout
    #[clap(long)]
    pub stdout: bool,
    /// Sub command
    #[clap(subcommand)]
    pub command: Commands,
}

/// Types of sub command
#[derive(Subcommand, Debug, Clone, Copy)]
pub enum Commands {
    /// Put args
    Put {
        /// Key size
        #[clap(long, default_value_t = 8)]
        key_size: usize,
        /// Value size
        #[clap(long, default_value_t = 8)]
        val_size: usize,
        /// Total number of requests
        #[clap(long, default_value_t = 10000)]
        total: usize,
        /// Key space size
        #[clap(long, default_value_t = 1)]
        key_space_size: usize,
        /// sequential keys or not
        #[clap(long, default_value_t = false)]
        sequential_keys: bool,
    },
}
