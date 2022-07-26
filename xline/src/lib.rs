/// rpc definition module
mod rpc;
/// Xline server
pub mod server;
/// Storage module
mod storage;

use std::net::{AddrParseError, SocketAddr};

use clap::Parser;

/// Command line arguments
#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct ServerArgs {
    /// Node name
    #[clap(long)]
    pub name: String,
    /// Cluster peers. eg: node1=192.168.x.x:8080;node2=192.168.x.x:8080
    #[clap(long)]
    pub cluster_peers: String,
    /// Current node ip and port. eg: 192.168.x.x:8080
    #[clap(long, parse(try_from_str))]
    pub ip_port: SocketAddr,
    /// if node is leader
    #[clap(long)]
    pub is_leader: bool,
    /// leader's ip and port. eg: 192.168.x.x:8080
    #[clap(long, parse(try_from_str))]
    pub leader_ip_port: SocketAddr,
    /// current node ip and port. eg: 192.168.x.x:8080
    #[clap(long, parse(try_from_str))]
    pub self_ip_port: SocketAddr,
}

/// Parse member list
pub fn parse_members(member_list: &str) -> Result<Vec<SocketAddr>, AddrParseError> {
    member_list.split(';').map(str::parse).collect()
}
