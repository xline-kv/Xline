use std::io;

use anyhow::{anyhow, bail, Result};
use clap::{arg, ArgMatches, Command};
use regex::Regex;
use xline_client::{
    types::kv::{Compare, TxnOp, TxnRequest},
    Client,
};
use xlineapi::CompareResult;

use crate::{delete, get, put, utils::printer::Printer};

/// Definition of `txn` command
pub(crate) fn command() -> Command {
    Command::new("txn")
        .about("Txn processes all the requests in one transaction")
        .arg(arg!(--interactive "set interactive mode"))
}

/// Build request from matches
pub(crate) fn build_request(matches: &ArgMatches) -> Result<TxnRequest> {
    let interactive = matches.get_flag("interactive");
    let (cmp_arg, op_then_arg, op_else_arg) = if interactive {
        /// Read until empty line from stdin
        fn read() -> Result<Vec<String>> {
            Ok(io::stdin()
                .lines()
                .take_while(|line| {
                    line.as_ref()
                        .map_or_else(|_| false, |l| !l.trim().is_empty())
                })
                .collect::<std::io::Result<_>>()?)
        }

        println!("Type an empty line to finish input");
        println!("compares:");
        let cmp_arg = read()?;
        println!("successful request:");
        let op_then_arg = read()?;
        println!("failure request:");
        let op_else_arg = read()?;
        (cmp_arg, op_then_arg, op_else_arg)
    } else {
        let input = io::read_to_string(io::stdin())?;
        let args: Vec<_> = input
            .split("\n\n")
            .filter(|s| !s.trim().is_empty())
            .map(ToOwned::to_owned)
            .collect();
        if args.len() != 3 {
            bail!("the arg length should be 3");
        }
        #[allow(clippy::indexing_slicing)] // checked above so it's safe to index
        (
            args[0].lines().map(ToOwned::to_owned).collect(),
            args[1].lines().map(ToOwned::to_owned).collect(),
            args[2].lines().map(ToOwned::to_owned).collect(),
        )
    };

    let cmp: Vec<_> = cmp_arg
        .iter()
        .map(|line| parse_cmp_line(line))
        .collect::<Result<_>>()?;
    let op_then: Vec<_> = op_then_arg
        .iter()
        .map(|line| parse_op_line(line))
        .collect::<Result<_>>()?;
    let op_else: Vec<_> = op_else_arg
        .iter()
        .map(|line| parse_op_line(line))
        .collect::<Result<_>>()?;

    Ok(TxnRequest::new()
        .when(cmp)
        .and_then(op_then)
        .or_else(op_else))
}

/// Parse one line of compare command
fn parse_cmp_line(line: &str) -> Result<Compare> {
    // match something like `mod("key1) > "0"`
    #[allow(clippy::unwrap_used)] // This regex is tested to be valid
    let re = Regex::new(r#"(\w+)\("([^"]+)"\) ([<=>]) "([^"]+)"$"#).unwrap();

    match re.captures(line) {
        #[allow(clippy::indexing_slicing)] // checked in regex so it's safe to index
        Some(cap) => {
            let target = &cap[1];
            let key = &cap[2];
            let op = &cap[3];
            let val = &cap[4];

            let cmp_op = match op {
                "<" => CompareResult::Less,
                "=" => CompareResult::Equal,
                ">" => CompareResult::Greater,
                _ => {
                    bail!("no such cmp operator")
                }
            };

            match target {
                "ver" | "version" => {
                    let v = val.parse()?;
                    Ok(Compare::version(key, cmp_op, v))
                }
                "c" | "create" => {
                    let v = val.parse()?;
                    Ok(Compare::create_revision(key, cmp_op, v))
                }
                "m" | "mod" => {
                    let v = val.parse()?;
                    Ok(Compare::mod_revision(key, cmp_op, v))
                }
                "val" | "value" => Ok(Compare::value(key, cmp_op, val.as_bytes())),
                "lease" => {
                    let v = val.parse()?;
                    Ok(Compare::lease(key, cmp_op, v))
                }
                _ => Err(anyhow!("no such compare type")),
            }
        }
        None => Err(anyhow!("input cmp not match")),
    }
}

/// Parse one line of operation command
fn parse_op_line(line: &str) -> Result<TxnOp> {
    let put_cmd = put::command();
    let get_cmd = get::command();
    let delete_cmd = delete::command();

    let args =
        shlex::split(line).ok_or_else(|| anyhow!(format!("parse op failed in: `{line}`")))?;

    #[allow(clippy::indexing_slicing)] // there should be at least one argument
    match args[0].as_str() {
        "put" => {
            let matches = put_cmd.try_get_matches_from(args.clone())?;
            let req = put::build_request(&matches);
            Ok(TxnOp::put(req))
        }
        "get" => {
            let matches = get_cmd.try_get_matches_from(args.clone())?;
            let req = get::build_request(&matches);
            Ok(TxnOp::range(req))
        }
        "delete" => {
            let matches = delete_cmd.try_get_matches_from(args.clone())?;
            let req = delete::build_request(&matches);
            Ok(TxnOp::delete(req))
        }
        _ => Err(anyhow!(format!("parse op failed in: `{line}`"))),
    }
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches)?;
    let resp = client.kv_client().txn(req).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use xline_client::types::kv::{PutRequest, RangeRequest};

    use super::*;

    #[test]
    fn parse_cmp() {
        assert_eq!(
            parse_cmp_line(r#"mod("key1") > "0""#).unwrap(),
            Compare::mod_revision("key1", CompareResult::Greater, 0)
        );
        assert_eq!(
            parse_cmp_line(r#"create("key2") = "0""#).unwrap(),
            Compare::create_revision("key2", CompareResult::Equal, 0)
        );
    }

    #[test]
    fn parse_op() {
        assert_eq!(
            parse_op_line(r#"put key1 "created-key1""#).unwrap(),
            TxnOp::put(PutRequest::new("key1", "created-key1"))
        );
        assert_eq!(
            parse_op_line(r#"get key1 key11"#).unwrap(),
            TxnOp::range(RangeRequest::new("key1").with_range_end("key11"))
        );
        assert_eq!(
            parse_op_line(r#"get key1 --from_key"#).unwrap(),
            TxnOp::range(RangeRequest::new("key1").with_from_key())
        );
    }
}
