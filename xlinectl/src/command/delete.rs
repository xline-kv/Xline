use anyhow::Result;
use clap::{arg, ArgMatches, Command};
use xline_client::{types::kv::DeleteRangeRequest, Client};

use crate::utils::printer::Printer;

/// Definition of `delete` command
pub(crate) fn command() -> Command {
    Command::new("delete")
        .about("Deletes the key or a range of keys")
        .arg(arg!(<key> "The key"))
        .arg(arg!([range_end] "The range end"))
        .arg(
            arg!(--prefix "delete keys with matching prefix")
                .conflicts_with("range_end"),
        )
        .arg(
            arg!(--prev_kv "return deleted key-value pairs")
        )
        .arg(
            arg!(--from_key "delete keys that are greater than or equal to the given key using byte compare")
                .conflicts_with("prefix")
                .conflicts_with("range_end"),
        )
}

/// Build request from matches
pub(crate) fn build_request(matches: &ArgMatches) -> DeleteRangeRequest {
    let key = matches.get_one::<String>("key").expect("required");
    let range_end = matches.get_one::<String>("range_end");
    let prefix = matches.get_flag("prefix");
    let prev_kv = matches.get_flag("prev_kv");
    let from_key = matches.get_flag("from_key");

    let mut request = DeleteRangeRequest::new(key.as_bytes());
    if let Some(range_end) = range_end {
        request = request.with_range_end(range_end.as_bytes());
    }
    if prefix {
        request = request.with_prefix();
    }
    request = request.with_prev_kv(prev_kv);
    if from_key {
        request = request.with_from_key();
    }

    request
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.kv_client().delete(req).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(DeleteRangeRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["delete", "key1"],
                Some(DeleteRangeRequest::new("key1".as_bytes())),
            ),
            TestCase::new(
                vec!["delete", "key2", "end2"],
                Some(DeleteRangeRequest::new("key2".as_bytes()).with_range_end("end2".as_bytes())),
            ),
            TestCase::new(
                vec!["delete", "key3", "--prefix"],
                Some(DeleteRangeRequest::new("key3".as_bytes()).with_prefix()),
            ),
            TestCase::new(
                vec!["delete", "key4", "--prev_kv"],
                Some(DeleteRangeRequest::new("key4".as_bytes()).with_prev_kv(true)),
            ),
            TestCase::new(
                vec!["delete", "key5", "--from_key"],
                Some(DeleteRangeRequest::new("key5".as_bytes()).with_from_key()),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }

    #[test]
    fn command_parse_should_be_invalid() {
        let test_cases = vec![
            TestCase::new(vec!["delete", "key", "key2", "--from_key"], None),
            TestCase::new(vec!["delete", "key", "key2", "--prefix"], None),
            TestCase::new(vec!["delete", "key", "--from_key", "--prefix"], None),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
