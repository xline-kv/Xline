use anyhow::Result;
use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{types::kv::RangeRequest, Client};
use xlineapi::{SortOrder, SortTarget};

use crate::utils::printer::Printer;

/// Definition of `get` command
pub(crate) fn command() -> Command {
    Command::new("get")
        .about("Gets the key or a range of keys")
        .arg(arg!(<key> "The key"))
        .arg(arg!([range_end] "Range end"))
        .arg(
            arg!(--consistency <CONSISTENCY> "Linearizable(L) or Serializable(S)")
                .value_parser(["L", "S"])
                .default_value("L"),
        )
        .arg(
            arg!(--order <ORDER> "Order of results")
                .value_parser(["ASCEND", "DESCEND"])
        )
        .arg(
            arg!(--sort_by <SORTBY> "Sort target")
                .value_parser([ "CREATE", "KEY", "MODIFY", "VALUE", "VERSION",])
        )
        .arg(
            arg!(--limit <LIMIT> "Maximum number of results")
                .value_parser(value_parser!(i64))
                .default_value("0"),
        )
        .arg(
            arg!(--prefix "Get keys with matching prefix")
                .conflicts_with("range_end")
        )
        .arg(
            arg!(--from_key "Get keys that are greater than or equal to the given key using byte compare")
                .conflicts_with("prefix")
                .conflicts_with("range_end")
        )
        .arg(
            arg!(--rev <REV> "Specify the kv revision")
                .value_parser(value_parser!(i64))
                .default_value("0")
        )
        .arg(
            arg!(--keys_only "Get only the keys")
        )
        .arg(
            arg!(--count_only "Get only the count")
                .conflicts_with("keys_only")
        )
}

/// Build request from matches
pub(crate) fn build_request(matches: &ArgMatches) -> RangeRequest {
    let key = matches.get_one::<String>("key").expect("required");
    let range_end = matches.get_one::<String>("range_end");
    let consistency = matches.get_one::<String>("consistency").expect("required");
    let order = matches.get_one::<String>("order");
    let sort_by = matches.get_one::<String>("sort_by");
    let limit = matches.get_one::<i64>("limit").expect("Required");
    let prefix = matches.get_flag("prefix");
    let from_key = matches.get_flag("from_key");
    let rev = matches.get_one::<i64>("rev").expect("Required");
    let keys_only = matches.get_flag("keys_only");
    let count_only = matches.get_flag("count_only");

    let mut request = RangeRequest::new(key.as_bytes());
    if let Some(range_end) = range_end {
        request = request.with_range_end(range_end.as_bytes());
    }
    request = match consistency.as_str() {
        "L" => request.with_serializable(false),
        "S" => request.with_serializable(true),
        _ => unreachable!("The format should be checked by Clap."),
    };
    if let Some(order) = order {
        request = request.with_sort_order(match order.as_str() {
            "ASCEND" => SortOrder::Ascend,
            "DESCEND" => SortOrder::Descend,
            _ => unreachable!("The format should be checked by Clap."),
        });
    }
    if let Some(sort_by) = sort_by {
        request = request.with_sort_target(match sort_by.as_str() {
            "CREATE" => SortTarget::Create,
            "KEY" => SortTarget::Key,
            "MODIFY" => SortTarget::Mod,
            "VALUE" => SortTarget::Value,
            "VERSION" => SortTarget::Version,
            _ => unreachable!("The format should be checked by Clap."),
        });
    }
    request = request.with_limit(*limit);
    if prefix {
        request = request.with_prefix();
    }
    if from_key {
        request = request.with_from_key();
    }
    request = request.with_revision(*rev);
    request = request.with_keys_only(keys_only);
    request = request.with_count_only(count_only);

    request
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.kv_client().range(req).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(RangeRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["get", "key"],
                Some(RangeRequest::new("key".as_bytes())),
            ),
            TestCase::new(
                vec!["get", "key", "key2"],
                Some(RangeRequest::new("key".as_bytes()).with_range_end("key2".as_bytes())),
            ),
            TestCase::new(
                vec!["get", "key", "--consistency", "L"],
                Some(RangeRequest::new("key".as_bytes()).with_serializable(false)),
            ),
            TestCase::new(
                vec!["get", "key", "--order", "DESCEND"],
                Some(RangeRequest::new("key".as_bytes()).with_sort_order(SortOrder::Descend)),
            ),
            TestCase::new(
                vec!["get", "key", "--sort_by", "MODIFY"],
                Some(RangeRequest::new("key".as_bytes()).with_sort_target(SortTarget::Mod)),
            ),
            TestCase::new(
                vec!["get", "key", "--limit", "10"],
                Some(RangeRequest::new("key".as_bytes()).with_limit(10)),
            ),
            TestCase::new(
                vec!["get", "key", "--prefix"],
                Some(RangeRequest::new("key".as_bytes()).with_prefix()),
            ),
            TestCase::new(
                vec!["get", "key", "--from_key"],
                Some(RangeRequest::new("key".as_bytes()).with_from_key()),
            ),
            TestCase::new(
                vec!["get", "key", "--rev", "5"],
                Some(RangeRequest::new("key".as_bytes()).with_revision(5)),
            ),
            TestCase::new(
                vec!["get", "key", "--keys_only"],
                Some(RangeRequest::new("key".as_bytes()).with_keys_only(true)),
            ),
            TestCase::new(
                vec!["get", "key", "--count_only"],
                Some(RangeRequest::new("key".as_bytes()).with_count_only(true)),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }

    #[test]
    fn command_parse_should_be_invalid() {
        let test_cases = vec![
            TestCase::new(vec!["get", "key", "key2", "--from_key"], None),
            TestCase::new(vec!["get", "key", "key2", "--prefix"], None),
            TestCase::new(vec!["get", "key", "--from_key", "--prefix"], None),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
