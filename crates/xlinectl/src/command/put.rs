use anyhow::Result;
use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{types::kv::PutRequest, Client};

use crate::utils::printer::Printer;

/// Definition of `get` command
pub(crate) fn command() -> Command {
    Command::new("put")
        .about("Puts the given key into the store")
        .arg(arg!(<key> "The key"))
        // TODO: support reading value from stdin
        .arg(arg!(<value> "The value"))
        .arg(
            arg!(--lease <ID> "lease ID to attach to the key")
                .value_parser(value_parser!(i64))
                .default_value("0"),
        )
        .arg(arg!(--prev_kv "return the previous key-value pair before modification"))
        .arg(arg!(--ignore_value "updates the key using its current value"))
        .arg(arg!(--ignore_lease "updates the key using its current lease"))
}

/// Build request from matches
pub(crate) fn build_request(matches: &ArgMatches) -> PutRequest {
    let key = matches.get_one::<String>("key").expect("required");
    let value = matches.get_one::<String>("value").expect("required");
    let lease = matches.get_one::<i64>("lease").expect("required");
    let prev_kv = matches.get_flag("prev_kv");
    let ignore_value = matches.get_flag("ignore_value");
    let ignore_lease = matches.get_flag("ignore_lease");

    let mut request = PutRequest::new(key.as_bytes(), value.as_bytes());
    request = request.with_lease(*lease);
    request = request.with_prev_kv(prev_kv);
    request = request.with_ignore_value(ignore_value);
    request = request.with_ignore_lease(ignore_lease);

    request
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.kv_client().put(req).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(PutRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["put", "key1", "value1"],
                Some(PutRequest::new("key1".as_bytes(), "value1".as_bytes())),
            ),
            TestCase::new(
                vec!["put", "key2", "value2", "--lease", "1"],
                Some(PutRequest::new("key2".as_bytes(), "value2".as_bytes()).with_lease(1)),
            ),
            TestCase::new(
                vec!["put", "key3", "value3", "--prev_kv"],
                Some(PutRequest::new("key3".as_bytes(), "value3".as_bytes()).with_prev_kv(true)),
            ),
            TestCase::new(
                vec!["put", "key4", "value4", "--ignore_value"],
                Some(
                    PutRequest::new("key4".as_bytes(), "value4".as_bytes()).with_ignore_value(true),
                ),
            ),
            TestCase::new(
                vec!["put", "key5", "value5", "--ignore_lease"],
                Some(
                    PutRequest::new("key5".as_bytes(), "value5".as_bytes()).with_ignore_lease(true),
                ),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
