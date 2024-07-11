use anyhow::Result;
use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{types::kv::PutOptions, Client};

use crate::utils::printer::Printer;

/// Indicates the type of the request builted.
/// The first `Vec<u8>` is the key, the second `Vec<u8>` is the value,
/// and the third is the options.
type PutRequest = (Vec<u8>, Vec<u8>, PutOptions);

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

    let request = PutOptions::default()
        .with_lease(*lease)
        .with_prev_kv(prev_kv)
        .with_ignore_value(ignore_value)
        .with_ignore_lease(ignore_lease);

    (key.as_bytes().to_vec(), value.as_bytes().to_vec(), request)
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.kv_client().put(req.0, req.1, Some(req.2)).await?;
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
                Some(("key1".into(), "value1".into(), PutOptions::default())),
            ),
            TestCase::new(
                vec!["put", "key2", "value2", "--lease", "1"],
                Some((
                    "key2".into(),
                    "value2".into(),
                    PutOptions::default().with_lease(1),
                )),
            ),
            TestCase::new(
                vec!["put", "key3", "value3", "--prev_kv"],
                Some((
                    "key3".into(),
                    "value3".into(),
                    PutOptions::default().with_prev_kv(true),
                )),
            ),
            TestCase::new(
                vec!["put", "key4", "value4", "--ignore_value"],
                Some((
                    "key4".into(),
                    "value4".into(),
                    PutOptions::default().with_ignore_value(true),
                )),
            ),
            TestCase::new(
                vec!["put", "key5", "value5", "--ignore_lease"],
                Some((
                    "key5".into(),
                    "value5".into(),
                    PutOptions::default().with_ignore_lease(true),
                )),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
