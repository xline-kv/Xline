use std::io;
use std::{collections::HashMap, ffi::OsString};

use anyhow::{anyhow, Result};
use clap::{arg, value_parser, ArgMatches, Command};
use std::process::Command as StdCommand;
use xline_client::{
    error::XlineClientError,
    types::watch::{WatchOptions, Watcher},
    Client,
};
use xlineapi::command::Command as XlineCommand;
use xlineapi::WatchResponse;

use crate::utils::printer::Printer;

/// Definition of `watch` command :
/// `WATCH [options] [key or prefix] [range_end] [--] [exec-command arg1 arg2 ...]`
pub(crate) fn command() -> Command {
    Command::new("watch")
        .about("Watches events stream on keys or prefixes")
        .arg(
            arg!([key] "The key or prefix, not needed in interactive mode")
                .required_unless_present("interactive"),
        )
        .arg(arg!([range_end] "The range end"))
        .arg(arg!(--prefix "Watch a prefix").conflicts_with("range_end"))
        .arg(arg!(--rev <REVISION> "Revision to start watching").value_parser(value_parser!(i64)))
        .arg(arg!(--pre_kv "Get the previous key-value pair before the event happens"))
        .arg(arg!(--progress_notify "Get periodic watch progress notification from server"))
        .arg(arg!(--interactive "Interactive mode"))
        .arg(
            arg!(<exec_command> "command to execute after -- ")
                .num_args(1..)
                .required(false)
                .last(true),
        )
}

/// a function that builds a watch request with existing fields
type BuildRequestFn = dyn Fn(Option<&str>) -> WatchOptions;

/// Build request from matches
pub(crate) fn build_request(matches: &ArgMatches) -> Box<BuildRequestFn> {
    let prefix = matches.get_flag("prefix");
    let rev = matches.get_one::<i64>("rev").map(Clone::clone);
    let pre_kv = matches.get_flag("pre_kv");
    let progress_notify = matches.get_flag("progress_notify");

    Box::new(move |range_end: Option<&str>| -> WatchOptions {
        let mut request = WatchOptions::default();

        if prefix {
            request = request.with_prefix();
        }
        if let Some(range_end_str) = range_end {
            request = request.with_range_end(range_end_str.as_bytes());
        }
        if let Some(rev) = rev {
            request = request.with_start_revision(rev);
        }
        if progress_notify {
            request = request.with_progress_notify();
        }
        if pre_kv {
            request = request.with_prev_kv();
        }

        request
    })
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let interactive = matches.get_flag("interactive");

    if interactive {
        exec_interactive(client, matches).await?;
    } else {
        exec_non_interactive(client, matches).await?;
    }

    Ok(())
}

/// Execute the command in non-interactive mode
async fn exec_non_interactive(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let key = matches.get_one::<String>("key").expect("required");
    let range_end = matches.get_one::<String>("range_end");
    let watch_options = build_request(matches)(range_end.map(String::as_str));

    // extract the command provided by user
    let command_to_execute: Vec<OsString> = matches
        .get_many::<String>("exec_command")
        .unwrap_or_default()
        .map(OsString::from)
        .collect();

    let (_watcher, mut stream) = client
        .watch_client()
        .watch(key.as_bytes(), Some(watch_options))
        .await?;
    while let Some(resp) = stream
        .message()
        .await
        .map_err(|e| XlineClientError::<XlineCommand>::WatchError(e.to_string()))?
    {
        resp.print();
        if !command_to_execute.is_empty() {
            execute_command_on_events(&command_to_execute, &resp)?;
        }
    }

    Ok(())
}

/// Execute user command for each event. We wanna support things like:
///
/// ```shell
/// ./xlinectl watch foo -- sh -c "env | grep ETCD_WATCH_"
/// ```
///
/// Expected output format:
/// ```plain
/// PUT
/// foo
/// bar
/// XLINE_WATCH_REVISION=11
/// XLINE_WATCH_KEY="foo"
/// XLINE_WATCH_EVENT_TYPE="PUT"
/// XLINE_WATCH_VALUE="bar"
/// ```
///
fn execute_command_on_events(command_to_execute: &[OsString], resp: &WatchResponse) -> Result<()> {
    let watch_revision = resp
        .header
        .as_ref()
        .map(|header| header.revision)
        .unwrap_or_default();

    for event in &resp.events {
        let kv = event.kv.as_ref().expect("expected key-value pair");
        let watch_key = String::from_utf8(kv.key.clone()).expect("expected valid UTF-8");
        let watch_value = String::from_utf8(kv.value.clone()).expect("expected valid UTF-8");
        let event_type = match event.r#type {
            0 => "PUT",
            1 => "DELETE",
            _ => "UNKNOWN",
        };

        let envs = HashMap::from([
            ("XLINE_WATCH_REVISION", watch_revision.to_string()),
            ("XLINE_WATCH_KEY", watch_key),
            ("XLINE_WATCH_EVENT_TYPE", event_type.to_owned()),
            ("XLINE_WATCH_VALUE", watch_value),
        ]);

        execute_inner(command_to_execute, envs)?;
    }

    Ok(())
}

/// Actual executor responsible for the user command,
/// command format: [exec-command arg1 arg2 ...]
#[allow(clippy::indexing_slicing)] // this is safe 'cause we always check non-empty.
#[allow(clippy::let_underscore_untyped)] // skip type annotation to make code clean, it's safe.
fn execute_inner(command: &[OsString], envs: HashMap<&str, String>) -> Result<()> {
    let mut cmd = StdCommand::new(&command[0]);

    // adding environment variables
    for (key, value) in envs {
        let _ = cmd.env(key, value);
    }

    // collecting the args
    if command.len() > 1 {
        let _ = cmd.args(&command[1..]);
    }
    // executing the command
    let output = cmd.output()?;
    if !output.status.success() {
        eprintln!("Command failed with status: {}", output.status);
        eprintln!("Error details: {}", String::from_utf8_lossy(&output.stderr));
    }
    println!("{}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

/// Execute the command in interactive mode
async fn exec_interactive(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req_builder = build_request(matches);
    let mut watcher: Option<Watcher> = None;

    loop {
        /// Macro for printing error and continue
        macro_rules! failed {
            ($line:expr) => {
                let err = anyhow!(format!("parse failed in: `{}`", $line));
                eprintln!("{err}");
                continue;
            };
        }

        let mut line = String::new();
        let _n = io::stdin().read_line(&mut line)?;
        let Some(args) = shlex::split(&line) else {
            failed!(line);
        };

        if args.len() < 2 {
            failed!(line);
        }

        let mut args = args.iter().map(String::as_str);

        #[allow(clippy::unwrap_used)] // checked above so it's safe to unwrap
        match args.next().unwrap() {
            "watch" => {
                let Some(key) = args.next() else {
                    failed!(line);
                };
                let watch_options = req_builder(args.next());
                let (new_watcher, mut stream) = client
                    .watch_client()
                    .watch(key.as_bytes(), Some(watch_options))
                    .await?;
                watcher = Some(new_watcher);
                let _handle = tokio::spawn(async move {
                    while let Some(resp) = stream.message().await? {
                        resp.print();
                    }
                    Ok::<(), XlineClientError<XlineCommand>>(())
                });
            }
            "cancel" => {
                let Some(watcher) = watcher.as_mut() else {
                    eprintln!("No currently active watch");
                    continue;
                };
                let cancel_result = if let Some(id_str) = args.next() {
                    let Ok(id) = id_str.parse() else {
                        failed!(line);
                    };
                    watcher.cancel_by_id(id)
                } else {
                    watcher.cancel()
                };

                if let Err(e) = cancel_result {
                    eprintln!("{e}");
                }
            }
            "progress" => {}
            _ => {
                failed!(line);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestCase {
        arg: Vec<&'static str>,
        key: String,
        req: Option<WatchOptions>,
    }

    impl TestCase {
        fn new(
            arg: Vec<&'static str>,
            key: impl Into<String>,
            req: Option<WatchOptions>,
        ) -> TestCase {
            TestCase {
                arg,
                key: key.into(),
                req,
            }
        }

        fn run_test(&self) {
            let matches = match command().try_get_matches_from(self.arg.clone()) {
                Ok(matches) => matches,
                Err(e) => {
                    assert!(
                        self.req.is_none(),
                        "the arg {:?} is invalid, err: {}",
                        self.arg,
                        e
                    );
                    return;
                }
            };
            let key = matches.get_one::<String>("key").expect("required");
            let range_end = matches.get_one::<String>("range_end");
            let req = build_request(&matches)(range_end.map(String::as_str));
            assert_eq!(key.to_owned(), self.key);
            assert_eq!(Some(req), self.req);
            // Extract the command to execute from the matches
            let command_to_execute: Vec<OsString> = matches
                .get_many::<String>("exec_command")
                .unwrap_or_default()
                .map(OsString::from)
                .collect();
            // Execute the user command upon receiving an event
            if !command_to_execute.is_empty() {
                // Mock environment variables to be passed to the command
                let mock_envs = HashMap::from([
                    ("XLINE_WATCH_REVISION", "11".to_owned()),
                    ("XLINE_WATCH_KEY", "mock_key".to_owned()),
                    ("XLINE_WATCH_EVENT_TYPE", "PUT".to_owned()),
                    ("XLINE_WATCH_VALUE", "mock_value".to_owned()),
                ]);
                // Here we ideally call a function to execute and capture the command output
                // Since we are testing, we can check the command formation INSTEAD OF actual execution.
                assert!(
                    execute_inner(&command_to_execute, mock_envs).is_ok(),
                    "Command execution failed"
                );
            }
        }
    }

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["watch", "key1", "key11"],
                "key1",
                Some(WatchOptions::default().with_range_end("key11")),
            ),
            TestCase::new(
                vec!["watch", "key1", "key11", "--rev", "100", "--pre_kv"],
                "key1",
                Some(
                    WatchOptions::default()
                        .with_range_end("key11")
                        .with_start_revision(100)
                        .with_prev_kv(),
                ),
            ),
            TestCase::new(
                vec!["watch", "key1", "--prefix", "--progress_notify"],
                "key1",
                Some(WatchOptions::default().with_prefix().with_progress_notify()),
            ),
            // newly added test case:
            // testing command `-- echo watch event received`
            TestCase::new(
                vec![
                    "watch",
                    "key1",
                    "--prefix",
                    "--progress_notify",
                    "--",
                    "echo",
                    "watch event received",
                ],
                "key1",
                Some(WatchOptions::default().with_prefix().with_progress_notify()),
            ),
            // newly added test case:
            // testing command `-- sh -c ls`
            TestCase::new(
                vec![
                    "watch",
                    "key1",
                    "--prefix",
                    "--progress_notify",
                    "--",
                    "sh",
                    "-c",
                    "ls",
                ],
                "key1",
                Some(WatchOptions::default().with_prefix().with_progress_notify()),
            ),
            // newly added test case:
            // testing command `-- sh -c "env | grep XLINE_WATCH_"`
            TestCase::new(
                vec![
                    "watch",
                    "key1",
                    "--prefix",
                    "--progress_notify",
                    "--",
                    "sh",
                    "-c",
                    "env | grep XLINE_WATCH_",
                ],
                "key1",
                Some(WatchOptions::default().with_prefix().with_progress_notify()),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
