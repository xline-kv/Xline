use std::io;

use anyhow::{anyhow, Result};
use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{
    error::XlineClientError,
    types::watch::{WatchRequest, Watcher},
    Client,
};
use xlineapi::command::Command as XlineCommand;

use crate::utils::printer::Printer;

/// Definition of `watch` command
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
}

/// a function that builds a watch request with existing fields
type BuildRequestFn = dyn Fn(&str, Option<&str>) -> WatchRequest;

/// Build request from matches
pub(crate) fn build_request(matches: &ArgMatches) -> Box<BuildRequestFn> {
    let prefix = matches.get_flag("prefix");
    let rev = matches.get_one::<i64>("rev").map(Clone::clone);
    let pre_kv = matches.get_flag("pre_kv");
    let progress_notify = matches.get_flag("progress_notify");

    Box::new(move |key: &str, range_end: Option<&str>| -> WatchRequest {
        let mut request = WatchRequest::new(key.as_bytes());

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
        let key = matches.get_one::<String>("key").expect("required");
        let range_end = matches.get_one::<String>("range_end");
        let request = build_request(matches)(key, range_end.map(String::as_str));

        let (_watcher, mut stream) = client.watch_client().watch(request).await?;
        while let Some(resp) = stream
            .message()
            .await
            .map_err(|e| XlineClientError::<XlineCommand>::WatchError(e.to_string()))?
        {
            resp.print();
        }
    }

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
                let request = req_builder(key, args.next());
                let (new_watcher, mut stream) = client.watch_client().watch(request).await?;
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
        req: Option<WatchRequest>,
    }

    impl TestCase {
        fn new(arg: Vec<&'static str>, req: Option<WatchRequest>) -> TestCase {
            TestCase { arg, req }
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
            let req = build_request(&matches)(key, range_end.map(String::as_str));
            assert_eq!(Some(req), self.req);
        }
    }

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["watch", "key1", "key11"],
                Some(WatchRequest::new("key1").with_range_end("key11")),
            ),
            TestCase::new(
                vec!["watch", "key1", "key11", "--rev", "100", "--pre_kv"],
                Some(
                    WatchRequest::new("key1")
                        .with_range_end("key11")
                        .with_start_revision(100)
                        .with_prev_kv(),
                ),
            ),
            TestCase::new(
                vec!["watch", "key1", "--prefix", "--progress_notify"],
                Some(
                    WatchRequest::new("key1")
                        .with_prefix()
                        .with_progress_notify(),
                ),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
