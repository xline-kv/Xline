/// Generate match handler for each command
#[macro_export]
macro_rules! handle_matches {
    ($matches:ident, $client:ident, { $($cmd:ident),* }) => {
        match $matches.subcommand() {
            $(Some((stringify!($cmd), sub_matches)) => {
                $cmd::execute(&mut $client, sub_matches).await?;
            })*
            _ => {},
        }
    };
}

///  Generate `TestCase` struct
#[macro_export]
macro_rules! test_case_struct {
    ($req:ident) => {
        struct TestCase {
            arg: Vec<&'static str>,
            req: Option<$req>,
        }

        impl TestCase {
            fn new(arg: Vec<&'static str>, req: Option<$req>) -> TestCase {
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
                let req = build_request(&matches);
                assert_eq!(Some(req), self.req);
            }
        }
    };
}
