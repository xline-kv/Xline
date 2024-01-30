use anyhow::{bail, Result};
use clap::ArgMatches;

/// Parser user name and password
pub(crate) fn parse_user(matches: &ArgMatches) -> Result<Option<(String, String)>> {
    let user_pw = matches.get_one::<String>("user");
    if let Some(user_pw) = user_pw {
        // try get password from `--user` first
        let mut split = user_pw.split(':');
        let user = split.next().map_or_else(
            || unreachable!("the string should exist"),
            ToOwned::to_owned,
        );
        let passwd = split.next().map(ToOwned::to_owned);
        let password_opt = matches.get_one::<String>("password");

        if let Some(passwd) = passwd {
            if password_opt.is_some() {
                bail!(
                    "Password already set in `--user`, please remove it from `--password`"
                        .to_owned(),
                );
            }
            Ok(Some((user, passwd)))
        } else {
            let Some(password) = password_opt else {
                    bail!("Password not set in `--user`, please set it in `--password`"
                        .to_owned());
                };
            Ok(Some((user, password.clone())))
        }
    } else {
        Ok(None)
    }
}
