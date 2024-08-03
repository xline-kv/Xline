use curp_external_api::cmd::Command;

use super::Unary;

impl<C: Command> Unary<C> {
    fn fetch_membership(&self) {}
}
