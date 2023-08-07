//! An example to show how the errors are organized in `xline-client`
use curp::error::CommandProposeError;
use xline::storage::ExecuteError;
use xline_client::{
    error::{ClientError, Result},
    types::kv::PutRequest,
    Client, ClientOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let client = Client::connect(curp_members, ClientOptions::default())
        .await?
        .kv_client();

    // We try to update the key using its previous value.
    // It should return an error and it should be `key not found`
    // as we did not add it before.
    let resp = client
        .put(PutRequest::new("key", "").with_ignore_value(true))
        .await;
    let err = resp.unwrap_err();

    // We first match the client error
    let ClientError::ProposeError(pe) = err else {
        unreachable!("client.put should not return any errors other than PropseError, but it receives {err:?}")
    };
    // Then we match the inner error returned by the Curp server.
    // The command should failed at execution stage.
    let CommandProposeError::Execute(ee) = pe else {
        unreachable!("the propose error should be an Execute error, but it is {pe:?}")
    };

    assert!(
        matches!(ee, ExecuteError::KeyNotFound),
        "the return result should be `KeyNotFound`"
    );
    println!("got error: {ee}");

    Ok(())
}
