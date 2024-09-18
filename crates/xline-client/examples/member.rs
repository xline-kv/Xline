use anyhow::Result;
use xline_client::{clients::Node, Client, ClientOptions};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let client = Client::connect(curp_members, ClientOptions::default())
        .await?
        .member_client();

    let node1 = Node::new("n1", vec!["10.0.0.4:2380"], vec!["10.0.0.4.2379"]);
    let node2 = Node::new("n2", vec!["10.0.0.5:2380"], vec!["10.0.0.5.2379"]);
    let ids = client.add_learner(vec![node1, node2]).await?;

    println!("got node ids of new learners: {ids:?}");

    // Remove the previously added learners
    client.remove_learner(ids).await?;

    Ok(())
}
