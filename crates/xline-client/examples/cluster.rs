use anyhow::Result;
use xline_client::{
    types::cluster::{
        MemberAddRequest, MemberListRequest, MemberPromoteRequest, MemberRemoveRequest,
        MemberUpdateRequest,
    },
    Client, ClientOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let mut client = Client::connect(curp_members, ClientOptions::default())
        .await?
        .cluster_client();

    // send a linearizable member list request
    let resp = client.member_list(MemberListRequest::new(true)).await?;
    println!("members: {:?}", resp.members);

    // whether the added member is a learner.
    // the learner does not participate in voting and will only catch up with the progress of the leader.
    let is_learner = true;

    // add a normal node into the cluster
    let resp = client
        .member_add(MemberAddRequest::new(
            vec!["127.0.0.1:2379".to_owned()],
            is_learner,
        ))
        .await?;
    let added_member = resp.member.unwrap();
    println!("members: {:?}, added: {}", resp.members, added_member.id);

    if is_learner {
        // promote the learner to a normal node
        let resp = client
            .member_promote(MemberPromoteRequest::new(added_member.id))
            .await?;
        println!("members: {:?}", resp.members);
    }

    // update the peer_ur_ls of the added member if the network topology has changed.
    let resp = client
        .member_update(MemberUpdateRequest::new(
            added_member.id,
            vec!["127.0.0.2:2379".to_owned()],
        ))
        .await?;
    println!("members: {:?}", resp.members);

    // remove the member from the cluster if it is no longer needed.
    let resp = client
        .member_remove(MemberRemoveRequest::new(added_member.id))
        .await?;
    println!("members: {:?}", resp.members);

    Ok(())
}
