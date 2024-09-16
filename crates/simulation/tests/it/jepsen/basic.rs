use jepsen_rs::{
    client::{Client, JepsenClient},
    generator::{controller::GeneratorGroupStrategy, elle_rw::ElleRwGenerator, GeneratorGroup},
    op::Op,
};
use tracing::info;

use simulation::xline_group::XlineGroup;

#[test]
fn basic_test() {
    let mut rt = madsim::runtime::Runtime::new();
    rt.set_allow_system_thread(true);

    rt.block_on(async move {
        let group = XlineGroup::new(5).await;
        let sim_client = group.client().await;
        let jepsen_client = JepsenClient::new(sim_client, ElleRwGenerator::new().unwrap());
        let jepsen_client = Box::leak(jepsen_client.into());
        info!("basic_test: client created");

        // get generators, transform and merge them
        let g1 = jepsen_client
            .new_generator(100)
            .filter(|o| matches!(o, Op::Txn(txn) if txn.len() == 1))
            .await;
        let g2 = jepsen_client.new_generator(50);
        let g3 = jepsen_client.new_generator(50);
        info!("intergration_test: generators created");
        let gen_g = GeneratorGroup::new([g1, g2, g3])
            .with_strategy(GeneratorGroupStrategy::RoundRobin(usize::MAX));
        info!("generator group created");
        let res = jepsen_client
            .run(gen_g)
            .await
            .unwrap_or_else(|e| panic!("{}", e));
        println!("history checked result: {:?}", res);
    })
}
