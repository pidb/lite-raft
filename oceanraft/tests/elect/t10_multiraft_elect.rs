use std::mem::take;
use std::time::Duration;

use oceanraft::task_group::TaskGroup;
use tokio::time::timeout_at;
use tokio::time::Instant;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::ClusterBuilder;
use crate::fixtures::MakeGroupPlan;
use crate::fixtures::MemStoreCluster;
use crate::fixtures::MemStoreEnv;

async fn check_replica_should_elected(
    cluster: &mut MemStoreCluster,
    node_id: u64,
    group_id: u64,
    expected_leaeder_id: u64,
) {
    // trigger an election for the replica in the group of the node where leader nodes.
    cluster.campaign_group(node_id, group_id).await;
    let election = cluster.wait_leader_elect_event(node_id).await.unwrap();

    assert_eq!(election.group_id, group_id);
    assert_eq!(election.leader_id, expected_leaeder_id);
    let replica_desc = cluster.replica_desc(node_id, group_id).await;
    assert_eq!(election.replica_id, replica_desc.replica_id);
}

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_initial_leader_elect() {
    for i in 0..3 {
        let nodes = 3;
        // let rockstore_env = RockStorageEnv::new(nodes);
        let mut env = MemStoreEnv::new(nodes);
        let task_group = TaskGroup::new();
        let mut cluster = ClusterBuilder::new(nodes)
            .election_ticks(2)
            .task_group(task_group.clone())
            .state_machines(env.state_machines.clone())
            .storages(env.storages.clone())
            .apply_rxs(take(&mut env.rxs))
            .build()
            .await;

        let mut plan = MakeGroupPlan {
            group_id: 1,
            first_node_id: 1,
            replica_nums: 3,
        };

        let _ = cluster.make_group(&mut plan).await.unwrap();
        check_replica_should_elected(&mut cluster, i + 1, plan.group_id, i + 1).await;

        if let Err(_) = timeout_at(
            Instant::now() + Duration::from_millis(100),
            cluster.transport.stop_all(),
        )
        .await
        {
            panic!("wait stop transport error")
        }
        cluster.transport.stop_all().await.unwrap();
        task_group.stop();
        if let Err(_) = timeout_at(
            Instant::now() + Duration::from_millis(100),
            task_group.joinner(),
        )
        .await
        {
            panic!("wait cluster taks stop error")
        }
        // rockstore_env.destory();
    }
}
