use std::time::Duration;

use harness::fixture::MakeGroupPlan;
use harness::fixture::MakeGroupPlanStatus;
use oceanraft::prelude::ReplicaDesc;
use opentelemetry::global;
use tokio::time::timeout_at;
use tokio::time::Instant;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

use harness::fixture::FixtureCluster;

use oceanraft::util::TaskGroup;

// async fn check_group_should_elected(
//     cluster: &mut FixtureCluster,
//     group_id: u64,
//     expected_leaeder_id: u64,
// ) {
//     let nodes = cluster.groups.get(&group_id).unwrap().clone();
//     for node_id in nodes {
//         check_replica_should_elected(cluster, node_id, group_id, expected_leaeder_id).await;
//     }
// }

async fn check_replica_should_elected(
    cluster: &mut FixtureCluster,
    node_id: u64,
    group_id: u64,
    // replica_desc: ReplicaDesc,
    expected_leaeder_id: u64,
) {
    // trigger an election for the replica in the group of the node where leader nodes.
    // self.trigger_elect(node_index, group_id).await;
    cluster.campaign_group(node_id, group_id).await;
    let election = FixtureCluster::wait_leader_elect_event(cluster, node_id)
        .await
        .unwrap();

    assert_eq!(election.group_id, group_id);
    assert_eq!(election.leader_id, expected_leaeder_id);
    let replica_desc = cluster.replica_desc(node_id, group_id).await;
    assert_eq!(election.replica_id, replica_desc.replica_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_initial_leader_elect() {
    // // install global collector configured based on RUST_LOG env var.
    // // Allows you to pass along context (i.e., trace IDs) across services
    // global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    // // Sets up the machinery needed to export data to Jaeger
    // // There are other OTel crates that provide pipelines for the vendors
    // // mentioned earlier.
    // let tracer = opentelemetry_jaeger::new_agent_pipeline()
    //     .with_service_name("test_initial_leader_elect")
    //     .install_simple()
    //     .unwrap();

    // // Create a tracing layer with the configured tracer
    // let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // // The SubscriberExt and SubscriberInitExt traits are needed to extend the
    // // Registry to accept `opentelemetry (the OpenTelemetryLayer type).
    // tracing_subscriber::registry()
    //     .with(opentelemetry)
    //     // Continue logging to stdout
    //     .with(fmt::Layer::default())
    //     .try_init()
    //     .unwrap();

    for i in 0..3 {
        let task_group = TaskGroup::new();
        let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
        cluster.start();
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
    }
}
