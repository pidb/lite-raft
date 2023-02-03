use std::time::Duration;

use harness::fixture::MakeGroupPlan;
use oceanraft::multiraft::ApplyNormalEvent;
use oceanraft::multiraft::Error;
use oceanraft::multiraft::Event;
use oceanraft::multiraft::ProposalError;
use oceanraft::multiraft::RaftGroupError;
use oceanraft::prelude::AppWriteRequest;
use opentelemetry::global;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::time::timeout_at;
use tokio::time::Instant;
use tracing::info;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

use harness::fixture;
use harness::fixture::FixtureCluster;

use oceanraft::util::defer;
use oceanraft::util::TaskGroup;

/// Write data to raft. return a onshot::Receiver to recv apply result.
pub fn write_command(
    cluster: &mut FixtureCluster,
    group_id: u64,
    node_id: u64,
    data: Vec<u8>,
) -> oneshot::Receiver<Result<(), Error>> {
    let request = AppWriteRequest {
        group_id,
        term: 0,
        context: vec![],
        data,
    };
    cluster.nodes[node_id as usize - 1].async_write(request)
}

/// The test consensus group does not have a leader or the leader is
/// submitting a proposal during an election.
#[tokio::test(flavor = "multi_thread")]
async fn test_no_leader() {
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

    let task_group = TaskGroup::new();
    // defer! {
    //     task_group.stop();
    //     // FIXME: use sync wait
    //     // task_group.joinner().awa`it;
    // }

    let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
    cluster.start();

    let mut plan = MakeGroupPlan {
        group_id: 1,
        first_node_id: 1,
        replica_nums: 3,
    };
    let _ = cluster.make_group(&mut plan).await.unwrap();

    // all replicas should no elected.
    for i in 0..3 {
        let node_id = i + 1;
        if let Ok(ev) = FixtureCluster::wait_leader_elect_event(&mut cluster, node_id).await {
            panic!("expected no leader elected, got {:?}", ev);
        }
    }

    for i in 0..3 {
        let node_id = i + 1;
        let data = "data".as_bytes().to_vec();
        let res = write_command(&mut cluster, plan.group_id, node_id, data)
            .await
            .unwrap();
        let expected_err = Error::Proposal(ProposalError::NotLeader {
            group_id: plan.group_id,
            replica_id: i + 1,
        });
        match res {
            Ok(_) => panic!("expected {:?}, got {:?}", expected_err, res),
            Err(err) => assert_eq!(expected_err, err),
        }
    }

    // no ticker and campaign group,

    // let bad_group_id = 4;
    // let rx = cluster.write_command(bad_group_id, 0, "data".as_bytes().to_vec());
    // let expected_err = Error::RaftGroup(RaftGroupError::NotExist(1, 4));
    // match rx.await.unwrap() {
    //     Ok(_) => panic!("expected error = {:?}, got Ok(())", expected_err),
    //     Err(err) => assert_eq!(err, expected_err),
    // }
}

//

/// The test consensus group does not have a leader or the leader is
/// submitting a proposal during an election.
#[tokio::test(flavor = "multi_thread")]
async fn test_bad_group() {
    let task_group = TaskGroup::new();

    let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
    cluster.start();

    let mut plan = MakeGroupPlan {
        group_id: 1,
        first_node_id: 1,
        replica_nums: 3,
    };
    let _ = cluster.make_group(&mut plan).await.unwrap();

    cluster.campaign_group(1, plan.group_id).await;

    for i in 1..3 {
        let node_id = i + 1;
        let data = "data".as_bytes().to_vec();
        let res = write_command(&mut cluster, plan.group_id, node_id, data)
            .await
            .unwrap();
        let expected_err = Error::Proposal(ProposalError::NotLeader {
            group_id: plan.group_id,
            replica_id: i + 1,
        });
        println!("{:?}", res);
        match res {
            Ok(_) => panic!("expected {:?}, got {:?}", expected_err, res),
            Err(err) => assert_eq!(expected_err, err),
        }
    }
}