use std::time::Duration;

use harness::fixture::FixtureCluster;
use harness::fixture::MakeGroupPlan;

use oceanraft::multiraft::ApplyMembershipChangeEvent;
use oceanraft::prelude::ConfChange;
use oceanraft::prelude::ConfChangeType;
use oceanraft::prelude::MembershipChangeRequest;
use oceanraft::prelude::SingleMembershipChange;
use oceanraft::util::TaskGroup;
use opentelemetry::global;

use protobuf::Message;

use tokio::sync::oneshot;
use tokio::time::timeout_at;
use tokio::time::Instant;

use tracing::info;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn enable_jager(enable: bool) {
    if !enable {
        return;
    }
    // install global collector configured based on RUST_LOG env var.
    // Allows you to pass along context (i.e., trace IDs) across services
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    // Sets up the machinery needed to export data to Jaeger
    // There are other OTel crates that provide pipelines for the vendors
    // mentioned earlier.
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("test_initial_leader_elect")
        .install_simple()
        .unwrap();

    // Create a tracing layer with the configured tracer
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // The SubscriberExt and SubscriberInitExt traits are needed to extend the
    // Registry to accept `opentelemetry (the OpenTelemetryLayer type).
    tracing_subscriber::registry()
        .with(opentelemetry)
        // Continue logging to stdout
        .with(fmt::Layer::default())
        .try_init()
        .unwrap();
}

async fn check_cc<F>(cluster: &mut FixtureCluster, wait_node_id: u64, timeout: Duration, check: F)
where
    F: FnOnce(&ApplyMembershipChangeEvent),
{
    let mut event =
        FixtureCluster::wait_membership_change_apply_event(cluster, wait_node_id, timeout)
            .await
            .unwrap();
    check(&event);
    event.done().await.unwrap();
    // TODO: as method called
    event.tx.map(|tx| tx.send(Ok(())));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_step() {
    enable_jager(true);

    let task_group = TaskGroup::new();
    // defer! {
    //     task_group.stop();
    //     // FIXME: use sync wait
    //     // task_group.joinner().awa`it;
    // }

    let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
    cluster.start();

    let group_id = 1;
    let node_id = 1;
    let mut plan = MakeGroupPlan {
        group_id,
        first_node_id: 1,
        replica_nums: 3,
    };
    let _ = cluster.make_group(&mut plan).await.unwrap();

    // triger group to leader election.
    cluster.campaign_group(node_id, plan.group_id).await;
    let _ = FixtureCluster::wait_leader_elect_event(&mut cluster, node_id)
        .await
        .unwrap();

    let (done_tx, done_rx) = oneshot::channel();
    // send a membership change request
    let node = cluster.nodes[0].clone();
    tokio::spawn(async move {
        for i in 1..3 {
            let mut change = SingleMembershipChange::default();
            change.set_change_type(ConfChangeType::AddNode);
            change.node_id = (i + 1) as u64;
            change.replica_id = (i + 1) as u64;
            let req = MembershipChangeRequest {
                group_id,
                changes: vec![change],
                replicas: vec![],
            };
            info!("start membership change request = {:?}", req);
            node.membership_change(req.clone()).await.unwrap();
            info!("membership change request done, request = {:?}", req);
        }
        done_tx.send(()).unwrap();
    });

    for i in 1..3 {
        let check_fn = |event: &ApplyMembershipChangeEvent| {
            let mut cc = ConfChange::default();
            cc.merge_from_bytes(event.entry.get_data()).unwrap();
            assert_eq!(
                cc.node_id,
                i + 1,
                "expected add node_id = {}, got {}",
                i + 1,
                cc.node_id
            );

            assert_eq!(
                cc.change_type(),
                ConfChangeType::AddNode,
                "expected ConfChangeType::AddNode, got {:?}",
                cc.change_type()
            );
        };
        check_cc(&mut cluster, 1, Duration::from_millis(100), check_fn).await;
        check_cc(&mut cluster, 2, Duration::from_millis(100), check_fn).await;
        check_cc(&mut cluster, 3, Duration::from_millis(100), check_fn).await;
    }

    match timeout_at(Instant::now() + Duration::from_millis(2000), done_rx).await {
        Err(_) => panic!("timeouted for wait all membership change complte"),
        Ok(_) => {}
    }
}
