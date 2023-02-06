use std::time::Duration;

use harness::fixture;
use harness::fixture::FixtureCluster;
use harness::fixture::MakeGroupPlan;

use oceanraft::multiraft::ApplyMembershipChangeEvent;
use oceanraft::prelude::ConfChange;
use oceanraft::prelude::ConfChangeType;
use oceanraft::prelude::ConfChangeV2;
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

async fn check_cc<F>(
    cluster: &mut FixtureCluster,
    node_id: u64,
    wait_node_id: u64,
    timeout: Duration,
    check: F,
) where
    F: FnOnce(&ApplyMembershipChangeEvent),
{
    let mut event = FixtureCluster::wait_membership_change_apply_event(cluster, node_id, timeout)
        .await
        .expect(
            format!(
                "wait {} apply membership change event timeout on node = {}",
                wait_node_id, node_id
            )
            .as_str(),
        );
    check(&event);
    event.done().await.unwrap();
    // TODO: as method called
    event.tx.map(|tx| tx.send(Ok(())));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_step() {
    enable_jager(false);

    let task_group = TaskGroup::new();
    // defer! {
    //     task_group.stop();
    //     // FIXME: use sync wait
    //     // task_group.joinner().awa`it;
    // }

    let mut cluster = FixtureCluster::make(5, task_group.clone()).await;
    cluster.start();

    let group_id = 1;
    let node_id = 1;
    let mut plan = MakeGroupPlan {
        group_id,
        first_node_id: 1,
        replica_nums: 1,
    };
    let _ = cluster.make_group(&mut plan).await.unwrap();

    // triger group to leader election.
    cluster.campaign_group(node_id, plan.group_id).await;
    let _ = FixtureCluster::wait_leader_elect_event(&mut cluster, node_id)
        .await
        .unwrap();

    let (done_tx, done_rx) = oneshot::channel();
    let node = cluster.nodes[0].clone();
    tokio::spawn(async move {
        for i in 1..5 {
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

    for _ in 0..10 {
        cluster.tickers[2].non_blocking_tick();
    }

    // TODO: in raft-rs, the snapshot never store any entries, so in
    // here case, the node_3 can't apply entries of index equal to [1, 2, 3, 4],
    // thus lead to testtrace can not pass.

    // FIXME: we need reimplementation raft-rs memory storage and by the way
    // execution patch upstream code for raft-rs instead of embeded in oceanraft.

    // check leader should recv all apply events.
    for i in 1..5 {
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
        check_cc(
            &mut cluster,
            node_id,
            i + 1,
            Duration::from_millis(5000),
            check_fn,
        )
        .await;
    }

    match timeout_at(Instant::now() + Duration::from_millis(2000), done_rx).await {
        Err(_) => panic!("timeouted for wait all membership change complte"),
        Ok(_) => {}
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_joint_consensus() {
    enable_jager(false);

    let task_group = TaskGroup::new();
    // defer! {
    //     task_group.stop();
    //     // FIXME: use sync wait
    //     // task_group.joinner().awa`it;
    // }

    let mut cluster = FixtureCluster::make(5, task_group.clone()).await;
    cluster.start();

    let group_id = 1;
    let node_id = 1;
    let mut plan = MakeGroupPlan {
        group_id,
        first_node_id: 1,
        replica_nums: 1,
    };
    let _ = cluster.make_group(&mut plan).await.unwrap();

    // triger group to leader election.
    cluster.campaign_group(node_id, plan.group_id).await;
    let _ = FixtureCluster::wait_leader_elect_event(&mut cluster, node_id)
        .await
        .unwrap();

    let (done_tx, done_rx) = oneshot::channel();
    let node = cluster.nodes[0].clone();
    tokio::spawn(async move {
        let mut changes = vec![];
        for i in 1..5 {
            let mut change = SingleMembershipChange::default();
            change.set_change_type(ConfChangeType::AddNode);
            change.node_id = (i + 1) as u64;
            change.replica_id = (i + 1) as u64;
            changes.push(change);
        }
        let req = MembershipChangeRequest {
            group_id,
            changes,
            replicas: vec![],
        };

        println!("start membership change request = {:?}", req);
        node.membership_change(req.clone()).await.unwrap();
        println!("membership change request done, request = {:?}", req);
        done_tx.send(()).unwrap();
    });

    let check_fn = |event: &ApplyMembershipChangeEvent| {
        let mut cc = ConfChangeV2::default();
        cc.merge_from_bytes(&event.entry.data).unwrap();
        let changes = cc
            .changes
            .iter()
            .map(|change| (change.node_id, change.get_change_type()))
            .collect::<Vec<_>>();
        assert_eq!(
            changes,
            vec![
                (2, ConfChangeType::AddNode),
                (3, ConfChangeType::AddNode),
                (4, ConfChangeType::AddNode),
                (5, ConfChangeType::AddNode),
            ]
        );
    };

    check_cc(&mut cluster, 1, 1, Duration::from_millis(100), check_fn).await;

    // TODO: check all replicas inner states
    for i in 0..5 {
        for _ in 0..10 {
            cluster.tickers[i].tick().await;
        }
    }

    match timeout_at(Instant::now() + Duration::from_millis(2000), done_rx).await {
        Err(_) => panic!("timeouted for wait all membership change complte"),
        Ok(_) => {}
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove() {
    enable_jager(false);

    let task_group = TaskGroup::new();
    let mut cluster = FixtureCluster::make(5, task_group.clone()).await;
    cluster.start();

    let group_id = 1;
    let node_id = 1;
    let plan = MakeGroupPlan {
        group_id,
        first_node_id: 1,
        replica_nums: 5,
    };
    let _ = cluster.make_group(&plan).await.unwrap();

  
    // triger group to leader election.
    cluster.campaign_group(node_id, plan.group_id).await;
    let _ = FixtureCluster::wait_leader_elect_event(&mut cluster, node_id)
        .await
        .unwrap();


      // Let's submit some commands
    //   let node = cluster.nodes[0].clone();
    //   tokio::spawn(async move {
    //       let rx = FixtureCluster::write_command(&node, plan.group_id, "data".as_bytes().to_vec());
    //       let _ = rx.await.unwrap();
    //   });
  
    // let res = FixtureCluster::wait_for_command_apply(&mut cluster, node_id, Duration::from_millis(100)).await;
    // println!("res = {:?}", res);

    let (done_tx, done_rx) = oneshot::channel();
    let node = cluster.nodes[0].clone();
    // remove 4, 5 nodes
    tokio::spawn(async move {
        let mut changes = vec![];
        for i in 3..5 {
            let mut change = SingleMembershipChange::default();
            change.set_change_type(ConfChangeType::RemoveNode);
            change.node_id = (i + 1) as u64;
            change.replica_id = (i + 1) as u64;
            changes.push(change);
        }
        let req = MembershipChangeRequest {
            group_id,
            changes,
            replicas: vec![],
        };

        println!("start membership change request = {:?}", req);
        node.membership_change(req.clone()).await.unwrap();
        println!("membership change request done, request = {:?}", req);
        done_tx.send(()).unwrap();
    });

    let check_fn = |event: &ApplyMembershipChangeEvent| {
        let mut cc = ConfChangeV2::default();
        cc.merge_from_bytes(&event.entry.data).unwrap();
        let changes = cc
            .changes
            .iter()
            .map(|change| (change.node_id, change.get_change_type()))
            .collect::<Vec<_>>();
        assert_eq!(
            changes,
            vec![
                (4, ConfChangeType::RemoveNode),
                (5, ConfChangeType::RemoveNode),
            ]
        );
    };

    check_cc(&mut cluster, 1, 1, Duration::from_millis(100), check_fn).await;
    check_cc(&mut cluster, 2, 2, Duration::from_millis(100), check_fn).await;
    check_cc(&mut cluster, 3, 3, Duration::from_millis(100), check_fn).await;

    // TODO: check all replicas inner states
    for i in 0..3 {
        for _ in 0..10 {
            cluster.tickers[i].tick().await;
        }
    }

    // TODO: submmit command to bad node

    match timeout_at(Instant::now() + Duration::from_millis(2000), done_rx).await {
        Err(_) => panic!("timeouted for wait all membership change complte"),
        Ok(_) => {}
    }
}
