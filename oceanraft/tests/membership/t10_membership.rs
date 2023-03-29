use std::mem::take;
use std::time::Duration;

use oceanraft::multiraft::ApplyMembership;
use oceanraft::prelude::ConfChange;
use oceanraft::prelude::ConfChangeType;
use oceanraft::prelude::ConfChangeV2;
use oceanraft::prelude::MembershipChangeData;
use oceanraft::prelude::SingleMembershipChange;
use oceanraft::prelude::StoreData;
use oceanraft::util::TaskGroup;
use protobuf::Message;
use tokio::sync::oneshot;
use tokio::time::timeout_at;
use tokio::time::Instant;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::rand_string;
use crate::fixtures::ClusterBuilder;
use crate::fixtures::FixtureCluster;
use crate::fixtures::MakeGroupPlan;
use crate::fixtures::MemStoreEnv;
use crate::fixtures::RockCluster;
use crate::fixtures::RockStoreEnv;

async fn check_cc<F>(
    cluster: &mut RockCluster,
    node_id: u64,
    wait_node_id: u64,
    timeout: Duration,
    check: F,
) where
    F: FnOnce(&ApplyMembership<()>),
{
    let event = FixtureCluster::wait_membership_change_apply_event(cluster, node_id, timeout)
        .await
        .expect(
            format!(
                "wait {} apply membership change event timeout on node = {}",
                wait_node_id, node_id
            )
            .as_str(),
        );
    check(&event);
    //    event.done().await.unwrap();
    // TODO: as method called
    // event.tx.map(|tx| tx.send(Ok(())));
}

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_single_step() {
    let task_group = TaskGroup::new();

    // start five nodes
    let nodes = 5;
    let mut rockstore_env = RockStoreEnv::<()>::new(nodes);
    let mut mem_env = MemStoreEnv::<StoreData, ()>::new(nodes);
    let mut cluster = ClusterBuilder::new(nodes)
        .election_ticks(2)
        .state_machines(rockstore_env.state_machines.clone())
        .storages(rockstore_env.storages.clone())
        .task_group(task_group.clone())
        .apply_rxs(take(&mut rockstore_env.rxs))
        .build()
        .await;

    let mut cluster2 = ClusterBuilder::new(nodes)
        .election_ticks(2)
        .state_machines(mem_env.state_machines.clone())
        .storages(mem_env.storages.clone())
        .task_group(task_group.clone())
        .apply_rxs(take(&mut mem_env.rxs))
        .build()
        .await;

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

    let leader = cluster.nodes[0].clone();

    // execute single step membership change for node 2 and replica 2 in group 1.
    let mut change = SingleMembershipChange::default();
    change.set_change_type(ConfChangeType::AddNode);
    change.node_id = 2;
    change.replica_id = 2;
    leader
        .membership_change(MembershipChangeData {
            group_id,
            term: 0, // not check
            changes: vec![change],
            replicas: vec![],
        })
        .await
        .unwrap();

    // let check_fn = |apply: &ApplyMembership<()>| {
    //     // let mut cc = ConfChange::default();
    //     // cc.merge_from_bytes(event.entry.get_data()).unwrap();
    //     let cc = &apply.conf_change.changes[0];
    //     assert_eq!(
    //         cc.node_id,
    //         2,
    //         "expected add node_id = {}, got {}",
    //         2,
    //         cc.node_id
    //     );

    //     assert_eq!(
    //         cc.change_type(),
    //         ConfChangeType::AddNode,
    //         "expected ConfChangeType::AddNode, got {:?}",
    //         cc.change_type()
    //     );
    // };
    // check_cc(
    //     &mut cluster,
    //     node_id,
    //     2,
    //     Duration::from_millis(1000),
    //     check_fn,
    // )
    // .await;

    // let check_fn = |apply: &ApplyMembership<()>| {
    //     // let mut cc = ConfChange::default();
    //     // cc.merge_from_bytes(event.entry.get_data()).unwrap();
    //     let cc = &apply.conf_change.changes[0];
    //     assert_eq!(
    //         cc.node_id,
    //         3,
    //         "expected add node_id = {}, got {}",
    //         3,
    //         cc.node_id
    //     );

    //     assert_eq!(
    //         cc.change_type(),
    //         ConfChangeType::AddNode,
    //         "expected ConfChangeType::AddNode, got {:?}",
    //         cc.change_type()
    //     );
    // };
    // check_cc(
    //     &mut cluster,
    //     node_id,
    //     3,
    //     Duration::from_millis(1000),
    //     check_fn,
    // )
    // .await;

    // The qurom of the cluster is 2. Therefore, at least two replicas must
    // be approved before adding replica 4, so we need ticks other follower to campaign.

    loop {
        if leader
            .can_submmit_membership_change(group_id)
            .await
            .unwrap()
        {
            // execute single step membership change for node 3 and replica 3 in group 1.
            let mut change = SingleMembershipChange::default();
            change.set_change_type(ConfChangeType::AddNode);
            change.node_id = 3;
            change.replica_id = 3;
            leader
                .membership_change(MembershipChangeData {
                    group_id,
                    term: 0, // not check
                    changes: vec![change],
                    replicas: vec![],
                })
                .await
                .unwrap();
        }

        if leader
            .can_submmit_membership_change(group_id)
            .await
            .unwrap()
        {
            let mut change = SingleMembershipChange::default();
            change.set_change_type(ConfChangeType::AddNode);
            change.node_id = 4;
            change.replica_id = 4;
            leader
                .membership_change(MembershipChangeData {
                    group_id,
                    term: 0, // not check
                    changes: vec![change],
                    replicas: vec![],
                })
                .await
                .unwrap();
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // rx.await.unwrap();

    // let (done_tx, done_rx) = oneshot::channel();
    // tokio::spawn(async move {
    //     for i in 1..5 {
    //         let mut change = SingleMembershipChange::default();
    //         change.set_change_type(ConfChangeType::AddNode);
    //         change.node_id = (i + 1) as u64;
    //         change.replica_id = (i + 1) as u64;

    //         node.membership_change(req.clone()).await.unwrap();
    //     }

    //     done_tx.send(()).unwrap();
    // });

    // for _ in 0..30 {
    //     cluster.tickers[2].non_blocking_tick();
    // }

    // TODO: in raft-rs, the snapshot never store any entries, so in
    // here case, the node_3 can't apply entries of index equal to [1, 2, 3, 4],
    // thus lead to testtrace can not pass.

    // FIXME: we need reimplementation raft-rs memory storage and by the way
    // execution patch upstream code for raft-rs instead of embeded in oceanraft.

    // check leader should recv all apply events.
    // for i in 1..2 {

    // }

    // match timeout_at(Instant::now() + Duration::from_millis(2000), done_rx).await {
    //     Err(_) => panic!("timeouted for wait all membership change complte"),
    //     Ok(_) => {}
    // }

    rockstore_env.destory();
}

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_joint_consensus() {
    let task_group = TaskGroup::new();
    // defer! {
    //     task_group.stop();
    //     // FIXME: use sync wait
    //     // task_group.joinner().awa`it;
    // }

    // let mut cluster = FixtureCluster::make(5, task_group.clone()).await;

    // cluster.start();
    let nodes = 5;
    let mut rockstore_env = RockStoreEnv::new(nodes);
    let mut cluster = ClusterBuilder::new(nodes)
        .election_ticks(2)
        .state_machines(rockstore_env.state_machines.clone())
        .storages(rockstore_env.storages.clone())
        .apply_rxs(take(&mut rockstore_env.rxs))
        .task_group(task_group.clone())
        .build()
        .await;

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

    // create joint consenus for membership change
    let mut changes = vec![];
    for i in 1..5 {
        let mut change = SingleMembershipChange::default();
        change.set_change_type(ConfChangeType::AddNode);
        change.node_id = (i + 1) as u64;
        change.replica_id = (i + 1) as u64;
        changes.push(change);
    }
    let rx = cluster.nodes[0]
        .membership_change_non_block(MembershipChangeData {
            group_id,
            changes,
            term: 0, // no check term
            replicas: vec![],
        })
        .unwrap();

    let check_fn = |apply: &ApplyMembership<()>| {
        let changes = apply
            .conf_change
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

    // wait joint consensus apply and check
    check_cc(&mut cluster, 1, 1, Duration::from_millis(100), check_fn).await;
    match timeout_at(Instant::now() + Duration::from_millis(2000), rx).await {
        Err(_) => panic!("timeouted for wait all membership change complte"),
        Ok(_) => {}
    }

    // send heartbeats to new replica and creating it.
    for _ in 0..10 {
        cluster.tickers[0].tick().await;
    }

    // wait new replicas apply membership change
    check_cc(&mut cluster, 2, 2, Duration::from_millis(100), check_fn).await;
    check_cc(&mut cluster, 3, 3, Duration::from_millis(100), check_fn).await;
    check_cc(&mut cluster, 4, 4, Duration::from_millis(100), check_fn).await;
    check_cc(&mut cluster, 5, 5, Duration::from_millis(100), check_fn).await;
}

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_remove() {
    let task_group = TaskGroup::new();
    // let mut cluster = FixtureCluster::make(5, task_group.clone()).await;
    // cluster.start();

    let nodes = 5;
    let mut rockstore_env = RockStoreEnv::new(nodes);
    let mut cluster = ClusterBuilder::new(nodes)
        .election_ticks(2)
        .state_machines(rockstore_env.state_machines.clone())
        .storages(rockstore_env.storages.clone())
        .task_group(task_group.clone())
        .apply_rxs(take(&mut rockstore_env.rxs))
        .build()
        .await;

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
        let req = MembershipChangeData {
            group_id,
            changes,
            term: 0, // no check term
            replicas: vec![],
        };

        node.membership_change(req.clone()).await.unwrap();
        done_tx.send(()).unwrap();
    });

    let check_fn = |apply: &ApplyMembership<()>| {
        let changes = apply
            .conf_change
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
