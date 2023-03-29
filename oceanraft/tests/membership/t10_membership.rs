use std::mem::take;
use std::time::Duration;

use oceanraft::multiraft::storage::MultiRaftStorage;
use oceanraft::multiraft::storage::RaftStorageReader;
use oceanraft::multiraft::Apply;
use oceanraft::multiraft::ApplyMembership;
use oceanraft::prelude::ConfChangeType;
use oceanraft::prelude::ConfState;
use oceanraft::prelude::MembershipChangeData;
use oceanraft::prelude::SingleMembershipChange;
use oceanraft::prelude::StoreData;
use oceanraft::util::TaskGroup;
use tokio::sync::oneshot;
use tokio::time::timeout_at;
use tokio::time::Instant;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::rand_string;
use crate::fixtures::ClusterBuilder;
use crate::fixtures::FixtureCluster;
use crate::fixtures::MakeGroupPlan;
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

    // execute single step membership change from 3..5
    for i in 3..=5 {
        loop {
            if leader
                .can_submmit_membership_change(group_id)
                .await
                .unwrap()
            {
                let mut change = SingleMembershipChange::default();
                change.set_change_type(ConfChangeType::AddNode);
                change.node_id = i;
                change.replica_id = i;
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
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    let expected = ConfState {
        voters: vec![1, 2, 3, 4, 5],
        learners: vec![],
        voters_outgoing: vec![],
        learners_next: vec![],
        auto_leave: false,
    };

    // wait all nodes apply conf state
    for (i, rx) in cluster.apply_events.iter_mut().enumerate() {
        let rx = rx.as_mut().unwrap();
        loop {
            cluster.tickers[i].tick().await;
            match rx.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => unreachable!(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Ok(applys) => {
                    for apply in applys {
                        match apply {
                            Apply::Membership(apply) => {
                                let mut conf_state = apply.conf_state;
                                conf_state.voters.sort();
                                if conf_state == expected {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    // check leader conf_state in storage.
    let store = &cluster.storages[0]
        .group_storage(group_id, 1)
        .await
        .unwrap();
    let rs = store.initial_state().unwrap();
    let mut conf_state = rs.conf_state;
    conf_state.voters.sort();
    assert_eq!(expected, conf_state);

    // TODO: check all nodes conf_state in rock state machine.
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
    let mut rockstore_env = RockStoreEnv::<()>::new(nodes);
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

    // let check_fn = |apply: &ApplyMembership<()>| {
    //     let changes = apply
    //         .conf_change
    //         .changes
    //         .iter()
    //         .map(|change| (change.node_id, change.get_change_type()))
    //         .collect::<Vec<_>>();
    //     assert_eq!(
    //         changes,
    //         vec![
    //             (2, ConfChangeType::AddNode),
    //             (3, ConfChangeType::AddNode),
    //             (4, ConfChangeType::AddNode),
    //             (5, ConfChangeType::AddNode),
    //         ]
    //     );
    // };

    // wait joint consensus apply and check
    // check_cc(&mut cluster, 1, 1, Duration::from_millis(100), check_fn).await;
    match timeout_at(Instant::now() + Duration::from_millis(2000), rx).await {
        Err(_) => panic!("timeouted for wait all membership change complte"),
        Ok(_) => {}
    }

    // send heartbeats to new replica and creating it.
    for _ in 0..10 {
        cluster.tickers[0].tick().await;
    }

    // wait new replicas apply membership change
    // check_cc(&mut cluster, 2, 2, Duration::from_millis(100), check_fn).await;
    // check_cc(&mut cluster, 3, 3, Duration::from_millis(100), check_fn).await;
    // check_cc(&mut cluster, 4, 4, Duration::from_millis(100), check_fn).await;
    // check_cc(&mut cluster, 5, 5, Duration::from_millis(100), check_fn).await;
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
    let mut rockstore_env = RockStoreEnv::<()>::new(nodes);
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

    // let check_fn = |apply: &ApplyMembership<()>| {
    //     let changes = apply
    //         .conf_change
    //         .changes
    //         .iter()
    //         .map(|change| (change.node_id, change.get_change_type()))
    //         .collect::<Vec<_>>();
    //     assert_eq!(
    //         changes,
    //         vec![
    //             (4, ConfChangeType::RemoveNode),
    //             (5, ConfChangeType::RemoveNode),
    //         ]
    //     );
    // };

    // check_cc(&mut cluster, 1, 1, Duration::from_millis(100), check_fn).await;
    // check_cc(&mut cluster, 2, 2, Duration::from_millis(100), check_fn).await;
    // check_cc(&mut cluster, 3, 3, Duration::from_millis(100), check_fn).await;

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
