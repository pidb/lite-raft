use std::mem::take;
use std::time::Duration;

use oceanraft::multiraft::storage::MultiRaftStorage;
use oceanraft::multiraft::storage::RaftStorageReader;
use oceanraft::multiraft::Apply;
use oceanraft::prelude::ConfChangeTransition;
use oceanraft::prelude::ConfChangeType;
use oceanraft::prelude::ConfState;
use oceanraft::prelude::MembershipChangeData;
use oceanraft::prelude::SingleMembershipChange;
use oceanraft::prelude::StoreData;
use oceanraft::util::TaskGroup;
use tokio::time::sleep;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::rand_string;
use crate::fixtures::ClusterBuilder;
use crate::fixtures::FixtureCluster;
use crate::fixtures::MakeGroupPlan;
use crate::fixtures::RockStoreEnv;

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
            transition: 0,
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
                        transition: 0,
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

    // TODO: wait all nodes apply conf state, this need refactor
    // heartbeat by heartbeat compensation

    // check leader conf_state in storage.
    let store = &cluster.storages[0]
        .group_storage(group_id, 1)
        .await
        .unwrap();
    let rs = store.initial_state().unwrap();
    let mut conf_state = rs.conf_state;
    conf_state.voters.sort();
    assert_eq!(expected, conf_state);

    // check leader node conf_state in rock state machine.
    let mut conf_state = rockstore_env.rock_kv_stores[0]
        .get_conf_state(group_id)
        .unwrap();
    conf_state.voters.sort();
    assert_eq!(expected, conf_state);
    rockstore_env.destory();
}

/// Test initial configuration for joint consensus.
#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_initial_joint_consensus() {
    let task_group = TaskGroup::new();

    // start five nodes.
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

    // create leader at node 1.
    let group_id = 1;
    let node_id = 1;
    let mut plan = MakeGroupPlan {
        group_id,
        first_node_id: 1,
        replica_nums: 1,
    };
    let _ = cluster.make_group(&mut plan).await.unwrap();
    cluster.campaign_group(node_id, plan.group_id).await;
    let _ = FixtureCluster::wait_leader_elect_event(&mut cluster, node_id)
        .await
        .unwrap();

    let leader = &cluster.nodes[0];

    // create joint consenus to add nodes 2..5.
    let mut changes = vec![];
    for next_id in 2..=5 {
        let mut change = SingleMembershipChange::default();
        change.set_change_type(ConfChangeType::AddNode);
        change.node_id = next_id;
        change.replica_id = next_id;
        changes.push(change);
    }
    let mut change = MembershipChangeData::default();
    change.set_transition(ConfChangeTransition::Explicit);
    change.set_group_id(group_id);
    change.set_changes(changes);
    change.set_term(0);
    change.set_replicas(vec![]);

    let _ = leader.membership_change(change).await.unwrap();

    let expected = ConfState {
        voters: vec![1, 2, 3, 4, 5],
        learners: vec![],
        voters_outgoing: vec![],
        learners_next: vec![],
        auto_leave: false,
    };

    // wait all replicas apply membership change.
    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }
    for (_, rx) in cluster.apply_events.iter_mut().enumerate() {
        let rx = rx.as_mut().unwrap();
        loop {
            let mut matched = false;

            match rx.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => unreachable!(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                Ok(applys) => {
                    for apply in applys {
                        match apply {
                            Apply::Membership(mut membership) => {
                                membership.conf_state.voters.sort();
                                if membership.conf_state.voters == expected.voters {
                                    matched = true;
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            if matched {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    // leave joint consensus use no-op changes and wait it applied for all replicas.
    let mut change = MembershipChangeData::default();
    change.set_group_id(group_id);
    change.set_changes(vec![]);
    change.set_term(0);
    change.set_replicas(vec![]);
    let _ = leader.membership_change(change).await.unwrap();
    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }
    for (_, rx) in cluster.apply_events.iter_mut().enumerate() {
        let rx = rx.as_mut().unwrap();
        loop {
            let mut matched = false;

            match rx.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => unreachable!(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                Ok(applys) => {
                    for apply in applys {
                        match apply {
                            Apply::Membership(mut membership) => {
                                membership.conf_state.voters.sort();
                                if membership.conf_state == expected {
                                    matched = true;
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            if matched {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    // check all replicas conf_states.
    for i in 0..5 {
        let store = &cluster.storages[i]
            .group_storage(group_id, (i + 1) as u64)
            .await
            .unwrap();
        let rs = store.initial_state().unwrap();
        let mut conf_state = rs.conf_state;
        conf_state.voters.sort();
        assert_eq!(expected, conf_state);
    }
}

/// Test an existing group for joint consensus
#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_joint_consensus() {
    let task_group = TaskGroup::new();

    // start five nodes.
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

    // create three replicas and elect node 1 became leader.
    let group_id = 1;
    let node_id = 1;
    let mut plan = MakeGroupPlan {
        group_id,
        first_node_id: 1,
        replica_nums: 3,
    };
    let _ = cluster.make_group(&mut plan).await.unwrap();
    cluster.campaign_group(node_id, plan.group_id).await;
    let _ = FixtureCluster::wait_leader_elect_event(&mut cluster, node_id)
        .await
        .unwrap();
    let leader = &cluster.nodes[0];

    // write some commands
    for _ in 0..5 {
        let _ = leader
            .write(
                group_id,
                0,
                StoreData {
                    key: rand_string(4),
                    value: rand_string(8).into(),
                },
                None,
            )
            .await
            .unwrap();
    }

    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }

    // create joint consenus to add nodes 4..5.
    let mut changes = vec![];
    for next_id in 4..=5 {
        let mut change = SingleMembershipChange::default();
        change.set_change_type(ConfChangeType::AddNode);
        change.node_id = next_id;
        change.replica_id = next_id;
        changes.push(change);
    }
    let mut change = MembershipChangeData::default();
    change.set_transition(ConfChangeTransition::Explicit);
    change.set_group_id(group_id);
    change.set_changes(changes);
    change.set_term(0);
    change.set_replicas(vec![]);

    let _ = leader.membership_change(change).await.unwrap();

    // wait all replicas apply membership change.
    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }

    // Note:
    // C_old is [1, 2, 3], when entering the joint consensus point,
    // should be in the outgoing state, and C_new should be [1, 2, 3, 4, 5],
    // but the current configuration is still in the transition stage, [4, 5]
    // still can not see the memberships change.
    let expected_entered = ConfState {
        voters: vec![1, 2, 3, 4, 5],
        learners: vec![],
        voters_outgoing: vec![1, 2, 3],
        learners_next: vec![],
        auto_leave: false,
    };
    for (_, rx) in cluster.apply_events[0..3].iter_mut().enumerate() {
        let rx = rx.as_mut().unwrap();
        loop {
            let mut matched = false;

            match rx.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => unreachable!(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                Ok(applys) => {
                    for apply in applys {
                        match apply {
                            Apply::Membership(mut membership) => {
                                membership.conf_state.voters.sort();
                                if membership.conf_state == expected_entered {
                                    matched = true;
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            if matched {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    // write some commands, the {C_old, C_new} sets mustbe hava one applied.
    let data = StoreData {
        key: format!("command",),
        value: format!("data").into(),
    };
    let _ = leader.write(group_id, 0, data.clone(), None).await.unwrap();

    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }

    let rx = cluster.apply_events[0].as_mut().unwrap();
    let mut matched = false;
    loop {
        match rx.try_recv() {
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => unreachable!(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
            Ok(applys) => {
                for apply in applys {
                    match apply {
                        Apply::Normal(apply) => {
                            if data == apply.data {
                                matched = true;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        if matched {
            break;
        }
    }

    let rx = cluster.apply_events[3].as_mut().unwrap();
    let mut matched = false;
    loop {
        match rx.try_recv() {
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => unreachable!(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
            Ok(applys) => {
                for apply in applys {
                    match apply {
                        Apply::Normal(apply) => {
                            if data == apply.data {
                                matched = true;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        if matched {
            break;
        }
    }

    // leave joint consensus use no-op changes and wait it applied for all replicas.
    let expected = ConfState {
        voters: vec![1, 2, 3, 4, 5],
        learners: vec![],
        voters_outgoing: vec![],
        learners_next: vec![],
        auto_leave: false,
    };

    let mut change = MembershipChangeData::default();
    change.set_group_id(group_id);
    change.set_changes(vec![]);
    change.set_term(0);
    change.set_replicas(vec![]);
    let _ = leader.membership_change(change).await.unwrap();
    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }
    for (_, rx) in cluster.apply_events.iter_mut().enumerate() {
        let rx = rx.as_mut().unwrap();
        loop {
            let mut matched = false;

            match rx.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => unreachable!(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                Ok(applys) => {
                    for apply in applys {
                        match apply {
                            Apply::Membership(mut membership) => {
                                membership.conf_state.voters.sort();
                                if membership.conf_state == expected {
                                    matched = true;
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            if matched {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    // check all replicas conf_states.
    for i in 0..5 {
        let store = &cluster.storages[i]
            .group_storage(group_id, (i + 1) as u64)
            .await
            .unwrap();
        let rs = store.initial_state().unwrap();
        let mut conf_state = rs.conf_state;
        conf_state.voters.sort();
        assert_eq!(expected, conf_state);
    }
}

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_remove() {
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

    // create five replicas on group 1 and election replica 1 to leader.
    let group_id = 1;
    let node_id = 1;
    let plan = MakeGroupPlan {
        group_id,
        first_node_id: 1,
        replica_nums: 5,
    };
    let _ = cluster.make_group(&plan).await.unwrap();
    cluster.campaign_group(node_id, plan.group_id).await;
    let _ = FixtureCluster::wait_leader_elect_event(&mut cluster, node_id)
        .await
        .unwrap();

    let leader = cluster.nodes[0].clone();
    // remove 3..5 nodes
    let mut changes = vec![];
    for next_id in 3..=5 {
        let mut change = SingleMembershipChange::default();
        change.set_change_type(ConfChangeType::RemoveNode);
        change.node_id = next_id;
        change.replica_id = next_id;
        changes.push(change);
    }
    let mut req = MembershipChangeData {
        group_id,
        changes,
        term: 0, // no check term
        replicas: vec![],
        transition: 0,
    };
    req.set_transition(ConfChangeTransition::Explicit);
    let _ = leader.membership_change(req.clone()).await.unwrap();

    // wait all nodes apply joint consensus membership change.
    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }
    let expected = ConfState {
        voters: vec![1, 2],
        learners: vec![],
        voters_outgoing: vec![],
        learners_next: vec![],
        auto_leave: false,
    };

    for (_, rx) in cluster.apply_events.iter_mut().enumerate() {
        let rx = rx.as_mut().unwrap();
        loop {
            let mut matched = false;

            match rx.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => unreachable!(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                Ok(applys) => {
                    for apply in applys {
                        match apply {
                            Apply::Membership(mut membership) => {
                                membership.conf_state.voters.sort();
                                if membership.conf_state.voters == expected.voters {
                                    matched = true;
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            if matched {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    let mut change = MembershipChangeData::default();
    change.set_group_id(group_id);
    change.set_changes(vec![]);
    change.set_term(0);
    change.set_replicas(vec![]);
    let _ = leader.membership_change(change).await.unwrap();
    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }
    for (_, rx) in cluster.apply_events.iter_mut().enumerate() {
        let rx = rx.as_mut().unwrap();
        loop {
            let mut matched = false;

            match rx.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => unreachable!(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                Ok(applys) => {
                    for apply in applys {
                        match apply {
                            Apply::Membership(mut membership) => {
                                membership.conf_state.voters.sort();
                                if membership.conf_state == expected {
                                    matched = true;
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            if matched {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    // check all replicas conf_states.
    for i in 0..5 {
        let store = &cluster.storages[i]
            .group_storage(group_id, (i + 1) as u64)
            .await
            .unwrap();
        let rs = store.initial_state().unwrap();
        let mut conf_state = rs.conf_state;
        conf_state.voters.sort();
        assert_eq!(expected, conf_state);
    }
    // TODO: submmit command to bad node
}
