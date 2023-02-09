use oceanraft::multiraft::Error;
use oceanraft::multiraft::ProposalError;
use oceanraft::prelude::AppWriteRequest;
use oceanraft::util::TaskGroup;
use tokio::sync::oneshot;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::FixtureCluster;
use crate::fixtures::MakeGroupPlan;
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
#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_no_leader() {
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
}

//

/// The test consensus group does not have a leader or the leader is
/// submitting a proposal during an election.
#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_bad_group() {
    let task_group = TaskGroup::new();

    let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
    cluster.start();

    let mut plan = MakeGroupPlan {
        group_id: 1,
        first_node_id: 1,
        replica_nums: 3,
    };

    // now, trigger leader elect and it's should became leader.
    let _ = cluster.make_group(&mut plan).await.unwrap();
    cluster.campaign_group(1, plan.group_id).await;
    let _ = FixtureCluster::wait_leader_elect_event(&mut cluster, 1)
        .await
        .unwrap();

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
        match res {
            Ok(_) => panic!("expected {:?}, got {:?}", expected_err, res),
            Err(err) => assert_eq!(expected_err, err),
        }
    }
}

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_basic_write() {
    let task_group = TaskGroup::new();
    let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
    cluster.start();

    // create multi groups
    for group_id in 0..3 {
        let plan = MakeGroupPlan {
            group_id: group_id + 1,
            first_node_id: 1,
            replica_nums: 3,
        };
        let _ = cluster.make_group(&plan).await.unwrap();
        cluster.campaign_group(1, plan.group_id).await;
        let _ = FixtureCluster::wait_leader_elect_event(&mut cluster, 1)
            .await
            .unwrap();
    }

    // for i in 1..3 {
    //     let node_id = i + 1;
    //     let data = "data".as_bytes().to_vec();
    //     let res = write_command(&mut cluster, plan.group_id, node_id, data)
    //         .await
    //         .unwrap();
    //     let expected_err = Error::Proposal(ProposalError::NotLeader {
    //         group_id: plan.group_id,
    //         replica_id: i + 1,
    //     });
    //     println!("{:?}", res);
    //     match res {
    //         Ok(_) => panic!("expected {:?}, got {:?}", expected_err, res),
    //         Err(err) => assert_eq!(expected_err, err),
    //     }
    // }
}
