use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

use oceanraft::multiraft::Error;
use oceanraft::multiraft::ProposalError;
use oceanraft::prelude::AppWriteRequest;
use oceanraft::util::TaskGroup;
use tokio::sync::mpsc::channel;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::FixtureCluster;
use crate::fixtures::MakeGroupPlan;

#[derive(Default)]
struct WriteCollection {
    data: HashMap<u64, Vec<Vec<u8>>>,
}

impl WriteCollection {
    fn new(group_size: usize) -> Self {
        let mut writes = HashMap::new();
        for g in 0..group_size {
            writes.insert(g as u64 + 1, vec![]);
        }

        Self { data: writes }
    }
}

impl Debug for WriteCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = write!(f, "group_size = {}, [", self.data.len())?;
        for (group_id, commands) in self.data.iter() {
            let _ = write!(f, "{}: commands = {}, ", *group_id, commands.len())?;
        }
        write!(f, "]")
    }
}

impl PartialEq for WriteCollection {
    fn eq(&self, other: &Self) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }

        for (group_id, commands) in self.data.iter() {
            if let Some(other_commands) = other.data.get(group_id) {
                if commands.len() != other_commands.len() {
                    return false;
                }

                for (c1, c2) in commands.iter().zip(other_commands) {
                    if c1 != c2 {
                        unsafe {
                            println!(
                                "commands not equal {:?} != {:?}",
                                std::str::from_utf8_unchecked(c1),
                                std::str::from_utf8_unchecked(c2)
                            );
                        }
                        return false;
                    }
                }
            } else {
                return false;
            }
        }

        true
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

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

    let group_size = 3;
    let command_size_per_group = 100;
    // create multi groups
    for group_id in 1..group_size + 1 {
        let plan = MakeGroupPlan {
            group_id,
            first_node_id: 1,
            replica_nums: 3,
        };
        let _ = cluster.make_group(&plan).await.unwrap();
        cluster.campaign_group(1, plan.group_id).await;
        let _ = FixtureCluster::wait_leader_elect_event(&mut cluster, 1)
            .await
            .unwrap();
    }

    // and then write commands to theses leader of groups
    let mut recvs = vec![];
    let mut writes = WriteCollection::new(3);
    for group_id in 1..group_size + 1 {
        for command_id in 1..command_size_per_group + 1 {
            let data = format!("{}: data on group_id = {}", command_id, group_id)
                .as_bytes()
                .to_vec();

            let rx = cluster.write_command(1, group_id, data.clone());
            recvs.push(rx);
            writes.data.get_mut(&group_id).unwrap().push(data);
        }
    }

    // check all command apply
    let (done_tx, mut done_rx) = channel(1);
    tokio::spawn(async move {
        for rx in recvs.iter_mut() {
            // TODO: add response generic
            let _ = rx.await;
        }
        done_tx.send(()).await.unwrap();
    });

    // ticks
    for _ in 0..1000 {
        cluster.tickers[0].non_blocking_tick();
        // tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let mut applys = WriteCollection::default();
    let events = FixtureCluster::wait_for_command_apply(
        cluster.mut_event_rx(1),
        Duration::from_millis(3000),
        (group_size * command_size_per_group) as usize ,
    ).await.unwrap();


    for event in events {
        match applys.data.get_mut(&event.group_id) {
            None => {
                applys
                    .data
                    .insert(event.group_id, vec![event.entry.data.clone()]);
            }
            Some(cmds) => cmds.push(event.entry.data.clone()),
        };
        event.tx.map(|tx| tx.send(Ok(())));
    }

    assert_eq!(writes, applys);

    match timeout(Duration::from_millis(100), done_rx.recv()).await {
        Err(_) => panic!("wait apply timeouted"),
        Ok(_) => {}
    }
}
