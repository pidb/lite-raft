use std::time::Duration;

use tokio::time::timeout_at;
use tokio::time::Instant;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::quickstart_group;
use crate::fixtures::FixtureCluster;
use crate::fixtures::WriteChecker;

/// Testing pending proposals after removing the leader of
/// a single consensus group should return an error。
#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_group_stale_write() {
    // defer! {
    //     task_group.stop();
    //     // FIXME: use sync wait
    //     // task_group.joinner().awa`it;
    // }

    let node_nums = 3;
    let (_task_group, mut cluster) = quickstart_group(node_nums).await;

    let group_id = 1;

    // disconnect node 1 network
    cluster.transport.disconnect(1, 2).await;
    cluster.transport.disconnect(1, 3).await;

    // propose command, but node network is disconnected, so command can't commit.
    let command_size = 10;
    let mut stale_recvs = vec![];
    for command_id in 1..command_size + 1 {
        let data = format!("{}: data on group_id = {}", command_id, group_id)
            .as_bytes()
            .to_vec();

        let rx = cluster.write_command(1, group_id, data.clone());
        stale_recvs.push(rx);
        // TODO: use tick method
        cluster.tickers[0].tick().await;
    }

    cluster.campaign_group(2, group_id).await;
    for i in 1..3 {
        let el = FixtureCluster::wait_leader_elect_event(&mut cluster, i + 1)
            .await
            .unwrap();
        assert_eq!(el.leader_id, 2);
    }

    cluster.transport.reconnect(1, 2).await;
    cluster.transport.reconnect(1, 3).await;

    // check stale
    let mut write_checker = WriteChecker::default();
    let mut recvs = vec![];
    for (i, stale_rx) in stale_recvs.into_iter().enumerate() {
        // because heartbeat can not set committed index, so whenever we
        // are ready to verify stale command, we need send append to active
        // commit of stale node.
        let data = format!("{}: data on group_id = {}", i, group_id)
            .as_bytes()
            .to_vec();
        write_checker.insert_write(group_id, data.clone());
        recvs.push(cluster.write_command(2, group_id, data));

        // TODO: use tick method
        // cluster.tickers[0].tick().await;
        cluster.tickers[1].tick().await;
        // cluster.tickers[2].tick().await;
        // TODO: assertion error type
        // let res = timeout_at(Instant::now() + Duration::from_millis(1000), stale_rx)
        // .await
        // .expect(format!("wait stale {} timeouted", i + 1).as_str());
        assert_eq!(stale_rx.await.unwrap().is_err(), true);
    }

    // check normal
    let apply_events = FixtureCluster::wait_for_command_apply(
        cluster.mut_apply_event_rx(2),
        Duration::from_millis(1000),
        command_size,
    )
    .await
    .unwrap();
    write_checker.check(&apply_events);
    for event in apply_events {
        // TODO: use done method
        event.tx.map(|tx| tx.send(Ok(())));
    }

    for rx in recvs {
        // TODO: assertiong response type
        assert_eq!(rx.await.unwrap().is_ok(), true);
    }
}
