use std::time::Duration;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::quickstart_group;
use crate::fixtures::quickstart_multi_groups;
use crate::fixtures::FixtureCluster;
use crate::fixtures::WriteChecker;

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_group_write() {
    let node_nums = 3;
    let command_nums = 10;
    let (_, mut cluster) = quickstart_group(node_nums).await;

    let mut recvs = vec![];
    let mut write_checker = WriteChecker::default();
    let group_id = 1;
    for j in 0..command_nums {
        let command_id = (j + 1) as u64;
        let data = format!("{}: data on group_id = {}", command_id, group_id)
            .as_bytes()
            .to_vec();

        let rx = cluster.write_command(1, group_id, data.clone());
        recvs.push(rx);
        write_checker.insert_write(group_id, data);
        cluster.tickers[0].non_blocking_tick();
    }

    // for _ in 0..1000 {
    //     cluster.tickers[0].non_blocking_tick();
    //     // tokio::time::sleep(Duration::from_millis(1)).await;
    // }

    let events = FixtureCluster::wait_for_command_apply(
        cluster.mut_apply_event_rx(1),
        Duration::from_millis(1000),
        command_nums as usize,
    )
    .await
    .unwrap();

    write_checker.check(&events);

    for event in events {
        // TODO: use done method
        event.tx.map(|tx| tx.send(Ok(())));
    }

    for rx in recvs {
        assert_eq!(rx.await.unwrap().is_ok(), true);
    }

    cluster.stop().await;
}

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_multigroup_write() {
    let group_nums = 3;
    let node_nums = 3;
    let command_nums = 10;
    let (_, mut cluster) = quickstart_multi_groups(node_nums, group_nums).await;

    let mut recvs = vec![];
    let mut write_checker = WriteChecker::default();
    for i in 0..group_nums {
        let group_id = (i + 1) as u64;
        for j in 0..command_nums {
            let command_id = (j + 1) as u64;
            let data = format!("{}: data on group_id = {}", command_id, group_id)
                .as_bytes()
                .to_vec();

            let rx = cluster.write_command(1, group_id, data.clone());
            recvs.push(rx);
            write_checker.insert_write(group_id, data);
            cluster.tickers[0].non_blocking_tick();
        }
    }

    let events = FixtureCluster::wait_for_command_apply(
        cluster.mut_apply_event_rx(1),
        Duration::from_millis(5000),
        (group_nums * command_nums) as usize,
    )
    .await
    .unwrap();

    write_checker.check(&events);

    for event in events {
        // TODO: use done method
        event.tx.map(|tx| tx.send(Ok(())));
    }

    for rx in recvs {
        assert_eq!(rx.await.unwrap().is_ok(), true);
    }

    cluster.stop().await;
}
