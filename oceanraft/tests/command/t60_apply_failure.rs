use std::time::Duration;

use oceanraft::prelude::StoreData;
use tokio::time::sleep;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::quickstart_memstorage_group;
use crate::fixtures::rand_string;
use crate::fixtures::MemStoreEnv;
use crate::fixtures::WriteChecker;

/// Testing pending proposals after removing the leader of
/// a single consensus group should return an error。
#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_apply_failure() {
    let nodes = 3;
    let mut env = MemStoreEnv::new(nodes);
    let (_, mut cluster) = quickstart_memstorage_group(&mut env, nodes).await;

    let group_id = 1;

    // disconnect node 1 network
    cluster.transport.disconnect(1, 2).await;
    cluster.transport.disconnect(1, 3).await;

    env.state_machines[1].trigger_apply_error(true, 3).await;
    env.state_machines[2].trigger_apply_error(true, 5).await;

    // propose command, but node network is disconnected, so command can't commit.
    let command_size = 3;
    let mut rxs = vec![];
    for _ in 1..command_size + 1 {
        let data = StoreData {
            key: rand_string(4),
            value: rand_string(8).as_bytes().to_vec(),
        };

        let rx = cluster.write_command(1, group_id, data.clone());
        rxs.push(rx);
    }

    sleep(Duration::from_millis(100)).await;

    cluster.transport.reconnect(1, 2).await;
    cluster.transport.reconnect(1, 3).await;

    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }

    let res = cluster
        .wait_for_commands_apply(2, command_size, Duration::from_millis(1000))
        .await;
    assert_eq!(res.is_err(), true);

    env.state_machines[1].trigger_apply_error(false, 0).await;
    env.state_machines[2].trigger_apply_error(false, 0).await;

    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }

    let applys = cluster
        .wait_for_commands_apply(2, command_size, Duration::from_millis(1000))
        .await
        .unwrap();

    let mut write_checker = WriteChecker::default();
    write_checker.check(&applys);
    for apply in applys {
        // TODO: use done method
        apply.tx.map(|tx| tx.send(Ok(())));
    }

    for rx in rxs {
        // TODO: assertiong response type
        assert_eq!(rx.unwrap().await.unwrap().is_ok(), true);
    }
    // cluster.stop().await;
}
