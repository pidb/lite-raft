use std::time::Duration;

use oceanraft::prelude::StoreData;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::quickstart_rockstore_group;
use crate::fixtures::quickstart_rockstore_multi_groups;
use crate::fixtures::rand_string;
use crate::fixtures::RockStoreEnv;
use crate::fixtures::WriteChecker;

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_group_write() {
    let nodes = 3;
    let command_nums = 10;
    let mut rockstore_env = RockStoreEnv::new(nodes);
    let (_, mut cluster) = quickstart_rockstore_group(&mut rockstore_env, nodes).await;

    let mut recvs = vec![];
    let mut write_checker = WriteChecker::default();
    let group_id = 1;
    for j in 0..command_nums {
        let data = StoreData {
            key: rand_string(4),
            value: rand_string(8).as_bytes().to_vec(),
        };

        let rx = cluster.write_command(1, group_id, data.clone());
        recvs.push(rx);
        write_checker.insert_write(group_id, data);
        cluster.tickers[0].non_blocking_tick();
    }

    // for _ in 0..1000 {
    //     cluster.tickers[0].non_blocking_tick();
    //     // tokio::time::sleep(Duration::from_millis(1)).await;
    // }

    let events = cluster
        .wait_for_commands_apply(1, command_nums as usize, Duration::from_millis(1000))
        .await
        .unwrap();

    write_checker.check(&events);

    for event in events {
        // TODO: use done method
        event.tx.map(|tx| tx.send(Ok(())));
    }

    for rx in recvs {
        assert_eq!(rx.unwrap().await.unwrap().is_ok(), true);
    }

    rockstore_env.destory()
    // cluster.stop().await;
}

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_multigroup_write() {
    let groups = 3;
    let nodes = 3;
    let command_nums = 10;

    let mut rockstore_env = RockStoreEnv::new(nodes);

    let (_, mut cluster) = quickstart_rockstore_multi_groups(&mut rockstore_env, nodes, groups).await;

    let mut recvs = vec![];
    let mut write_checker = WriteChecker::default();
    for i in 0..groups {
        let group_id = (i + 1) as u64;
        for j in 0..command_nums {
            let data = StoreData {
                key: rand_string(4),
                value: rand_string(8).as_bytes().to_vec(),
            };

            let rx = cluster.write_command(1, group_id, data.clone());
            recvs.push(rx);
            write_checker.insert_write(group_id, data);
            cluster.tickers[0].non_blocking_tick();
        }
    }

    let events = cluster
        .wait_for_commands_apply(
            1,
            (groups * command_nums) as usize,
            Duration::from_millis(5000),
        )
        .await
        .unwrap();

    write_checker.check(&events);

    for event in events {
        // TODO: use done method
        event.tx.map(|tx| tx.send(Ok(())));
    }

    for rx in recvs {
        assert_eq!(rx.unwrap().await.unwrap().is_ok(), true);
    }

    rockstore_env.destory();

    // cluster.stop().await;
}
