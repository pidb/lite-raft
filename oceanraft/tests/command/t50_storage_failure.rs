use std::time::Duration;

use oceanraft::storage::MultiRaftStorage;
use oceanraft::prelude::StoreData;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::quickstart_memstorage_group;
use crate::fixtures::rand_string;
use crate::fixtures::MemStoreEnv;
use crate::fixtures::WriteChecker;

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_log_storeage_unavailable() {
    let command_nums = 10;
    // create five nodes
    let nodes = 3;
    let mut env = MemStoreEnv::new(nodes);
    let (_, mut cluster) = quickstart_memstorage_group(&mut env, nodes).await;

    // trigger unavailable for 1,2 nodes storage
    env.storages[1]
        .group_storage(1, 1)
        .await
        .unwrap()
        .wl()
        .trigger_log_unavailable(true);
    env.storages[2]
        .group_storage(1, 2)
        .await
        .unwrap()
        .wl()
        .trigger_log_unavailable(true);

    let mut recvs = vec![];
    let mut write_checker = WriteChecker::default();
    let group_id = 1;
    for _ in 0..command_nums {
        let data = StoreData {
            key: rand_string(4),
            value: rand_string(8).as_bytes().to_vec(),
        };

        let rx = cluster.write_command(1, group_id, data.clone());
        recvs.push(rx);
        write_checker.insert_write(group_id, data);
        cluster.tickers[0].non_blocking_tick();
    }

    let events = cluster
        .wait_for_commands_apply(1, command_nums as usize, Duration::from_millis(100))
        .await;
    assert_eq!(events.is_err(), true);

    // trigger available for 1,2 nodes storage
    env.storages[1]
        .group_storage(1, 1)
        .await
        .unwrap()
        .wl()
        .trigger_log_unavailable(false);
    env.storages[2]
        .group_storage(1, 2)
        .await
        .unwrap()
        .wl()
        .trigger_log_unavailable(false);

    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }

    let events = cluster
        .wait_for_commands_apply(1, command_nums as usize, Duration::from_millis(1000))
        .await
        .unwrap();

    write_checker.check(&events);

    for event in events {
        // TODO: use done method
        event.tx.map(|tx| tx.send(Ok(((), None))));
    }

    for rx in recvs {
        assert_eq!(rx.unwrap().await.unwrap().is_ok(), true);
    }
}

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_multi_storeage_unavailable() {
    let command_nums = 10;
    // create five nodes
    let nodes = 3;
    let mut env = MemStoreEnv::new(nodes);
    let (_, mut cluster) = quickstart_memstorage_group(&mut env, nodes).await;

    // trigger unavailable for 1,2 nodes storage
    env.storages[1].trigger_storage_temp_unavailable(true).await;
    env.storages[2].trigger_storage_temp_unavailable(true).await;

    let mut recvs = vec![];
    let mut write_checker = WriteChecker::default();
    let group_id = 1;
    for _ in 0..command_nums {
        let data = StoreData {
            key: rand_string(4),
            value: rand_string(8).as_bytes().to_vec(),
        };

        let rx = cluster.write_command(1, group_id, data.clone());
        recvs.push(rx);
        write_checker.insert_write(group_id, data);
        cluster.tickers[0].non_blocking_tick();
    }

    let events = cluster
        .wait_for_commands_apply(1, command_nums as usize, Duration::from_millis(100))
        .await;
    assert_eq!(events.is_err(), true);

    env.storages[1]
        .trigger_storage_temp_unavailable(false)
        .await;
    env.storages[2]
        .trigger_storage_temp_unavailable(false)
        .await;

    // trigger available for 1,2 nodes storage
    // env.storages[1]
    //     .group_storage(1, 1)
    //     .await
    //     .unwrap()
    //     .wl()
    //     .trigger_log_unavailable(false);
    // env.storages[2]
    //     .group_storage(1, 2)
    //     .await
    //     .unwrap()
    //     .wl()
    //     .trigger_log_unavailable(false);

    for _ in 0..10 {
        cluster.tickers[0].non_blocking_tick();
    }

    let events = cluster
        .wait_for_commands_apply(1, command_nums as usize, Duration::from_millis(1000))
        .await
        .unwrap();

    write_checker.check(&events);

    for event in events {
        // TODO: use done method
        event.tx.map(|tx| tx.send(Ok(((), None))));
    }

    for rx in recvs {
        assert_eq!(rx.unwrap().await.unwrap().is_ok(), true);
    }
}
