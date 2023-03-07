use std::collections::HashMap;
use std::time::Duration;

use oceanraft::prelude::AppReadIndexRequest;
use serde::Deserialize;
use serde::Serialize;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::quickstart_group;
use crate::fixtures::FixtureCluster;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct KVCommand {
    id: u64,
    key: String,
    value: String,
}

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_group_read_index() {
    let node_nums = 3;
    let command_nums = 10;
    let (_, mut cluster) = quickstart_group(node_nums).await;

    let mut recvs = vec![];
    let group_id = 1;
    for i in 0..command_nums {
        let command_id = (i + 1) as u64;

        let kv_cmd = KVCommand {
            id: command_id,
            key: format!("key_{}", command_id),
            value: format!("value_{}", command_id),
        };

        let data = serde_json::to_vec(&kv_cmd).unwrap();
        let rx = cluster.write_command(1, group_id, data.clone());
        recvs.push(rx);
        cluster.tickers[0].non_blocking_tick();
    }

    let mut applied_kvs = HashMap::<String, KVCommand>::new();

    let events = FixtureCluster::wait_for_command_apply(
        cluster.mut_apply_event_rx(1),
        Duration::from_millis(1000),
        command_nums as usize,
    )
    .await
    .unwrap();

    for event in events {
        let kv_cmd: KVCommand = serde_json::from_slice(event.entry.get_data()).unwrap();
        applied_kvs.insert(kv_cmd.key.clone(), kv_cmd);

        // TODO: use done method
        event.tx.map(|tx| tx.send(Ok(())));
    }

    // write_checker.check(&events);

    for rx in recvs {
        assert_eq!(rx.await.unwrap().is_ok(), true);
    }

    println!("{}", serde_json::to_string(&applied_kvs).unwrap());

    let _ = cluster.nodes[0]
        .read_index(AppReadIndexRequest {
            context: None,
            group_id,
        })
        .await
        .unwrap();

    for i in 0..command_nums {
        let command_id = (i + 1) as u64;
        let kv_cmd = KVCommand {
            id: command_id,
            key: format!("key_{}", command_id),
            value: format!("value_{}", command_id),
        };
        assert_eq!(*applied_kvs.get(&kv_cmd.key).unwrap(), kv_cmd);
    }
    cluster.stop().await;
    // write -> 1 -> 2 -> 3
    // read  -> commit_index = 1
    // ready -> read -> pending
    // commit -> 1 -> 2 -> 3
    // read  -> commit_index = 4 (fatal, consistency)
}
