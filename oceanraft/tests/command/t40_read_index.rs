use std::collections::HashMap;
use std::time::Duration;

use oceanraft::prelude::StoreData;
use oceanraft::ReadIndexContext;
use oceanraft::ReadIndexMessage;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::quickstart_rockstore_group;
use crate::fixtures::RockStoreEnv;

#[async_entry::test(
    flavor = "multi_thread",
    init = "init_default_ut_tracing()",
    tracing_span = "debug"
)]
async fn test_group_read_index() {
    let nodes = 3;
    let command_nums = 10;

    let mut rockstore_env = RockStoreEnv::new(nodes);
    let mut cluster = quickstart_rockstore_group(&mut rockstore_env, nodes).await;

    let mut recvs = vec![];
    let group_id = 1;
    for i in 0..command_nums {
        let command_id = (i + 1) as u64;

        let kv_cmd = StoreData {
            key: format!("key_{}", command_id),
            value: format!("value_{}", command_id).as_bytes().to_vec(),
        };

        let rx = cluster.write_command(1, group_id, kv_cmd.clone());
        recvs.push(rx);
        cluster.tickers[0].non_blocking_tick();
    }

    let mut applied_kvs = HashMap::<String, StoreData>::new();

    let events = cluster
        .wait_for_commands_apply(1, command_nums as usize, Some(Duration::from_millis(1000)))
        .await
        .unwrap();

    for event in events {
        // FIXME: Fuck ugly, use trait in Apply.
        // let r = flexbuffers::Reader::get_root(event.data.as_ref()).unwrap();
        // let wd = FixtureWriteData::deserialize(r).unwrap();
        applied_kvs.insert(event.data.key.clone(), event.data);

        // TODO: use done method
        event.tx.map(|tx| tx.send(Ok(((), None))));
    }

    // write_checker.check(&events);

    for rx in recvs {
        assert_eq!(rx.unwrap().await.unwrap().is_ok(), true);
    }

    let _ = cluster.nodes[0]
        .read_index(ReadIndexMessage {
            group_id,
            context: ReadIndexContext {
                uuid: None,
                data: None,
            },
        })
        .await
        .unwrap();

    for i in 0..command_nums {
        let command_id = (i + 1) as u64;
        let kv_cmd = StoreData {
            key: format!("key_{}", command_id),
            value: format!("value_{}", command_id).as_bytes().to_vec(),
        };
        assert_eq!(*applied_kvs.get(&kv_cmd.key).unwrap(), kv_cmd);
    }
    rockstore_env.destory();
    // cluster.stop().await;
    // write -> 1 -> 2 -> 3
    // read  -> commit_index = 1
    // ready -> read -> pending
    // commit -> 1 -> 2 -> 3
    // read  -> commit_index = 4 (fatal, consistency)
}
