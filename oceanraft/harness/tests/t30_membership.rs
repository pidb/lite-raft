use std::time::Duration;

use oceanraft::multiraft::ApplyNormalEvent;
use oceanraft::multiraft::Error;
use oceanraft::multiraft::Event;
use oceanraft::multiraft::RaftGroupError;
use oceanraft::prelude::AppWriteRequest;
use oceanraft::prelude::ConfChangeType;
use oceanraft::prelude::MembershipChangeRequest;
use oceanraft::prelude::SingleMembershipChange;
use opentelemetry::global;
use tokio::sync::mpsc::Receiver;
use tokio::time::timeout_at;
use tokio::time::Instant;
use tracing::debug;
use tracing::info;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

use harness::fixture;
use harness::fixture::FixtureCluster;

use oceanraft::util::defer;
use oceanraft::util::TaskGroup;

pub fn enable_jager(enable: bool) {
    if !enable {
        return;
    }
    // install global collector configured based on RUST_LOG env var.
    // Allows you to pass along context (i.e., trace IDs) across services
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    // Sets up the machinery needed to export data to Jaeger
    // There are other OTel crates that provide pipelines for the vendors
    // mentioned earlier.
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("test_initial_leader_elect")
        .install_simple()
        .unwrap();

    // Create a tracing layer with the configured tracer
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // The SubscriberExt and SubscriberInitExt traits are needed to extend the
    // Registry to accept `opentelemetry (the OpenTelemetryLayer type).
    tracing_subscriber::registry()
        .with(opentelemetry)
        // Continue logging to stdout
        .with(fmt::Layer::default())
        .try_init()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_step() {
    enable_jager(true);

    let task_group = TaskGroup::new();
    let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
    cluster.start();

    // Create a raft group with single member
    let group_id = 1;
    cluster.make_group(group_id, 0, 1).await;

    // TODO: use manual tick
    cluster.trigger_elect(0, group_id).await;
    cluster.check_elect(0, 1, group_id).await;

    // for i in 0..3 {
    let mut event_rx = cluster.take_event_rx(0);
    tokio::spawn(async move {
        for _ in 0..3 {
            loop {
                let events = match event_rx.recv().await {
                    None => panic!("sender dropped"),
                    Some(events) => events,
                };

                let mut ok = false;
                for event in events.into_iter() {
                    match event {
                        Event::ApplyMembershipChange(mut event) => {
                            info!("apply membership change event = {:?}", event);
                            event.commit_to_multiraft().await.unwrap();
                            // TODO: as method called
                            event.tx.map(|tx| tx.send(Ok(())));
                            ok = true;
                            break;
                        },
                        _ => continue,
                    }
                }
                if ok {
                    break;
                }
            }
        }
        // fixture::wait_for_membership_change_apply(
        //     &mut event_rx,
        //     |mut event| async move {
        //         info!("apply membership change event = {:?}", event);
        //         event.commit_to_multiraft().await.unwrap();
        //         // TODO: as method called
        //         event.tx.map(|tx| tx.send(Ok(())));
        //         Ok(true)
        //     },
        //     Duration::from_millis(100000),
        // )
        // .await;
    });
    // }

    for i in 1..3 {
        let mut change = SingleMembershipChange::default();
        change.set_change_type(ConfChangeType::AddNode);
        change.node_id = (i + 1) as u64;
        change.replica_id = (i + 1) as u64;
        let req = MembershipChangeRequest {
            group_id,
            changes: vec![change],
            replicas: vec![],
        };
        info!("start membership change request = {:?}", req);
        cluster.multirafts[0]
            .membership_change(req.clone())
            .await
            .unwrap();
        info!("membership change request done, request = {:?}", req);
    }

    println!("all done...");

    // let command = "command".as_bytes().to_vec();

    // // recv commit event
    // // FIXME: the cluster.events don't embeded FixtureCluster struct inner.
    // let command2 = command.clone();
    // let mut event_rx = cluster.take_event_rx(0);
    // tokio::spawn(async move {
    //     fixture::wait_for_command_apply(
    //         &mut event_rx,
    //         |apply_event| {
    //             if apply_event.entry.data == command2 {
    //                 apply_event.done(Ok(()));
    //                 Ok(true)
    //             } else {
    //                 let err = format!("expected {:?} got {:?}", command2, apply_event.entry.data);
    //                 apply_event.done(Ok(()));
    //                 Err(err)
    //             }
    //         },
    //         Duration::from_millis(100),
    //     )
    //     .await;
    // });

    // // wait command apply
    // match timeout_at(
    //     Instant::now() + Duration::from_millis(100000),
    //     cluster.write_command(group_id, 0, command.clone()),
    // )
    // .await
    // {
    //     Err(_) => panic!("wait propose command {:?} timeouted", command),
    //     Ok(response) => match response {
    //         Err(err) => panic!("propose write command {:?} error {}", command, err),
    //         Ok(_) => {
    //             println!("ok...")
    //         }
    //     },
    // };
}
