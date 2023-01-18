use std::time::Duration;

use oceanraft::multiraft::ApplyNormalEvent;
use oceanraft::multiraft::Error;
use oceanraft::multiraft::Event;
use oceanraft::multiraft::RaftGroupError;
use oceanraft::prelude::AppWriteRequest;
use opentelemetry::global;
use tokio::sync::mpsc::Receiver;
use tokio::time::timeout_at;
use tokio::time::Instant;
use tracing::info;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

use harness::fixture::FixtureCluster;

use oceanraft::util::TaskGroup;
use oceanraft::util::defer;

async fn wait_for_command_apply<P>(rx: &mut Receiver<Vec<Event>>, mut predicate: P)
where
    P: FnMut(ApplyNormalEvent) -> Result<bool, String>,
{
    // let loop_fn = async {
    loop {
        let events = match rx.recv().await {
            None => panic!("sender dropped"),
            Some(events) => events,
        };

        for event in events.into_iter() {
            match event {
                Event::ApplyNormal(apply_event) => match predicate(apply_event) {
                    Err(err) => panic!("{}", err),
                    Ok(matched) => {
                        if !matched {
                            continue;
                        } else {
                            return;
                        }
                    }
                },
                _ => continue,
            }
        }
    }
    // };
}

#[tokio::test(flavor="multi_thread")]
async fn test_write_bad_group() {
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

    let task_group = TaskGroup::new();
    defer! {
        task_group.stop();
        // FIXME: use sync wait
        // task_group.joinner().awa`it;
    }
    let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
    let bad_group_id = 4;
    let rx = cluster.write_command(bad_group_id, 0, "data".as_bytes().to_vec());
    let expected_err = Error::RaftGroup(RaftGroupError::NotExist(1, 4));
    match rx.await.unwrap() {
        Ok(_) => panic!("expected error = {:?}, got Ok", expected_err),
        Err(err) => assert_eq!(err, expected_err),
    }
 
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write() {
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

    let task_group = TaskGroup::new();
    let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
    let group_id = 1;
    cluster.make_group(group_id, 0, 3).await;
    cluster.trigger_elect(0, group_id).await;
    FixtureCluster::wait_for_leader_elect(&mut cluster.events, 1)
        .await
        .unwrap();

    let command = "command".as_bytes().to_vec();

    // recv commit event
    // FIXME: the cluster.events don't embeded FixtureCluster struct inner.
    let command2 = command.clone();
    let mut event_rx = cluster.take_event_rx(0);
    tokio::spawn(async move {
        let fut = wait_for_command_apply(&mut event_rx, |apply_event| {
            if apply_event.entry.data == command2 {
                apply_event.done(Ok(()));
                Ok(true)
            } else {
                let err = format!("expected {:?} got {:?}", command2, apply_event.entry.data);
                apply_event.done(Ok(()));
                Err(err)
            }
        });

        match timeout_at(Instant::now() + Duration::from_millis(100), fut).await {
            Err(_) => {
                panic!("timeout");
            }
            // Err(_) => panic!("wait commit event for proposed command {:?}", command2),
            Ok(_) => {}
        };
    });

    // wait command apply
    match timeout_at(
        Instant::now() + Duration::from_millis(100000),
        cluster.write_command(group_id, 0, command.clone()),
    )
    .await
    {
        Err(_) => panic!("wait propose command {:?} timeouted", command),
        Ok(response) => match response {
            Err(err) => panic!("propose write command {:?} error {}", command, err),
            Ok(_) => {}
        },
    };
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leader_writing_disk_parallel() {
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

    let task_group = TaskGroup::new();
    let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
    let group_id = 1;
    cluster.make_group(group_id, 0, 3).await;
}
