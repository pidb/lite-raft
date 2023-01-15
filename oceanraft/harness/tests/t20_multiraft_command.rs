use std::time::Duration;

use oceanraft::multiraft::Event;
use oceanraft::prelude::AppWriteRequest;
use opentelemetry::global;
use tokio::time::timeout_at;
use tokio::time::Instant;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

use harness::fixture::FixtureCluster;

use oceanraft::util::TaskGroup;

fn make_write_request(group_id: u64, data: Vec<u8>) -> AppWriteRequest {
    AppWriteRequest {
        group_id,
        term: 0,
        context: vec![],
        data,
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
    

    let command = "command".as_bytes().to_vec();
    // wait command apply
    match timeout_at(
        Instant::now() + Duration::from_millis(100),
        cluster.multirafts[0].write(make_write_request(group_id, command.clone())),
    )
    .await
    {
        Err(_) => panic!("wait stop transport error"),
        Ok(response) => match response {
            Err(err) => panic!("propose write command {:?} error {}", command, err),
            Ok(_) => {}
        },
    };

    // if command apply on leader, then event is arrived.
    let events = match timeout_at(
        Instant::now() + Duration::from_millis(100),
        cluster.events[0].recv(),
    )
    .await
    {
        Err(_) => panic!("wait commit event for proposed command {:?}", command),
        Ok(event) => match event {
            None => panic!(
                "execpted recv commit event for proposed command {:?}, but sender closed",
                command
            ),
            Some(events) => events,
        },
    };

    let mut execpted_command = None;
    for event in events.into_iter() {
        match event {
            Event::ApplyNormal(mut event) => {
                execpted_command = Some(event.entry.take_data());
                break;
            }
            _ => continue,
        }
    }

    assert_eq!(execpted_command, Some(command));
}
