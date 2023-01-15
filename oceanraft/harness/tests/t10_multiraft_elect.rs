use std::time::Duration;

use opentelemetry::global;
use tokio::time::timeout_at;
use tokio::time::Instant;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

use harness::fixture::FixtureCluster;

use oceanraft::util::TaskGroup;

#[tokio::test(flavor = "multi_thread")]
async fn test_initial_leader_elect() {
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

    for node_index in 0..3 {
        let task_group = TaskGroup::new();
        let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
        let group_id = 1;
        cluster.make_group(group_id, 0, 3).await;

        cluster
            .check_elect(node_index, node_index + 1, group_id)
            .await;

        if let Err(_) = timeout_at(
            Instant::now() + Duration::from_millis(100),
            cluster.transport.stop_all(),
        )
        .await
        {
            panic!("wait stop transport error")
        }
        cluster.transport.stop_all().await.unwrap();
        task_group.stop();
        if let Err(_) = timeout_at(
            Instant::now() + Duration::from_millis(100),
            task_group.joinner(),
        )
        .await
        {
            panic!("wait cluster taks stop error")
        }
    }
}
