///! the codes from openraft.
use std::env;
use std::panic::PanicInfo;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Once;


use lazy_static::lazy_static;
use tracing::Event;
use tracing::Subscriber;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::FormatEvent;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::FormattedFields;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

lazy_static! {
    static ref GLOBAL_UT_LOG_GUARD: Arc<Mutex<Option<WorkerGuard>>> = Arc::new(Mutex::new(None));
}

pub struct EventFormatter {}

impl<S, N> FormatEvent<S, N> for EventFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let meta = event.metadata();

        SystemTime {}.format_time(&mut writer)?;
        writer.write_char(' ')?;

        let fmt_level = meta.level().as_str();
        write!(writer, "{:>5} ", fmt_level)?;

        write!(writer, "{:0>2?} ", std::thread::current().id())?;

        if let Some(scope) = ctx.event_scope() {
            let mut seen = false;

            for span in scope.from_root() {
                write!(writer, "{}", span.metadata().name())?;
                write!(writer, "#{:x}", span.id().into_u64())?;

                seen = true;
                let ext = span.extensions();
                if let Some(fields) = &ext.get::<FormattedFields<N>>() {
                    if !fields.is_empty() {
                        write!(writer, "{{{}}}", fields)?;
                    }
                }
                write!(writer, ":")?;
            }

            if seen {
                writer.write_char(' ')?;
            }
        };

        ctx.format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

pub fn init_file_logging(app_name: &str, dir: &str, level: &str) -> (WorkerGuard, impl Subscriber) {
    // open log file

    let f = RollingFileAppender::new(Rotation::HOURLY, dir, app_name);

    // build subscriber

    let (writer, writer_guard) = tracing_appender::non_blocking(f);

    let file_layer = fmt::Layer::new()
        .with_span_events(fmt::format::FmtSpan::FULL)
        .with_writer(writer)
        .with_ansi(false)
        .event_format(EventFormatter {});

    let console_layer = fmt::Layer::default();

    // TODO: add opentelemetry trace
    // global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    // let opentelemetry = tracing_opentelemetry::layer();

    // Use env RUST_LOG to initialize log if present.
    // Otherwise use the specified level.
    let directives = env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_x| level.to_string());
    let env_filter = EnvFilter::new(directives);

    let subscriber = Registry::default()
        .with(env_filter)
        .with(file_layer)
        .with(console_layer);

    (writer_guard, subscriber)
}

pub fn init_default_ut_tracing() {
    static START: Once = Once::new();

    START.call_once(|| {
        let mut g = GLOBAL_UT_LOG_GUARD.as_ref().lock().unwrap();
        *g = Some(init_global_tracing("ut", "_log", "DEBUG"));
    });
}

pub fn init_global_tracing(app_name: &str, dir: &str, level: &str) -> WorkerGuard {
    set_panic_hook();

    let (g, sub) = init_file_logging(app_name, dir, level);
    tracing::subscriber::set_global_default(sub).expect("error setting global tracing subscriber");

    tracing::info!(
        "initialized global tracing: in {}/{} at {}",
        dir,
        app_name,
        level
    );
    g
}

pub fn set_panic_hook() {
    std::panic::set_hook(Box::new(|panic| {
        log_panic(panic);
    }));
}

pub fn log_panic(panic: &PanicInfo) {
    let backtrace = {
        #[cfg(feature = "bt")]
        {
            format!("{:?}", Backtrace::force_capture())
        }

        #[cfg(not(feature = "bt"))]
        {
            "backtrace is disabled without --features 'bt'".to_string()
        }
    };

    if let Some(location) = panic.location() {
        tracing::error!(
            message = %panic,
            backtrace = %backtrace,
            panic.file = location.file(),
            panic.line = location.line(),
            panic.column = location.column(),
        );
    } else {
        tracing::error!(message = %panic, backtrace = %backtrace);
    }
}
