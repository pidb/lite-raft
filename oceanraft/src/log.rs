use std::env;
use std::env::temp_dir;
use std::panic::PanicInfo;
use std::path::PathBuf;
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
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;

lazy_static! {
    static ref GLOBAL_LOG_GUARD: Arc<Mutex<Option<WorkerGuard>>> = Arc::new(Mutex::new(None));
}

/// Enum representing the rotation frequency for a log file.
#[derive(Clone, Eq, PartialEq, Debug)]
#[allow(unused)]
pub enum RotationKind {
    /// Rotate the log file every minute.
    Minutely,
    /// Rotate the log file every hour.
    Hourly,
    /// Rotate the log file every day.
    Daily,
    /// Never rotate the log file.
    Never,
}

/// Implement the `FormatEvent` trait for `EventFormatter`
pub struct EventFormatter;

impl<S, N> FormatEvent<S, N> for EventFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    /// This method is called for each log event and formats it
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

/// Builder for a general subscriber that supports
/// console and file logging.
pub struct GeneralSubscriberBuilder {
    app_name: Option<String>,
    level: String,
    enable_file_log: bool,
    enable_console_log: bool,
    log_file_dir: Option<String>,
    log_file_rotation: Option<RotationKind>,
}

impl GeneralSubscriberBuilder {
    /// Creates a new builder instance with the given application name.
    pub fn builder() -> Self {
        Self {
            app_name: None,
            level: "error".to_string(),
            enable_file_log: false,
            enable_console_log: false,
            log_file_dir: None,
            log_file_rotation: None,
        }
    }

    /// Sets the logging level.
    /// Note: Log level is extraced from the default env. otherwise, the log set by user provided.
    pub fn level(mut self, level: &str) -> Self {
        self.level = level.to_string();
        self
    }

    /// Enables logging to console.
    pub fn enable_console_log(mut self) -> Self {
        self.enable_console_log = true;
        self
    }

    /// Enables logging to file.
    pub fn enable_file_log(mut self, app_name: &str) -> Self {
        self.enable_file_log = true;
        self.app_name = Some(app_name.to_string());
        self
    }

    /// Sets the rotation kind for the log file.
    pub fn rotation(mut self, rotation: RotationKind) -> Self {
        if !self.enable_file_log {
            panic!("only file log enabled can set rotation")
        }

        self.log_file_rotation = Some(rotation);
        self
    }

    /// Sets the directory for the log file.
    pub fn dir(mut self, dir: &str) -> Self {
        if !self.enable_file_log {
            panic!("only file log enabled can set dir")
        }

        self.log_file_dir = Some(dir.to_string());
        self
    }

    /// Builds the subscriber with the given configurations.
    /// Returnd `WokerGuard` if `enable_file_log` and flush logs
    /// to file when dropped.
    pub fn build(mut self) -> (impl Subscriber, Option<WorkerGuard>) {
        // extract log level
        // log level is extraced from the default env. otherwise, the log set by user provided.
        let directives = env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_x| self.level);
        let env_filter = EnvFilter::new(directives);

        let mut guard = None;
        let mut layers = vec![];
        if self.enable_console_log {
            let console_layer = fmt::Layer::default()
                .event_format(EventFormatter {})
                .boxed();
            layers.push(console_layer);
        }

        if self.enable_file_log {
            let dir = if let Some(dir) = self.log_file_dir.take() {
                PathBuf::from(dir)
            } else {
                temp_dir()
            };
            let rotation = if let Some(rot) = self.log_file_rotation.take() {
                match rot {
                    RotationKind::Daily => Rotation::DAILY,
                    RotationKind::Hourly => Rotation::HOURLY,
                    RotationKind::Minutely => Rotation::MINUTELY,
                    RotationKind::Never => Rotation::NEVER,
                }
            } else {
                Rotation::NEVER
            };

            let file_appender = RollingFileAppender::new(
                rotation,
                dir,
                self.app_name.take().unwrap_or("".to_string()),
            );

            let (writer, writer_guard) = tracing_appender::non_blocking(file_appender);
            let layer = fmt::Layer::new()
                .with_span_events(fmt::format::FmtSpan::FULL)
                .with_writer(writer)
                .with_ansi(false)
                .event_format(EventFormatter {})
                .boxed();
            guard = Some(writer_guard);
            layers.push(layer);
        }

        (Registry::default().with(env_filter).with(layers), guard)
    }
}

/// Set up global tracing for the application with a given directory for log files and logging level.
/// This function will be called only once.
///
/// # Arguments
///
/// * `dir` - A string slice that holds the directory path where log files will be stored.
/// * `level` - A string slice that holds the logging level for tracing.
pub fn set_default_tracing(dir: &str, level: &str) {
    // Use `Once` to ensure this function is called only once
    static START: Once = Once::new();

    // Acquire the global log guard and initialize the global tracing
    START.call_once(|| {
        let mut g = GLOBAL_LOG_GUARD.as_ref().lock().unwrap();
        *g = Some(init_global_tracing("oceanfs", dir, level));
    });
}

/// Initializes a global tracing subscriber that logs to the console.
///
/// # Arguments
///
/// * `level`: A string slice that specifies the log level to enable for the global tracing subscriber.
///
/// # Panics
///
/// This function panics if it fails to set the global tracing subscriber.
pub fn init_global_console_tracing(level: &str) {
    let (sub, _) = GeneralSubscriberBuilder::builder()
        .enable_console_log()
        .level(level)
        .build();
    tracing::subscriber::set_global_default(sub).expect("error setting global tracing subscriber");
    tracing::info!("initialized global {} tracing", level);
}

fn init_global_tracing(app_name: &str, dir: &str, level: &str) -> WorkerGuard {
    let (sub, guard) = GeneralSubscriberBuilder::builder()
        .enable_console_log()
        .enable_file_log(app_name)
        .rotation(RotationKind::Hourly)
        .dir(dir)
        .level(level)
        .build();
    tracing::subscriber::set_global_default(sub).expect("error setting global tracing subscriber");

    tracing::info!(
        "initialized global tracing: in {}/{} at {}",
        dir,
        app_name,
        level
    );
    guard.unwrap()
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
