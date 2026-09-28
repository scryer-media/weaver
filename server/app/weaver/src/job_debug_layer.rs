//! The tracing layer that feeds each job's debug ring.
//!
//! It carries its own filter, so it sees a job's DEBUG and TRACE events
//! whatever the output filter is set to. Only events with a `job_id` field
//! qualify, and that is decided per callsite from the event's metadata, so a
//! callsite without one is never enabled on this layer's account.

use std::fmt::{self, Write as _};

use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::Filtered;
use tracing_subscriber::layer::{Context, Filter};

use weaver_server_core::runtime::job_debug_ring;

pub(crate) struct JobDebugRingLayer;

/// Whether an event belongs in a job's ring: DEBUG or finer, with a `job_id`.
pub(crate) fn captures(metadata: &Metadata<'_>) -> bool {
    metadata.is_event()
        && *metadata.level() >= Level::DEBUG
        && metadata.fields().field("job_id").is_some()
}

/// The ring's own filter. The verdict depends on the callsite alone, so it is
/// cached per callsite: one without a `job_id` is never asked about again.
pub(crate) struct JobDebugFilter;

impl<S> Filter<S> for JobDebugFilter {
    fn enabled(&self, metadata: &Metadata<'_>, _ctx: &Context<'_, S>) -> bool {
        captures(metadata)
    }

    fn callsite_enabled(&self, metadata: &'static Metadata<'static>) -> Interest {
        if captures(metadata) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::TRACE)
    }
}

/// The layer with its filter attached, ready to add to the registry.
pub(crate) fn layer<S>() -> Filtered<JobDebugRingLayer, JobDebugFilter, S>
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    job_debug_ring::install();
    JobDebugRingLayer.with_filter(JobDebugFilter)
}

impl<S: Subscriber> Layer<S> for JobDebugRingLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = LineVisitor::default();
        event.record(&mut visitor);
        let Some(job_id) = visitor.job_id else {
            return;
        };
        let metadata = event.metadata();
        job_debug_ring::record(job_id, visitor.render(metadata));
    }
}

#[derive(Default)]
struct LineVisitor {
    job_id: Option<u64>,
    message: String,
    fields: String,
}

impl LineVisitor {
    /// `LEVEL target: message field=value...`. The ring stamps the capture
    /// time itself and renders it only when the ring is dumped.
    fn render(self, metadata: &Metadata<'_>) -> String {
        let level = metadata.level().as_str();
        let target = metadata.target();
        let mut line = String::with_capacity(
            level.len() + target.len() + self.message.len() + self.fields.len() + 3,
        );
        line.push_str(level);
        line.push(' ');
        line.push_str(target);
        line.push_str(": ");
        line.push_str(&self.message);
        line.push_str(&self.fields);
        line
    }

    fn push_field(&mut self, field: &Field, value: fmt::Arguments<'_>) {
        if field.name() == "message" {
            let _ = self.message.write_fmt(value);
        } else {
            let _ = write!(self.fields, " {}={}", field.name(), value);
        }
    }
}

impl Visit for LineVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "job_id" {
            self.job_id = Some(value);
        }
        self.push_field(field, format_args!("{value}"));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "job_id" {
            self.job_id = u64::try_from(value).ok();
        }
        self.push_field(field, format_args!("{value}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "job_id" {
            self.job_id = value.trim().parse().ok();
        }
        self.push_field(field, format_args!("{value}"));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "job_id" {
            // `%job_id` and `?job_id` arrive here; a job id renders as its
            // number, possibly wrapped in the type's name.
            let rendered = format!("{value:?}");
            let digits: String = rendered.chars().filter(char::is_ascii_digit).collect();
            self.job_id = digits.parse().ok();
        }
        self.push_field(field, format_args!("{value:?}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn job_debug_events_are_captured_whatever_the_output_filter_says() {
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::sink)
                    .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
            )
            .with(layer());
        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(job_id = 91_001_u64, segment = 3, "fetched an article");
            tracing::trace!(job_id = 91_001_u64, "trace detail");
            tracing::debug!(other = 1, "no job here");
            tracing::info!(job_id = 91_001_u64, "info is not captured");
        });

        let lines: Vec<String> = job_debug_ring::take(91_001)
            .into_iter()
            .map(|line| line.text)
            .collect();
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(lines[0].starts_with("DEBUG "), "{lines:?}");
        assert!(lines[0].contains("fetched an article"), "{lines:?}");
        assert!(lines[0].contains("segment=3"), "{lines:?}");
        assert!(lines[1].contains("trace detail"), "{lines:?}");
    }

    /// WARN records whose message mentions stall diagnostics, as
    /// `(message, fields)`.
    #[derive(Clone, Default)]
    struct StallRecords(std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>);

    impl<S: Subscriber> Layer<S> for StallRecords {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = LineVisitor::default();
            event.record(&mut visitor);
            if *event.metadata().level() == Level::WARN
                && visitor.message.contains("stall diagnostics")
            {
                self.0
                    .lock()
                    .unwrap()
                    .push((visitor.message, visitor.fields));
            }
        }
    }

    #[test]
    fn a_dump_writes_the_ring_once_and_clears_it() {
        let records = StallRecords::default();
        let subscriber = tracing_subscriber::registry()
            .with(records.clone().with_filter(LevelFilter::INFO))
            .with(layer());
        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(job_id = 91_002_u64, "before the stall");
            tracing::debug!(job_id = 91_002_u64, "just before the stall");
            job_debug_ring::dump(91_002, "test stall");
            job_debug_ring::dump(91_002, "test stall");
        });

        let records = records.0.lock().unwrap();
        let headers: Vec<_> = records
            .iter()
            .filter(|(message, _)| message.starts_with("stall diagnostics:"))
            .collect();
        assert_eq!(
            headers.len(),
            1,
            "an emptied ring writes nothing: {records:?}"
        );
        assert!(headers[0].0.contains("2 recent debug lines"), "{records:?}");
        assert!(headers[0].1.contains("job_id=91002"), "{records:?}");

        let lines: Vec<&String> = records
            .iter()
            .filter(|(message, _)| message == "stall diagnostics line")
            .map(|(_, fields)| fields)
            .collect();
        assert_eq!(lines.len(), 2, "one record per captured line: {records:?}");
        assert!(lines[0].contains("before the stall"), "{records:?}");
        assert!(lines[1].contains("just before the stall"), "{records:?}");
        for fields in &lines {
            assert!(fields.contains("job_id=91002"), "{records:?}");
            assert!(fields.contains("reason=test stall"), "{records:?}");
            assert!(fields.contains(" at="), "{records:?}");
        }
        assert!(job_debug_ring::take(91_002).is_empty());
    }
}
