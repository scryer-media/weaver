//! Prometheus text-exposition encoder.
//!
//! The exporter used to be a long run of `push_str` calls that interleaved
//! `# HELP`/`# TYPE` comments with samples by hand. That shape let a missing
//! newline swallow a whole metric family, and it let new samples ship with no
//! descriptor at all. Here a sample can only be written *through* its
//! [`MetricFamily`], and the encoder emits the descriptor the first time it
//! sees one — so "sample without HELP/TYPE" and "duplicate HELP/TYPE" are both
//! unrepresentable.

use std::collections::HashSet;
use std::fmt::Display;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum MetricKind {
    Counter,
    Gauge,
    Summary,
    Histogram,
}

impl MetricKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Summary => "summary",
            Self::Histogram => "histogram",
        }
    }
}

/// One metric family: the unit that owns a `# HELP` and a `# TYPE` line.
///
/// `labels` is the declared label set. It is documentation for `docs/metrics.md`
/// and the thing the catalogue test checks the rendered output against; the
/// encoder does not enforce it, because state-set families legitimately vary
/// which label values appear.
#[derive(Debug)]
pub(crate) struct MetricFamily {
    pub(crate) name: &'static str,
    pub(crate) kind: MetricKind,
    /// Read by the catalogue and documentation tests rather than by rendering.
    #[allow(dead_code)]
    pub(crate) labels: &'static [&'static str],
    pub(crate) help: &'static str,
    /// Set when this family is kept only for backwards compatibility. The
    /// replacement name is appended to the rendered HELP text and drives the
    /// naming-convention allow-list in the exposition tests.
    pub(crate) deprecated_by: Option<&'static str>,
}

pub(crate) struct Encoder {
    out: String,
    declared: HashSet<&'static str>,
}

impl Encoder {
    pub(crate) fn new() -> Self {
        Self {
            out: String::with_capacity(32 * 1024),
            declared: HashSet::with_capacity(256),
        }
    }

    /// Emit `# HELP`/`# TYPE` for `family` unless they were already written.
    ///
    /// Callers rarely need this: every `sample*` entry point calls it. It is
    /// public so a family whose samples are all conditional can still be
    /// declared deliberately.
    pub(crate) fn family(&mut self, family: &'static MetricFamily) {
        if !self.declared.insert(family.name) {
            return;
        }
        self.out.push_str("# HELP ");
        self.out.push_str(family.name);
        self.out.push(' ');
        self.out.push_str(family.help);
        if let Some(replacement) = family.deprecated_by {
            self.out.push_str(" (deprecated: use ");
            self.out.push_str(replacement);
            self.out.push(')');
        }
        self.out.push('\n');
        self.out.push_str("# TYPE ");
        self.out.push_str(family.name);
        self.out.push(' ');
        self.out.push_str(family.kind.as_str());
        self.out.push('\n');
    }

    /// Write one sample whose value renders through [`Display`] — integers and
    /// booleans-as-integers. Floats must use [`Encoder::sample_f64`] so that
    /// non-finite values get their Prometheus spellings.
    pub(crate) fn sample<T: Display>(
        &mut self,
        family: &'static MetricFamily,
        labels: &[(&str, &str)],
        value: T,
    ) {
        self.family(family);
        self.write_line(family.name, "", labels, &value.to_string());
    }

    pub(crate) fn sample_f64(
        &mut self,
        family: &'static MetricFamily,
        labels: &[(&str, &str)],
        value: f64,
    ) {
        self.family(family);
        self.write_line(family.name, "", labels, &format_prometheus_f64(value));
    }

    /// Emit the `_sum`/`_count` pair of a summary family.
    pub(crate) fn summary(
        &mut self,
        family: &'static MetricFamily,
        labels: &[(&str, &str)],
        sum: f64,
        count: u64,
    ) {
        debug_assert_eq!(family.kind, MetricKind::Summary);
        self.family(family);
        self.write_line(family.name, "_sum", labels, &format_prometheus_f64(sum));
        self.write_line(family.name, "_count", labels, &count.to_string());
    }

    /// Emit a histogram family from a bucketed snapshot.
    ///
    /// `counts` holds the per-bucket (not cumulative) observation counts and is
    /// one longer than `bounds`; the trailing entry is the `+Inf` overflow
    /// bucket. This is the shape the pipeline's latency recorders produce, so
    /// the conversion to Prometheus' cumulative `le` series lives here rather
    /// than at every call site.
    pub(crate) fn histogram(
        &mut self,
        family: &'static MetricFamily,
        labels: &[(&str, &str)],
        bounds: &[f64],
        counts: &[u64],
        sum: f64,
        count: u64,
    ) {
        debug_assert_eq!(family.kind, MetricKind::Histogram);
        debug_assert_eq!(counts.len(), bounds.len() + 1);
        self.family(family);

        // Render the bounds up front: the `le` label borrows these strings, so
        // they have to outlive the loop that pushes them into the label slice.
        let rendered_bounds: Vec<String> = bounds
            .iter()
            .map(|bound| format_prometheus_f64(*bound))
            .collect();

        let mut cumulative: u64 = 0;
        let mut bucket_labels: Vec<(&str, &str)> = Vec::with_capacity(labels.len() + 1);
        for (index, le) in rendered_bounds.iter().enumerate() {
            cumulative = cumulative.saturating_add(counts.get(index).copied().unwrap_or(0));
            bucket_labels.clear();
            bucket_labels.extend_from_slice(labels);
            bucket_labels.push(("le", le.as_str()));
            self.write_line(
                family.name,
                "_bucket",
                &bucket_labels,
                &cumulative.to_string(),
            );
        }
        cumulative = cumulative.saturating_add(counts.last().copied().unwrap_or(0));
        bucket_labels.clear();
        bucket_labels.extend_from_slice(labels);
        bucket_labels.push(("le", "+Inf"));
        self.write_line(
            family.name,
            "_bucket",
            &bucket_labels,
            &cumulative.to_string(),
        );
        self.write_line(family.name, "_sum", labels, &format_prometheus_f64(sum));
        self.write_line(family.name, "_count", labels, &count.to_string());
    }

    pub(crate) fn finish(self) -> String {
        self.out
    }

    fn write_line(&mut self, name: &str, suffix: &str, labels: &[(&str, &str)], value: &str) {
        self.out.push_str(name);
        self.out.push_str(suffix);
        append_labels(&mut self.out, labels);
        self.out.push(' ');
        self.out.push_str(value);
        self.out.push('\n');
    }
}

fn append_labels(out: &mut String, labels: &[(&str, &str)]) {
    if labels.is_empty() {
        return;
    }
    out.push('{');
    for (idx, (key, value)) in labels.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(key);
        out.push_str("=\"");
        push_escaped_label_value(out, value);
        out.push('"');
    }
    out.push('}');
}

#[cfg(test)]
pub(crate) fn escape_prometheus_label_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    push_escaped_label_value(&mut escaped, value);
    escaped
}

fn push_escaped_label_value(out: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
}

pub(crate) fn format_prometheus_f64(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else if value.is_nan() {
        "NaN".to_string()
    } else if value.is_sign_negative() {
        "-Inf".to_string()
    } else {
        "+Inf".to_string()
    }
}
