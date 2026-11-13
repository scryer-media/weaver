// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Prometheus text-exposition encoder.
//
// The exporter used to be a long run of `push_str` calls that interleaved
// `# HELP`/`# TYPE` comments with samples by hand. That shape let a missing
// newline swallow a whole metric family, and it let new samples ship with no
// descriptor at all. Here a sample can only be written *through* its
// [`MetricFamily`], and the encoder emits the descriptor the first time it
// sees one — so "sample without HELP/TYPE" and "duplicate HELP/TYPE" are both
// unrepresentable.
//
// Each family's lines are kept in a text of their own and joined, in the
// order the families were first seen, when the page is finished. A renderer
// that walks servers, jobs or egresses and writes several families for each
// one therefore still sends every family as one uninterrupted group, as the
// exposition format requires; a scraper that keeps one group per family name
// would otherwise see only the last server's lines.

use std::collections::HashMap;
use std::fmt::Display;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

// One metric family: the unit that owns a `# HELP` and a `# TYPE` line.
//
// `labels` is the declared label set. It documents the family and is the
// thing the catalogue test checks the rendered output against; the
// encoder does not enforce it, because state-set families legitimately vary
// which label values appear.
#[derive(Debug)]
pub(crate) struct MetricFamily {
    pub(crate) name: &'static str,
    pub(crate) kind: MetricKind,
    // Read by the catalogue and documentation tests rather than by rendering.
    #[allow(dead_code)]
    pub(crate) labels: &'static [&'static str],
    pub(crate) help: &'static str,
    // Set when this family is kept only for backwards compatibility. The
    // replacement name is appended to the rendered HELP text and drives the
    // naming-convention allow-list in the exposition tests.
    pub(crate) deprecated_by: Option<&'static str>,
}

pub(crate) struct Encoder {
    // One text per family, HELP and TYPE first, in first-seen order.
    groups: Vec<String>,
    index: HashMap<&'static str, usize>,
}

impl Encoder {
    pub(crate) fn new() -> Self {
        Self {
            groups: Vec::with_capacity(256),
            index: HashMap::with_capacity(256),
        }
    }

    // The text of `family`, its descriptor written the first time.
    fn group(&mut self, family: &'static MetricFamily) -> &mut String {
        let next = self.groups.len();
        let slot = *self.index.entry(family.name).or_insert(next);
        if slot == next {
            self.groups.push(descriptor(family));
        }
        &mut self.groups[slot]
    }

    // Write one sample whose value renders through [`Display`] — integers and
    // booleans-as-integers. Floats must use [`Encoder::sample_f64`] so that
    // non-finite values get their Prometheus spellings.
    pub(crate) fn sample<T: Display>(
        &mut self,
        family: &'static MetricFamily,
        labels: &[(&str, &str)],
        value: T,
    ) {
        write_line(
            self.group(family),
            family.name,
            "",
            labels,
            &value.to_string(),
        );
    }

    pub(crate) fn sample_f64(
        &mut self,
        family: &'static MetricFamily,
        labels: &[(&str, &str)],
        value: f64,
    ) {
        write_line(
            self.group(family),
            family.name,
            "",
            labels,
            &format_prometheus_f64(value),
        );
    }

    // Emit a histogram family from a bucketed snapshot.
    //
    // `counts` holds the per-bucket (not cumulative) observation counts and is
    // one longer than `bounds`; the trailing entry is the `+Inf` overflow
    // bucket. This is the shape the pipeline's latency recorders produce, so
    // the conversion to Prometheus' cumulative `le` series lives here rather
    // than at every call site.
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
        let out = self.group(family);

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
            write_line(
                out,
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
        write_line(
            out,
            family.name,
            "_bucket",
            &bucket_labels,
            &cumulative.to_string(),
        );
        write_line(
            out,
            family.name,
            "_sum",
            labels,
            &format_prometheus_f64(sum),
        );
        write_line(out, family.name, "_count", labels, &count.to_string());
    }

    pub(crate) fn finish(self) -> String {
        self.groups.concat()
    }
}

// A family's `# HELP` and `# TYPE` lines.
fn descriptor(family: &MetricFamily) -> String {
    let mut out = String::with_capacity(family.help.len() + 2 * family.name.len() + 32);
    out.push_str("# HELP ");
    out.push_str(family.name);
    out.push(' ');
    out.push_str(family.help);
    if let Some(replacement) = family.deprecated_by {
        out.push_str(" (deprecated: use ");
        out.push_str(replacement);
        out.push(')');
    }
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(family.name);
    out.push(' ');
    out.push_str(family.kind.as_str());
    out.push('\n');
    out
}

fn write_line(out: &mut String, name: &str, suffix: &str, labels: &[(&str, &str)], value: &str) {
    out.push_str(name);
    out.push_str(suffix);
    append_labels(out, labels);
    out.push(' ');
    out.push_str(value);
    out.push('\n');
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
