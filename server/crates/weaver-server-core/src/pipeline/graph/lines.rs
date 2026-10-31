// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph's observability lines as a test hears them: every INFO event on
// this thread under the graph's target, rendered as the log writes it, the
// message then each field as `key=value` in the order the line names them.

use std::fmt::Write;
use std::sync::{Arc, Mutex};

use tracing::subscriber::DefaultGuard;

const TARGET: &str = "weaver_server_core::pipeline::graph";

#[derive(Clone, Default)]
pub(crate) struct Lines(Arc<Mutex<Vec<String>>>);

impl Lines {
    // Hear the graph's lines until the guard drops.
    pub(crate) fn capture() -> (Self, DefaultGuard) {
        let lines = Self::default();
        let guard = tracing::subscriber::set_default(lines.clone());
        (lines, guard)
    }

    // Every line heard with this message, as written.
    pub(crate) fn of(&self, message: &str) -> Vec<String> {
        let prefix = format!("{message} ");
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|line| line.starts_with(&prefix))
            .cloned()
            .collect()
    }
}

impl tracing::Subscriber for Lines {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().starts_with(TARGET) && *metadata.level() == tracing::Level::INFO
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        #[derive(Default)]
        struct Fields {
            message: String,
            fields: String,
        }
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    let _ = write!(self.message, "{value:?}");
                } else {
                    let _ = write!(self.fields, " {field}={value:?}");
                }
            }
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0
            .lock()
            .unwrap()
            .push(format!("{}{}", fields.message, fields.fields));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
