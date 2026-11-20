// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph's observability lines as a test hears them: every INFO event on
// this thread under the graph's target, rendered as the log writes it, the
// message then each field as `key=value` in the order the line names them.
// Any other target's lines at one level are heard the same way.

use std::fmt::Write;
use std::sync::{Arc, Mutex};

use tracing::subscriber::DefaultGuard;

const TARGET: &str = "weaver_server_pipeline::pipeline::graph";

#[derive(Clone)]
pub(crate) struct Lines {
    heard: Arc<Mutex<Vec<String>>>,
    target: &'static str,
    level: tracing::Level,
}

impl Lines {
    // Hear the graph's lines until the guard drops.
    pub(crate) fn capture() -> (Self, DefaultGuard) {
        Self::capture_at(TARGET, tracing::Level::INFO)
    }

    // Hear the lines at `level` under `target` until the guard drops.
    pub(crate) fn capture_at(target: &'static str, level: tracing::Level) -> (Self, DefaultGuard) {
        BESIDE.get_or_init(|| tracing::Dispatch::new(Beside));
        let lines = Self {
            heard: Arc::default(),
            target,
            level,
        };
        let guard = tracing::subscriber::set_default(lines.clone());
        (lines, guard)
    }

    // Every line heard with this message, as written.
    pub(crate) fn of(&self, message: &str) -> Vec<String> {
        let prefix = format!("{message} ");
        self.heard
            .lock()
            .unwrap()
            .iter()
            .filter(|line| line.starts_with(&prefix))
            .cloned()
            .collect()
    }
}

// A dispatcher alive for the whole run beside every capture. While only one
// dispatcher is alive, tracing registers a callsite first hit on a thread
// with the interest of that thread's own default alone (tracing-core
// `Dispatchers::rebuilder`, `has_just_one`): a test with no capture would
// cache `never` for a line a capture on another thread is waiting to hear.
// With this one alive too, every live dispatcher is asked.
static BESIDE: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();

struct Beside;

impl tracing::Subscriber for Beside {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

impl tracing::Subscriber for Lines {
    // Asked at every event, never cached for the callsite: tests on other
    // threads hold subscribers of their own, and an interest one of them
    // left cached would silence a line this one hears.
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().starts_with(self.target) && *metadata.level() == self.level
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
        self.heard
            .lock()
            .unwrap()
            .push(format!("{}{}", fields.message, fields.fields));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
