// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which engine runs a job: the classic pipeline or the graph engine
// (`weaver-graph`). Both exist side by side until the graph engine replaces
// the classic one, so every job is stamped with the engine it was admitted
// under and a restart keeps that engine whatever the switch says now.

pub(crate) mod engine;
pub(crate) mod envelope;
pub(crate) mod job;
pub(crate) mod leaf;
pub(crate) mod limits;
pub(crate) mod namespace;
pub(crate) mod root;
pub(crate) mod stand_in;
pub(crate) mod status;

use std::fmt;
use std::str::FromStr;
use std::sync::OnceLock;

// The switch for new jobs. It is read once per process so a job admitted
// mid-run never sees a different answer from one admitted at start-up.
const ENGINE_VARIABLE: &str = "WEAVER_ENGINE";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Engine {
    #[default]
    Classic,
    Graph,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownEngine(pub String);

impl fmt::Display for UnknownEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown engine {:?}", self.0)
    }
}

impl std::error::Error for UnknownEngine {}

impl Engine {
    // The stored and configured spelling, so the database stamp and the
    // environment variable share one vocabulary.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Classic => "classic",
            Self::Graph => "graph",
        }
    }

    // The engine new jobs get in this process. Cached on first use: the
    // variable is process-global and must not change a decision mid-run.
    pub fn from_environment() -> Self {
        static ENGINE: OnceLock<Engine> = OnceLock::new();
        *ENGINE.get_or_init(|| Self::parse(std::env::var(ENGINE_VARIABLE).ok().as_deref()))
    }

    // The decision behind `from_environment`, apart from the process
    // environment so tests can drive it without setting a global variable.
    // Unset or blank is classic; an unknown value is classic too, with a
    // warning, so a typo never stops the server from starting.
    pub fn parse(value: Option<&str>) -> Self {
        let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            return Self::Classic;
        };
        match value.parse() {
            Ok(engine) => engine,
            Err(UnknownEngine(value)) => {
                tracing::warn!(
                    value = %value,
                    "{ENGINE_VARIABLE} is not classic or graph; new jobs use the classic engine"
                );
                Self::Classic
            }
        }
    }

    // Whether the classic engine's own switches (`[direct_store]`,
    // `WEAVER_RAR_DIRECT_STORE` and `[direct_unpack]`) apply to a job on this
    // engine. The graph engine decides those things itself, so for a graph job
    // the switch takes precedence over them and they are not consulted.
    pub fn classic_switches_apply(self) -> bool {
        matches!(self, Self::Classic)
    }
}

impl FromStr for Engine {
    type Err = UnknownEngine;

    // Case-insensitive so `Graph` or `GRAPH` in an environment file works;
    // the database only ever holds the lowercase spelling.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.eq_ignore_ascii_case("classic") {
            Ok(Self::Classic)
        } else if value.eq_ignore_ascii_case("graph") {
            Ok(Self::Graph)
        } else {
            Err(UnknownEngine(value.to_string()))
        }
    }
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests;
