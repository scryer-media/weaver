//! The process-wide log filter, adjustable while the process runs.
//!
//! The binary owns the tracing subscriber, so it owns the filter too: at
//! startup it installs a function that validates a directive string and swaps
//! it into the live subscriber. This module keeps that function and the
//! directives currently in force, so an operator surface can read and change
//! the filter without a restart.
//!
//! A change lasts until the process exits. Nothing is persisted: a restart
//! always comes back up on the directives it was started with.

use std::sync::{Mutex, OnceLock};

use tracing::info;

/// Validates a directive string and, if it parses, makes it the live filter.
/// Returns the parse error as text otherwise, leaving the live filter as it
/// was.
pub type ApplyDirectives = Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LogFilterError {
    #[error("the log filter cannot be changed in this process")]
    NotInstalled,
    #[error("invalid log filter directives: {0}")]
    Invalid(String),
}

/// The directives in force and the ones the process started with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFilterState {
    pub directives: String,
    pub default_directives: String,
}

/// One adjustable filter: the apply function and what it last applied.
pub struct LogFilterControl {
    apply: ApplyDirectives,
    default_directives: String,
    current: Mutex<String>,
}

impl LogFilterControl {
    pub fn new(default_directives: String, apply: ApplyDirectives) -> Self {
        Self {
            apply,
            current: Mutex::new(default_directives.clone()),
            default_directives,
        }
    }

    pub fn state(&self) -> LogFilterState {
        LogFilterState {
            directives: self.current_lock().clone(),
            default_directives: self.default_directives.clone(),
        }
    }

    /// Makes `directives` the live filter. Blank input restores the startup
    /// directives. Invalid input changes nothing.
    pub fn set(&self, directives: &str) -> Result<LogFilterState, LogFilterError> {
        let requested = directives.trim();
        let requested = if requested.is_empty() {
            self.default_directives.clone()
        } else {
            requested.to_string()
        };
        let mut current = self.current_lock();
        (self.apply)(&requested).map_err(LogFilterError::Invalid)?;
        let previous = std::mem::replace(&mut *current, requested.clone());
        drop(current);
        info!(
            previous = %previous,
            directives = %requested,
            "log filter changed"
        );
        Ok(self.state())
    }

    fn current_lock(&self) -> std::sync::MutexGuard<'_, String> {
        self.current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

static CONTROL: OnceLock<LogFilterControl> = OnceLock::new();

/// Registers the process's filter. The first call wins; a later one (a second
/// subscriber in a test, say) is ignored and returns `false`.
pub fn install(default_directives: String, apply: ApplyDirectives) -> bool {
    CONTROL
        .set(LogFilterControl::new(default_directives, apply))
        .is_ok()
}

/// The directives in force, or `None` when no filter was installed.
pub fn current_directives() -> Option<LogFilterState> {
    CONTROL.get().map(LogFilterControl::state)
}

/// Replaces the live filter. See [`LogFilterControl::set`].
pub fn set_directives(directives: &str) -> Result<LogFilterState, LogFilterError> {
    CONTROL
        .get()
        .ok_or(LogFilterError::NotInstalled)?
        .set(directives)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn control(applied: Arc<Mutex<Vec<String>>>) -> LogFilterControl {
        LogFilterControl::new(
            "info".to_string(),
            Box::new(move |directives| {
                if directives.contains('!') {
                    return Err(format!("cannot parse {directives}"));
                }
                applied.lock().unwrap().push(directives.to_string());
                Ok(())
            }),
        )
    }

    #[test]
    fn a_valid_change_is_applied_and_reported() {
        let applied = Arc::new(Mutex::new(Vec::new()));
        let control = control(applied.clone());

        let state = control.set("  info,weaver_server_core=debug ").unwrap();

        assert_eq!(state.directives, "info,weaver_server_core=debug");
        assert_eq!(state.default_directives, "info");
        assert_eq!(
            *applied.lock().unwrap(),
            vec!["info,weaver_server_core=debug".to_string()]
        );
    }

    #[test]
    fn an_invalid_change_leaves_the_filter_alone() {
        let applied = Arc::new(Mutex::new(Vec::new()));
        let control = control(applied.clone());
        control.set("debug").unwrap();

        let error = control.set("debug!").unwrap_err();

        assert!(matches!(error, LogFilterError::Invalid(_)));
        assert_eq!(control.state().directives, "debug");
        assert_eq!(*applied.lock().unwrap(), vec!["debug".to_string()]);
    }

    #[test]
    fn blank_input_restores_the_startup_directives() {
        let applied = Arc::new(Mutex::new(Vec::new()));
        let control = control(applied.clone());
        control.set("trace").unwrap();

        let state = control.set("   ").unwrap();

        assert_eq!(state.directives, "info");
        assert_eq!(
            *applied.lock().unwrap(),
            vec!["trace".to_string(), "info".to_string()]
        );
    }
}
