//! Facts about one server that only its own answers can establish.
//!
//! A download lane's connect exchange is pure latency: every command in it is
//! a round trip that runs before the first article byte can be asked for. The
//! minimum RFC 3977/4643 needs for `BODY <message-id>` is the greeting and
//! AUTHINFO USER/PASS — four round trips including the TCP handshake. Nothing
//! else is ever sent at setup, and in particular `MODE READER` never is: it is
//! only meaningful to a dual-mode (transit + reader) server that starts in
//! transit mode, weaver never posts or feeds, and paying a fifth round trip on
//! every connection to every provider to guard against that is a bad trade.
//!
//! What is recorded here instead is the far narrower set of things a server
//! can only tell us by refusing something:
//!
//! - It insists on a selected group even for a message-id fetch (412), which
//!   RFC 3977 does not require of it. Later connections then spend the GROUP
//!   round trip, and only for that server.
//! - It does not implement `STAT`, or does not implement `HEAD` (500).
//!   The existence probe uses whichever of the two the server does answer, and
//!   settles for the one verdict when only one is available.
//!
//! Refusals of this kind are answers, not faults: the connection that received
//! one is still perfectly healthy and keeps being used.
//!
//! The map is process-lifetime and deliberately not persisted: it is a
//! property of the server as it is answering right now, cheap to relearn, and
//! wrong to carry across a restart that may have moved the endpoint.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// What one server has proven about itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerCapabilities {
    /// The server refused a message-id fetch for want of a selected group.
    pub requires_group: bool,
    /// The server answers `STAT`. Assumed until it says otherwise.
    pub stat: bool,
    /// The server answers `HEAD`. Assumed until it says otherwise.
    pub head: bool,
}

impl Default for ServerCapabilities {
    fn default() -> Self {
        Self {
            requires_group: false,
            stat: true,
            head: true,
        }
    }
}

/// Whether `code` is a server saying it does not implement the command at all,
/// rather than answering it.
///
/// Only 500, "command not recognized", says that. Every other refusal is about
/// the one request, the article or the session, not about the command, and
/// none of them may retire `STAT` or `HEAD` for the rest of the process:
///
/// - 501 is a syntax error in the request just sent. A server that implements
///   the command perfectly well answers it to a message-id it cannot parse,
///   so one odd id in a batch must not push every later probe onto the
///   slower path.
/// - 502 is admission — a connection limit or an access denial.
/// - 480 is authentication, in both directions.
/// - 430 is simply the article not being here.
/// - 412 is the missing group, which is recorded separately.
pub fn is_command_unsupported(code: u16) -> bool {
    code == 500
}

type CapabilityMap = RwLock<HashMap<(String, u16), ServerCapabilities>>;

fn registry() -> &'static CapabilityMap {
    static REGISTRY: OnceLock<CapabilityMap> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// What this host has proven. Unknown servers start fully capable.
pub fn capabilities_for(host: &str, port: u16) -> ServerCapabilities {
    // A poisoned lock only means some thread panicked while holding it; the
    // map itself is a plain value, so reading through the poison is safe and
    // strictly better than refusing to connect.
    let map = match registry().read() {
        Ok(map) => map,
        Err(poisoned) => poisoned.into_inner(),
    };
    map.get(&(host.to_string(), port))
        .copied()
        .unwrap_or_default()
}

/// Whether this host has proven it refuses message-id fetches without a
/// selected group. Lanes skip the GROUP round trip unless it has.
pub fn requires_group_selection(host: &str, port: u16) -> bool {
    capabilities_for(host, port).requires_group
}

/// Whether `STAT` is worth sending to this host.
pub fn supports_stat(host: &str, port: u16) -> bool {
    capabilities_for(host, port).stat
}

/// Whether `HEAD` is worth sending to this host.
pub fn supports_head(host: &str, port: u16) -> bool {
    capabilities_for(host, port).head
}

/// Record that this host must have a group selected before a message-id
/// fetch. Returns true the first time, so the caller can log it exactly once.
pub fn note_group_required(host: &str, port: u16) -> bool {
    update(host, port, |caps| {
        let changed = !caps.requires_group;
        caps.requires_group = true;
        changed
    })
}

/// Record that this host does not implement `STAT`. Returns true the first
/// time.
pub fn note_stat_unsupported(host: &str, port: u16) -> bool {
    update(host, port, |caps| {
        let changed = caps.stat;
        caps.stat = false;
        changed
    })
}

/// Record that this host does not implement `HEAD`. Returns true the first
/// time.
pub fn note_head_unsupported(host: &str, port: u16) -> bool {
    update(host, port, |caps| {
        let changed = caps.head;
        caps.head = false;
        changed
    })
}

fn update(host: &str, port: u16, apply: impl FnOnce(&mut ServerCapabilities) -> bool) -> bool {
    let mut map = match registry().write() {
        Ok(map) => map,
        Err(poisoned) => poisoned.into_inner(),
    };
    let entry = map.entry((host.to_string(), port)).or_default();
    apply(entry)
}

/// Forget what a host proved. Tests own a port for their lifetime, so this
/// keeps one scripted server's verdict out of the next test's connect.
#[cfg(test)]
pub(crate) fn forget(host: &str, port: u16) {
    let mut map = match registry().write() {
        Ok(map) => map,
        Err(poisoned) => poisoned.into_inner(),
    };
    map.remove(&(host.to_string(), port));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_server_is_assumed_fully_capable() {
        let caps = capabilities_for("unknown.invalid", 119);
        assert_eq!(caps, ServerCapabilities::default());
        assert!(!caps.requires_group);
        assert!(caps.stat);
        assert!(caps.head);
    }

    #[test]
    fn only_500_means_the_command_is_missing() {
        assert!(is_command_unsupported(500));
        // A 501 is a syntax error in one request, so one unparseable
        // message-id must not retire the command; a full connection-limit
        // answer is about admission, a 480 is about credentials, a 430 is
        // the article simply not being here, and a 412 is the missing group.
        // None of them retire a command.
        assert!(!is_command_unsupported(501));
        assert!(!is_command_unsupported(502));
        assert!(!is_command_unsupported(480));
        assert!(!is_command_unsupported(430));
        assert!(!is_command_unsupported(412));
    }

    #[test]
    fn a_requirement_is_recorded_once_and_stays_recorded() {
        let host = "sticky.invalid";
        forget(host, 119);
        assert!(note_group_required(host, 119));
        assert!(!note_group_required(host, 119));
        assert!(requires_group_selection(host, 119));
        assert!(supports_stat(host, 119), "STAT is untouched by the group");
        assert!(
            !requires_group_selection(host, 563),
            "ports are distinct servers"
        );
        forget(host, 119);
        assert!(!requires_group_selection(host, 119));
    }

    #[test]
    fn a_retired_command_does_not_retire_the_other_one() {
        let host = "commands.invalid";
        forget(host, 119);
        assert!(note_stat_unsupported(host, 119));
        assert!(!note_stat_unsupported(host, 119));
        assert!(!supports_stat(host, 119));
        assert!(supports_head(host, 119));
        assert!(note_head_unsupported(host, 119));
        assert!(!supports_head(host, 119));
        assert!(
            !requires_group_selection(host, 119),
            "a missing command says nothing about groups"
        );
        forget(host, 119);
    }
}
