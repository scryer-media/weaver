//! What a server needs said to it before the first BODY, learned once.
//!
//! A download lane's connect exchange is pure latency: every command in it is
//! a round trip that runs before the first article byte can be asked for. The
//! minimum RFC 3977/4643 needs for `BODY <message-id>` is the greeting and
//! AUTHINFO USER/PASS — four round trips including the TCP handshake.
//! `MODE READER` is only meaningful to a dual-mode (transit + reader) server
//! that starts in transit mode, and a selected group is only meaningful for
//! article-number access, which no download lane uses.
//!
//! So both are dropped from the default prologue and learned instead: if the
//! first command on a connection that skipped them comes back with the status
//! a server in the wrong mode answers with, the requirement is recorded here
//! and every later connection to that host pays for it. Only the first
//! connection to such a server loses an article to the probe; every server
//! that does not need the commands never pays for them again.
//!
//! The map is process-lifetime and deliberately not persisted: it is a
//! property of the server as it is answering right now, cheap to relearn, and
//! wrong to carry across a restart that may have moved the endpoint.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// The prologue commands one server has proven it needs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerPrologue {
    /// The server answered reader commands as if it were not in reader mode.
    pub mode_reader: bool,
    /// The server refused a message-id fetch for want of a selected group.
    pub group: bool,
}

impl ServerPrologue {
    /// Whether anything at all has to be sent beyond authentication.
    pub fn is_minimal(&self) -> bool {
        !self.mode_reader && !self.group
    }
}

/// What a refused first command says the server was missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrologueRequirement {
    ModeReader,
    Group,
}

/// Armed on a connection that skipped part of the prologue, and disarmed by
/// the first response after setup — the only response that can still be about
/// the prologue rather than about the article.
#[derive(Debug, Clone, Copy, Default)]
pub struct PrologueProbe {
    pub skipped_mode_reader: bool,
    pub skipped_group: bool,
}

impl PrologueProbe {
    /// Whether `code` is a server saying it was not ready for the command,
    /// rather than answering it.
    ///
    /// The set is deliberately narrow, because every code that also has an
    /// ordinary meaning would put a permanent extra round trip on a server
    /// that never needed one:
    ///
    /// - 502 is admission, not mode: weaver already reads every 502 as a
    ///   connection-limit or access-denied answer (`NntpError::from_status`),
    ///   and a merely full server would be misread as a transit-mode one.
    /// - 480 is authentication, in both directions: on an authenticated
    ///   connection it is an expired session, and on an unauthenticated one it
    ///   is a server asking for credentials weaver was not given.
    /// - 430 is simply the article not being here.
    pub fn requirement_for(&self, code: u16) -> Option<PrologueRequirement> {
        match code {
            // 500/501: the command is unknown or unusable here, which is how
            // a dual-mode server still in transit mode answers a reader
            // command, and is not something a reader-mode server says to a
            // well-formed BODY.
            500 | 501 if self.skipped_mode_reader => Some(PrologueRequirement::ModeReader),
            // 412: the server insists on a selected group even for a
            // message-id fetch, which RFC 3977 does not require of it.
            412 if self.skipped_group => Some(PrologueRequirement::Group),
            _ => None,
        }
    }
}

type PrologueMap = RwLock<HashMap<(String, u16), ServerPrologue>>;

fn registry() -> &'static PrologueMap {
    static REGISTRY: OnceLock<PrologueMap> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// What this host has proven it needs. Unknown servers start minimal.
pub fn prologue_for(host: &str, port: u16) -> ServerPrologue {
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

/// Record that this host must be sent `MODE READER`. Returns true the first
/// time, so the caller can log the discovery exactly once.
pub fn note_mode_reader_required(host: &str, port: u16) -> bool {
    update(host, port, |prologue| {
        let changed = !prologue.mode_reader;
        prologue.mode_reader = true;
        changed
    })
}

/// Record that this host must have a group selected before a message-id
/// fetch. Returns true the first time.
pub fn note_group_required(host: &str, port: u16) -> bool {
    update(host, port, |prologue| {
        let changed = !prologue.group;
        prologue.group = true;
        changed
    })
}

fn update(host: &str, port: u16, apply: impl FnOnce(&mut ServerPrologue) -> bool) -> bool {
    let mut map = match registry().write() {
        Ok(map) => map,
        Err(poisoned) => poisoned.into_inner(),
    };
    let entry = map.entry((host.to_string(), port)).or_default();
    apply(entry)
}

/// Forget what a host proved. Tests own a port for their lifetime, so this
/// keeps one scripted server's fallback out of the next test's connect.
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
    fn an_unknown_server_starts_with_the_minimal_prologue() {
        let prologue = prologue_for("unknown.invalid", 119);
        assert_eq!(prologue, ServerPrologue::default());
        assert!(prologue.is_minimal());
    }

    #[test]
    fn only_a_skipped_command_can_be_blamed_for_a_refusal() {
        let skipped_both = PrologueProbe {
            skipped_mode_reader: true,
            skipped_group: true,
        };
        assert_eq!(
            skipped_both.requirement_for(500),
            Some(PrologueRequirement::ModeReader)
        );
        assert_eq!(
            skipped_both.requirement_for(412),
            Some(PrologueRequirement::Group)
        );
        // A full connection-limit answer is about admission, a 480 is about
        // credentials, and a 430 is the article simply not being here. None
        // of them may cost every later connection a round trip.
        assert_eq!(skipped_both.requirement_for(502), None);
        assert_eq!(skipped_both.requirement_for(480), None);
        assert_eq!(skipped_both.requirement_for(430), None);

        let skipped_mode_reader_only = PrologueProbe {
            skipped_mode_reader: true,
            skipped_group: false,
        };
        assert_eq!(
            skipped_mode_reader_only.requirement_for(412),
            None,
            "a 412 cannot be blamed on a GROUP that was never offered"
        );

        let sent_everything = PrologueProbe::default();
        assert_eq!(sent_everything.requirement_for(500), None);
    }

    #[test]
    fn a_requirement_is_recorded_once_and_stays_recorded() {
        let host = "sticky.invalid";
        forget(host, 119);
        assert!(note_mode_reader_required(host, 119));
        assert!(!note_mode_reader_required(host, 119));
        assert!(prologue_for(host, 119).mode_reader);
        assert!(!prologue_for(host, 119).group);
        assert!(!prologue_for(host, 563).mode_reader, "ports are distinct");
        forget(host, 119);
        assert!(prologue_for(host, 119).is_minimal());
    }
}
