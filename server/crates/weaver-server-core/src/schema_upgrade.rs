//! How far a database schema upgrade has got.
//!
//! Migrations run before Weaver can serve anything, so a long upgrade looks,
//! from a browser, exactly like a Weaver that failed to start. Publishing its
//! progress lets the binary put a page up in the meantime. Only an upgrade
//! publishes: a fresh install has nobody waiting on it.

use std::sync::LazyLock;

use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaUpgrade {
    /// No upgrade is being applied.
    Idle,
    /// `applied` of the `total` pending migrations are in.
    Running { applied: usize, total: usize },
}

static PROGRESS: LazyLock<watch::Sender<SchemaUpgrade>> =
    LazyLock::new(|| watch::Sender::new(SchemaUpgrade::Idle));

/// Follow schema upgrades in this process, starting from the current state.
pub fn subscribe() -> watch::Receiver<SchemaUpgrade> {
    PROGRESS.subscribe()
}

/// One upgrade being published. Dropping it returns the state to idle, so an
/// upgrade that fails part way still ends.
pub(crate) struct UpgradeProgress {
    sender: &'static watch::Sender<SchemaUpgrade>,
    applied: usize,
    total: usize,
}

impl UpgradeProgress {
    pub(crate) fn start(total: usize) -> Self {
        Self::start_on(&PROGRESS, total)
    }

    fn start_on(sender: &'static watch::Sender<SchemaUpgrade>, total: usize) -> Self {
        sender.send_replace(SchemaUpgrade::Running { applied: 0, total });
        Self {
            sender,
            applied: 0,
            total,
        }
    }

    pub(crate) fn advance(&mut self) {
        self.applied = (self.applied + 1).min(self.total);
        self.sender.send_replace(SchemaUpgrade::Running {
            applied: self.applied,
            total: self.total,
        });
    }
}

impl Drop for UpgradeProgress {
    fn drop(&mut self) {
        self.sender.send_replace(SchemaUpgrade::Idle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_upgrade_counts_up_and_ends_idle() {
        // Its own channel: migration tests elsewhere in the crate publish to
        // the process-wide one concurrently.
        let sender = Box::leak(Box::new(watch::Sender::new(SchemaUpgrade::Idle)));
        let receiver = sender.subscribe();
        let mut progress = UpgradeProgress::start_on(sender, 2);
        assert_eq!(
            *receiver.borrow(),
            SchemaUpgrade::Running {
                applied: 0,
                total: 2
            }
        );
        progress.advance();
        progress.advance();
        progress.advance();
        assert_eq!(
            *receiver.borrow(),
            SchemaUpgrade::Running {
                applied: 2,
                total: 2
            }
        );
        drop(progress);
        assert_eq!(*receiver.borrow(), SchemaUpgrade::Idle);
    }
}
