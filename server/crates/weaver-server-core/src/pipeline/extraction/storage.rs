use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::output::channel::StreamOwner;
use super::safety::MemoryPermit;
use super::session::{ArchiveSessions, SessionError};
use crate::pipeline::direct_store::ByteRanges;
use crate::pipeline::direct_store::router::{DirectDestination, SpanOwner};

// Session identity qualifies the local routing graph. A raw-child marker is
// reconstruction metadata, not part of a physical destination's identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Destination {
    pub(crate) namespace: StreamOwner,
    pub(crate) owner: SpanOwner,
    pub(crate) destination: DirectDestination,
}

type Key = (u64, u32, u64, u32, u64, u8, u32);

impl Destination {
    fn key(self) -> Key {
        let (kind, id) = match self.destination {
            DirectDestination::Member { member_id } => (0, member_id),
            DirectDestination::Envelope { volume_index } => (1, volume_index),
        };
        (
            self.namespace.root,
            self.namespace.node,
            self.namespace.generation,
            self.owner.node,
            self.owner.generation,
            kind,
            id,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PhysicalFileId(u32);

struct PhysicalFile {
    relative: PathBuf,
    length: Option<u64>,
    sealed: bool,
    placed: ByteRanges,
    range_slots: u64,
    #[cfg(test)]
    readers: Arc<()>,
}

// Only the dispatcher can mint a ticket. The I/O worker returns it after the
// whole write succeeds; forwarding bytes between engines never completes one.
pub(crate) struct WriteTicket {
    registry: Arc<()>,
    sequence: u64,
}

struct PendingWrite {
    destination: Destination,
    file: PhysicalFileId,
    offset: u64,
    len: u64,
}

#[cfg(test)]
pub(crate) struct PhysicalClaim {
    pub(crate) id: PhysicalFileId,
    pub(crate) relative: PathBuf,
    pub(crate) length: Option<u64>,
    pub(crate) sealed: bool,
    pub(crate) placed: ByteRanges,
    // Frozen checkpoint candidates retain the exact files they describe.
    pub(crate) readers: Arc<()>,
}

#[cfg(test)]
pub(crate) struct PhysicalSnapshot {
    pub(crate) files: Vec<PhysicalClaim>,
    _admission: MemoryPermit,
}

// This records physical placement only. Root source floors are derived by the
// graph and must pass a separate complete-reconstruction check before SQL CAS.
// No state here is inferred from apparent file length or decoder completion.
pub(crate) struct DestinationRegistry {
    identity: Arc<()>,
    admission: MemoryPermit,
    charged: u64,
    files: BTreeMap<PhysicalFileId, PhysicalFile>,
    locations: BTreeMap<String, PhysicalFileId>,
    destinations: BTreeMap<Key, PhysicalFileId>,
    pending: BTreeMap<u64, PendingWrite>,
    next_write: u64,
}

const FILE_CHARGE: u64 = 512;
const RANGE_CHARGE: u64 = 64;
const WRITE_CHARGE: u64 = 320;
// Account for the initial multi-slot nodes of all four trees before the first
// entry, in addition to conservative per-entry charges for subsequent growth.
const REGISTRY_CHARGE: u64 = 8192;

// Private physical names are portable ASCII identifiers, independent of the
// logical archive filename. This excludes platform-specific Unicode aliases,
// alternate streams, device names and trailing-dot/space normalization.
fn private_path(text: &str) -> bool {
    text.len() <= 4096
        && text.split('/').all(|part| {
            if part.is_empty()
                || part.len() > 255
                || part.ends_with('.')
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
            {
                return false;
            }
            let stem = part.split('.').next().unwrap_or_default();
            if ["con", "prn", "aux", "nul"]
                .iter()
                .any(|name| stem.eq_ignore_ascii_case(name))
            {
                return false;
            }
            !(stem.len() == 4
                && (stem[..3].eq_ignore_ascii_case("com") || stem[..3].eq_ignore_ascii_case("lpt"))
                && matches!(stem.as_bytes()[3], b'1'..=b'9'))
        })
}

impl DestinationRegistry {
    pub(crate) fn new(admission: MemoryPermit) -> Result<Self, SessionError> {
        if admission.bytes() < REGISTRY_CHARGE {
            return Err(SessionError::Admission);
        }
        Ok(Self {
            identity: Arc::new(()),
            admission,
            charged: REGISTRY_CHARGE,
            files: BTreeMap::new(),
            locations: BTreeMap::new(),
            destinations: BTreeMap::new(),
            pending: BTreeMap::new(),
            next_write: 0,
        })
    }

    fn charge(&mut self, bytes: u64) -> Result<(), SessionError> {
        let next = self
            .charged
            .checked_add(bytes)
            .ok_or(SessionError::Admission)?;
        if next > self.admission.bytes() {
            return Err(SessionError::Admission);
        }
        self.charged = next;
        Ok(())
    }

    pub(crate) fn register(
        &mut self,
        sessions: &ArchiveSessions,
        destination: Destination,
        relative: &Path,
        length: Option<u64>,
    ) -> Result<PhysicalFileId, SessionError> {
        if !sessions.accepts(destination.namespace) {
            return Err(SessionError::Stale);
        }
        let text = relative.to_str().ok_or(SessionError::InvalidTransition)?;
        if !private_path(text) {
            return Err(SessionError::InvalidTransition);
        }
        if let Some(id) = self.destinations.get(&destination.key()) {
            let file = &self.files[id];
            return if file.relative.as_os_str() == relative.as_os_str() && file.length == length {
                Ok(*id)
            } else {
                Err(SessionError::InvalidTransition)
            };
        }
        // Preflight both retained metadata and temporary normalization before
        // allocating. The registry already owns this fixed memory admission.
        let bytes = FILE_CHARGE + (text.len() as u64) * 5;
        if self
            .charged
            .checked_add(bytes)
            .is_none_or(|next| next > self.admission.bytes())
        {
            return Err(SessionError::Admission);
        }
        // Sharing a path never silently creates an alias between generations.
        let location = text.to_lowercase();
        let prefix = format!("{location}/");
        if self.locations.contains_key(&location)
            || self
                .locations
                .range(prefix.clone()..)
                .next()
                .is_some_and(|(path, _)| path.starts_with(&prefix))
            || location
                .match_indices('/')
                .any(|(index, _)| self.locations.contains_key(&location[..index]))
            || self.files.len() >= 65_536
        {
            return Err(SessionError::InvalidTransition);
        }
        self.charge(bytes)?;
        let id = PhysicalFileId(self.files.len() as u32);
        self.files.insert(
            id,
            PhysicalFile {
                relative: relative.to_owned(),
                length,
                sealed: false,
                placed: ByteRanges::default(),
                range_slots: 0,
                #[cfg(test)]
                readers: Arc::new(()),
            },
        );
        self.locations.insert(location, id);
        self.destinations.insert(destination.key(), id);
        Ok(id)
    }

    pub(crate) fn begin_write(
        &mut self,
        sessions: &ArchiveSessions,
        destination: Destination,
        offset: u64,
        len: u64,
    ) -> Result<WriteTicket, SessionError> {
        if !sessions.accepts(destination.namespace) {
            return Err(SessionError::Stale);
        }
        let file = *self
            .destinations
            .get(&destination.key())
            .ok_or(SessionError::Stale)?;
        let end = offset
            .checked_add(len)
            .ok_or(SessionError::InvalidTransition)?;
        if self.files[&file].sealed || self.files[&file].length.is_some_and(|length| end > length) {
            return Err(SessionError::InvalidTransition);
        }
        // Backings are immutable once claimed. Retried/repaired bytes select a
        // new file generation; they cannot overwrite evidence a reader holds.
        if self.files[&file]
            .placed
            .ranges()
            .iter()
            .any(|&(start, stop)| offset < stop && start < end)
            || self.pending.values().any(|write| {
                write.file == file && offset < write.offset + write.len && write.offset < end
            })
        {
            return Err(SessionError::InvalidTransition);
        }
        if self.pending.len() >= 4096 {
            return Err(SessionError::Admission);
        }
        let next = self
            .next_write
            .checked_add(1)
            .ok_or(SessionError::Admission)?;
        // Reserve the worst-case extra range before dispatch. An I/O success
        // must never become unrecordable because metadata admission ran out.
        self.charge(WRITE_CHARGE + RANGE_CHARGE)?;
        self.next_write = next;
        self.pending.insert(
            next,
            PendingWrite {
                destination,
                file,
                offset,
                len,
            },
        );
        Ok(WriteTicket {
            registry: Arc::clone(&self.identity),
            sequence: next,
        })
    }

    #[cfg(test)]
    pub(crate) fn write_target(
        &self,
        ticket: &WriteTicket,
    ) -> Result<(&Path, u64, u64), SessionError> {
        if !Arc::ptr_eq(&self.identity, &ticket.registry) {
            return Err(SessionError::Stale);
        }
        let pending = self
            .pending
            .get(&ticket.sequence)
            .ok_or(SessionError::Stale)?;
        Ok((
            &self.files[&pending.file].relative,
            pending.offset,
            pending.len,
        ))
    }

    // Local graph retirement removes only the binding. Its physical evidence
    // and readers remain until the replacement checkpoint retires that image.
    #[cfg(test)]
    pub(crate) fn retire(&mut self, destination: Destination) {
        self.destinations.remove(&destination.key());
    }

    fn take(&mut self, ticket: WriteTicket) -> Result<PendingWrite, SessionError> {
        if !Arc::ptr_eq(&self.identity, &ticket.registry) {
            return Err(SessionError::Stale);
        }
        let pending = self
            .pending
            .remove(&ticket.sequence)
            .ok_or(SessionError::Stale)?;
        self.charged -= WRITE_CHARGE + RANGE_CHARGE;
        Ok(pending)
    }

    // Call only after the worker stopped, including cancellation/panic paths.
    // Returning admission while the write still runs would permit an overlap.
    pub(crate) fn failed(&mut self, ticket: WriteTicket) -> Result<(), SessionError> {
        self.take(ticket).map(|_| ())
    }

    pub(crate) fn placed(
        &mut self,
        sessions: &ArchiveSessions,
        ticket: WriteTicket,
    ) -> Result<PhysicalFileId, SessionError> {
        let pending = self.take(ticket)?;
        if !sessions.accepts(pending.destination.namespace)
            || self.destinations.get(&pending.destination.key()) != Some(&pending.file)
        {
            return Err(SessionError::Stale);
        }
        let file = self
            .files
            .get_mut(&pending.file)
            .expect("registered physical file");
        file.placed.insert(pending.offset, pending.len);
        let after = file.placed.ranges().len() as u64;
        // ByteRanges retains allocation after coalescing. Charge its high-water
        // mark, not just its current range count.
        if after > file.range_slots {
            self.charged += (after - file.range_slots) * RANGE_CHARGE;
            file.range_slots = after;
        }
        Ok(pending.file)
    }

    // The worker has closed a successfully created output of this exact
    // length. Placement proves its bytes; this explicit transition also proves
    // empty-file creation. Integrity and durable sync are separate gates.
    pub(crate) fn seal(
        &mut self,
        sessions: &ArchiveSessions,
        destination: Destination,
        length: u64,
    ) -> Result<(), SessionError> {
        if !sessions.accepts(destination.namespace) {
            return Err(SessionError::Stale);
        }
        let id = *self
            .destinations
            .get(&destination.key())
            .ok_or(SessionError::Stale)?;
        if self.pending.values().any(|write| write.file == id) {
            return Err(SessionError::Busy);
        }
        let file = self.files.get_mut(&id).expect("registered physical file");
        if file.length.is_some_and(|expected| expected != length)
            || file
                .placed
                .ranges()
                .last()
                .is_some_and(|&(_, end)| end > length)
            || if length == 0 {
                !file.placed.is_empty()
            } else {
                file.placed.ranges() != [(0, length)]
            }
        {
            return Err(SessionError::InvalidTransition);
        }
        file.length = Some(length);
        file.sealed = true;
        Ok(())
    }

    // Fence and join dispatched writers before capturing. Sync every claimed
    // physical file before preparing the immutable job manifest from this view.
    #[cfg(test)]
    pub(crate) fn capture_bytes(&self) -> u64 {
        self.charged
    }

    #[cfg(test)]
    pub(crate) fn capture(
        &self,
        admission: MemoryPermit,
    ) -> Result<PhysicalSnapshot, SessionError> {
        if !self.pending.is_empty() {
            return Err(SessionError::Busy);
        }
        if admission.bytes() < self.capture_bytes() {
            return Err(SessionError::Admission);
        }
        Ok(PhysicalSnapshot {
            files: self
                .files
                .iter()
                .map(|(&id, file)| PhysicalClaim {
                    id,
                    relative: file.relative.clone(),
                    length: file.length,
                    sealed: file.sealed,
                    placed: file.placed.clone(),
                    readers: Arc::clone(&file.readers),
                })
                .collect(),
            _admission: admission,
        })
    }
}

#[cfg(test)]
mod tests;

pub(crate) mod checkpoint;
