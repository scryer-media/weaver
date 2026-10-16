use super::*;
use crate::pipeline::extraction::output::Integrity;
use crate::pipeline::extraction::output::channel::{Event, Frame, Generation};
use crate::pipeline::extraction::safety::MemoryPermit;
use std::collections::BTreeMap;
use std::io;

// Covers the member, occurrence index and allocator overhead, independently of
// frame admission. Draining a frame must not release retained catalog memory.
const ENTRY_BYTES: u64 = 512;

enum Storage {
    Stream { delivered: u64 },
    Physical,
    Directory,
}

struct Member {
    name: String,
    length: Option<u64>,
    storage: Storage,
    complete: bool,
    integrity: Option<Integrity>,
}

pub(crate) struct DecodedCatalog {
    generation: Generation,
    members: Vec<Member>,
    occurrences: BTreeMap<usize, u32>,
    revision: u64,
    closed: bool,
    charged: u64,
    memory: MemoryPermit,
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid archive output lifecycle",
    )
}

impl DecodedCatalog {
    pub(crate) fn generation(&self) -> u64 {
        self.generation.owner().generation
    }

    pub(crate) fn live_epoch(
        &self,
    ) -> Result<crate::pipeline::extraction::output::channel::StreamOwner, ProjectionError> {
        if self.generation.is_revoked() {
            return Err(ProjectionError::InvalidRange);
        }
        Ok(self.generation.owner())
    }

    pub(crate) fn new(generation: Generation, memory: MemoryPermit) -> Self {
        Self {
            generation,
            members: Vec::new(),
            occurrences: BTreeMap::new(),
            revision: 0,
            closed: false,
            charged: 0,
            memory,
        }
    }

    // No payload is retained or copied here. The caller transfers Bytes into
    // routing admission separately and owns placement and generation fencing.
    pub(crate) fn accept(&mut self, frame: &Frame) -> io::Result<u32> {
        if self.closed
            || frame.member.owner != self.generation.owner()
            || self.generation.is_revoked()
            || frame.lease.is_revoked()
        {
            return Err(invalid());
        }
        let declaration = match &frame.event {
            Event::File { path, length } => Some((path, *length, Storage::Stream { delivered: 0 })),
            Event::PhysicalFile { path, length } => Some((path, *length, Storage::Physical)),
            Event::Directory { path } => Some((path, Some(0), Storage::Directory)),
            _ => None,
        };
        if let Some((path, length, storage)) = declaration {
            if self.occurrences.contains_key(&frame.member.occurrence)
                || self.members.len() >= MAX_CATALOG_MEMBERS
            {
                return Err(invalid());
            }
            let name = path.to_str().ok_or_else(invalid)?;
            crate::pipeline::extraction::ExtractionRoot::parse_relative_path(name)
                .map_err(|_| invalid())?;
            let charged = self
                .charged
                .checked_add(ENTRY_BYTES)
                .and_then(|bytes| bytes.checked_add(name.len() as u64))
                .filter(|bytes| *bytes <= self.memory.bytes())
                .ok_or_else(|| io::Error::other("archive catalog admission exhausted"))?;
            let id = self.members.len() as u32;
            self.members.push(Member {
                name: name.to_owned(),
                length,
                storage,
                complete: false,
                integrity: None,
            });
            self.occurrences.insert(frame.member.occurrence, id);
            self.charged = charged;
            self.revision += 1;
            return Ok(id);
        }
        let id = *self
            .occurrences
            .get(&frame.member.occurrence)
            .ok_or_else(invalid)?;
        let member = &mut self.members[id as usize];
        match &frame.event {
            Event::Bytes { offset, bytes } => {
                let Storage::Stream { delivered } = &mut member.storage else {
                    return Err(invalid());
                };
                let end = offset.checked_add(bytes.len() as u64).ok_or_else(invalid)?;
                if member.complete
                    || *offset != *delivered
                    || member.length.is_some_and(|len| end > len)
                {
                    return Err(invalid());
                }
                *delivered = end;
            }
            Event::DataComplete { length } => {
                if member.complete
                    || matches!(member.storage, Storage::Directory)
                    || member.length.is_some_and(|expected| expected != *length)
                    || matches!(member.storage, Storage::Stream { delivered } if delivered != *length)
                {
                    return Err(invalid());
                }
                member.complete = true;
                if member.length.is_none() {
                    member.length = Some(*length);
                    self.revision += 1;
                }
            }
            Event::Integrity(integrity) => {
                if !member.complete || member.integrity.is_some() {
                    return Err(invalid());
                }
                member.integrity = Some(*integrity);
            }
            Event::Metadata(_) => {}
            _ => return Err(invalid()),
        }
        Ok(id)
    }

    // Worker success is separate from the last member proof: trailing archive
    // checks can still fail and revoke an otherwise complete member catalog.
    pub(crate) fn close(&mut self) -> io::Result<()> {
        if self.generation.is_revoked()
            || self.members.iter().any(|member| {
                !matches!(member.storage, Storage::Directory)
                    && (!member.complete || member.integrity.is_none())
            })
        {
            return Err(invalid());
        }
        self.closed = true;
        Ok(())
    }
}

impl MemberCatalog for DecodedCatalog {
    fn revision(&self) -> u64 {
        self.revision
    }

    fn members(&self) -> Vec<MemberFact<'_>> {
        self.members
            .iter()
            .enumerate()
            .map(|(id, member)| MemberFact {
                id: id as u32,
                name: &member.name,
                routable: !matches!(member.storage, Storage::Directory),
            })
            .collect()
    }

    fn length(&self, member: u32) -> Option<u64> {
        self.members
            .get(member as usize)
            .and_then(|member| member.length)
    }
}

impl MemberProjection for DecodedCatalog {
    fn project_member(
        &self,
        member: u32,
        range: MemberRange,
    ) -> Result<InputProjection, ProjectionError> {
        if self.generation.is_revoked() {
            return Err(ProjectionError::InvalidRange);
        }
        let member = self
            .members
            .get(member as usize)
            .ok_or(ProjectionError::Unmapped)?;
        let end = range
            .offset
            .checked_add(range.len)
            .ok_or(ProjectionError::InvalidRange)?;
        range
            .write_offset
            .checked_add(range.len)
            .ok_or(ProjectionError::InvalidRange)?;
        let available = match member.storage {
            Storage::Stream { delivered } => delivered,
            Storage::Physical if member.complete => {
                member.length.ok_or(ProjectionError::Unmapped)?
            }
            _ => return Err(ProjectionError::Unmapped),
        };
        if end > available {
            return Err(ProjectionError::Unmapped);
        }
        Ok(InputProjection::NotReversible)
    }
}

pub(crate) mod checkpoint;
