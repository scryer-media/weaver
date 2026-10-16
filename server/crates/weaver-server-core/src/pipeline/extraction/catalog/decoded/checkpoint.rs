use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    members: Vec<SavedMember>,
    occurrences: Vec<(usize, u32)>,
    revision: u64,
    closed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SavedMember {
    name: String,
    length: Option<u64>,
    kind: u8,
    delivered: u64,
    complete: bool,
    integrity: Option<u8>,
}

impl DecodedCatalog {
    pub(crate) fn capture_checkpoint(&self) -> Checkpoint {
        Checkpoint {
            members: self
                .members
                .iter()
                .map(|member| SavedMember {
                    name: member.name.clone(),
                    length: member.length,
                    kind: match member.storage {
                        Storage::Stream { .. } => 0,
                        Storage::Physical => 1,
                        Storage::Directory => 2,
                    },
                    delivered: match member.storage {
                        Storage::Stream { delivered } => delivered,
                        _ => 0,
                    },
                    complete: member.complete,
                    integrity: member.integrity.map(|proof| match proof {
                        Integrity::Verified => 0,
                        Integrity::NoChecksum => 1,
                    }),
                })
                .collect(),
            occurrences: self
                .occurrences
                .iter()
                .map(|(occurrence, id)| (*occurrence, *id))
                .collect(),
            revision: self.revision,
            closed: self.closed,
        }
    }

    pub(crate) fn restore_checkpoint(
        saved: &Checkpoint,
        generation: Generation,
        memory: MemoryPermit,
    ) -> io::Result<Self> {
        if saved.members.len() > MAX_CATALOG_MEMBERS
            || saved.occurrences.len() != saved.members.len()
        {
            return Err(invalid());
        }
        let mut catalog = Self::new(generation, memory);
        let mut names = std::collections::BTreeSet::new();
        for member in &saved.members {
            crate::pipeline::extraction::ExtractionRoot::parse_relative_path(&member.name)
                .map_err(|_| invalid())?;
            if !names.insert(&member.name)
                || member.kind > 2
                || member.integrity.is_some_and(|proof| proof > 1)
                || member.integrity.is_some() && !member.complete
                || member.length.is_some_and(|len| member.delivered > len)
            {
                return Err(invalid());
            }
            let storage = match member.kind {
                0 => Storage::Stream {
                    delivered: member.delivered,
                },
                1 => Storage::Physical,
                _ => Storage::Directory,
            };
            if member.complete
                && matches!(storage,Storage::Stream{delivered} if member.length!=Some(delivered))
            {
                return Err(invalid());
            }
            catalog.charged = catalog
                .charged
                .checked_add(ENTRY_BYTES + member.name.len() as u64)
                .filter(|n| *n <= catalog.memory.bytes())
                .ok_or_else(invalid)?;
            catalog.members.push(Member {
                name: member.name.clone(),
                length: member.length,
                storage,
                complete: member.complete,
                integrity: member.integrity.map(|proof| {
                    if proof == 0 {
                        Integrity::Verified
                    } else {
                        Integrity::NoChecksum
                    }
                }),
            });
        }
        let mut ids = std::collections::BTreeSet::new();
        for &(occurrence, id) in &saved.occurrences {
            if id as usize >= saved.members.len()
                || !ids.insert(id)
                || catalog.occurrences.insert(occurrence, id).is_some()
            {
                return Err(invalid());
            }
        }
        catalog.revision = saved.revision;
        if saved.closed {
            catalog.close()?;
        }
        Ok(catalog)
    }
}

impl Checkpoint {
    pub(crate) fn directories(&self) -> impl Iterator<Item = (u32, &str)> {
        self.members
            .iter()
            .enumerate()
            .filter(|(_, member)| self.closed && member.kind == 2)
            .map(|(id, member)| (id as u32, member.name.as_str()))
    }

    pub(crate) fn validate(&self) -> io::Result<()> {
        if self.members.len() > MAX_CATALOG_MEMBERS || self.occurrences.len() != self.members.len()
        {
            return Err(invalid());
        }
        let mut names = std::collections::BTreeSet::new();
        let mut ids = std::collections::BTreeSet::new();
        let mut occurrences = std::collections::BTreeSet::new();
        for member in &self.members {
            crate::pipeline::extraction::ExtractionRoot::parse_relative_path(&member.name)
                .map_err(|_| invalid())?;
            if !names.insert(&member.name)
                || member.kind > 2
                || member.integrity.is_some_and(|proof| proof > 1)
                || member.integrity.is_some() && !member.complete
                || member.length.is_some_and(|len| member.delivered > len)
                || member.complete && member.kind == 0 && member.length != Some(member.delivered)
                || self.closed
                    && member.kind != 2
                    && (!member.complete || member.integrity.is_none())
            {
                return Err(invalid());
            }
        }
        for &(occurrence, id) in &self.occurrences {
            if id as usize >= self.members.len()
                || !ids.insert(id)
                || !occurrences.insert(occurrence)
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}
