use super::*;
use serde::{Deserialize, Serialize};

pub(crate) type CheckpointClaim = (Destination, PathBuf, u64, Vec<(u64, u64)>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    files: Vec<File>,
    destinations: Vec<(Key, u32)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct File {
    relative: String,
    length: Option<u64>,
    sealed: bool,
    placed: Vec<(u64, u64)>,
}

impl DestinationRegistry {
    pub(crate) fn capture_checkpoint(&self) -> Result<Checkpoint, SessionError> {
        if !self.pending.is_empty() {
            return Err(SessionError::Busy);
        }
        Ok(Checkpoint {
            files: self
                .files
                .values()
                .map(|file| File {
                    relative: file
                        .relative
                        .to_str()
                        .expect("validated registry path")
                        .to_owned(),
                    length: file.length,
                    sealed: file.sealed,
                    placed: file.placed.ranges().to_vec(),
                })
                .collect(),
            destinations: self
                .destinations
                .iter()
                .map(|(key, id)| (*key, id.0))
                .collect(),
        })
    }

    pub(crate) fn restore_checkpoint(
        saved: &Checkpoint,
        admission: MemoryPermit,
        old_root: u64,
        root: u64,
    ) -> Result<Self, SessionError> {
        if saved.files.len() > 65_536 || saved.destinations.len() > 65_536 {
            return Err(SessionError::Admission);
        }
        let mut registry = Self::new(admission)?;
        for (index, file) in saved.files.iter().enumerate() {
            if !private_path(&file.relative) || file.placed.len() > 65_536 {
                return Err(SessionError::InvalidTransition);
            }
            let location = file.relative.to_lowercase();
            if registry.locations.contains_key(&location)
                || registry.locations.keys().any(|other| {
                    location.starts_with(&format!("{other}/"))
                        || other.starts_with(&format!("{location}/"))
                })
            {
                return Err(SessionError::InvalidTransition);
            }
            let mut placed = ByteRanges::default();
            let mut previous = 0;
            for &(start, end) in &file.placed {
                if start >= end || start < previous || file.length.is_some_and(|len| end > len) {
                    return Err(SessionError::InvalidTransition);
                }
                placed.insert(start, end - start);
                previous = end;
            }
            if file.sealed
                && file.length.is_none_or(|len| {
                    if len == 0 {
                        !placed.is_empty()
                    } else {
                        placed.ranges() != [(0, len)]
                    }
                })
            {
                return Err(SessionError::InvalidTransition);
            }
            registry.charge(
                FILE_CHARGE
                    + file.relative.len() as u64 * 5
                    + file.placed.len() as u64 * RANGE_CHARGE,
            )?;
            let id = PhysicalFileId(index as u32);
            registry.locations.insert(location, id);
            registry.files.insert(
                id,
                PhysicalFile {
                    relative: file.relative.clone().into(),
                    length: file.length,
                    sealed: file.sealed,
                    placed,
                    range_slots: file.placed.len() as u64,
                    #[cfg(test)]
                    readers: Arc::new(()),
                },
            );
        }
        for &(mut key, id) in &saved.destinations {
            if key.0 != old_root
                || key.2 == 0
                || key.4 == 0
                || key.5 > 1
                || id as usize >= saved.files.len()
            {
                return Err(SessionError::InvalidTransition);
            }
            key.0 = root;
            if registry
                .destinations
                .insert(key, PhysicalFileId(id))
                .is_some()
            {
                return Err(SessionError::InvalidTransition);
            }
        }
        Ok(registry)
    }
}

impl DestinationRegistry {
    pub(crate) fn checkpoint_claims(&self) -> Result<Vec<CheckpointClaim>, SessionError> {
        if !self.pending.is_empty() {
            return Err(SessionError::Busy);
        }
        self.files
            .iter()
            .map(|(id, file)| {
                let key = self
                    .destinations
                    .iter()
                    .find_map(|(key, bound)| (*bound == *id).then_some(*key))
                    .ok_or(SessionError::Busy)?;
                let destination = Destination {
                    namespace: StreamOwner {
                        root: key.0,
                        node: key.1,
                        generation: key.2,
                    },
                    owner: SpanOwner {
                        node: key.3,
                        generation: key.4,
                        raw_child: None,
                    },
                    destination: if key.5 == 0 {
                        DirectDestination::Member { member_id: key.6 }
                    } else {
                        DirectDestination::Envelope {
                            volume_index: key.6,
                        }
                    },
                };
                Ok((
                    destination,
                    file.relative.clone(),
                    file.length
                        .unwrap_or_else(|| file.placed.ranges().last().map_or(0, |&(_, end)| end)),
                    file.placed.ranges().to_vec(),
                ))
            })
            .collect()
    }
}

impl Checkpoint {
    pub(crate) fn validate(&self) -> std::io::Result<()> {
        let invalid = || {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid physical registry checkpoint",
            )
        };
        if self.files.len() > 65_536 || self.destinations.len() > 65_536 {
            return Err(invalid());
        }
        let mut locations = std::collections::BTreeSet::new();
        let mut keys = std::collections::BTreeSet::new();
        for file in &self.files {
            if !private_path(&file.relative) || file.placed.len() > 65_536 {
                return Err(invalid());
            }
            let location = file.relative.to_lowercase();
            if locations.iter().any(|other: &String| {
                location == *other
                    || location.starts_with(&format!("{other}/"))
                    || other.starts_with(&format!("{location}/"))
            }) {
                return Err(invalid());
            }
            locations.insert(location);
            let mut previous = 0;
            for &(start, end) in &file.placed {
                if start >= end || start < previous || file.length.is_some_and(|len| end > len) {
                    return Err(invalid());
                }
                previous = end;
            }
            if file.sealed
                && file.length.is_none_or(|len| {
                    if len == 0 {
                        !file.placed.is_empty()
                    } else {
                        file.placed != [(0, len)]
                    }
                })
            {
                return Err(invalid());
            }
        }
        for &(key, id) in &self.destinations {
            if key.2 == 0
                || key.4 == 0
                || key.5 > 1
                || id as usize >= self.files.len()
                || !keys.insert(key)
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}
