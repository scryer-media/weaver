//! Immutable placed-byte views shared by archive engines and their owner.
use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use super::ProcessMemoryBudget;
use super::safety::ProcessMemoryPermit;
use crate::pipeline::direct_store::provider::{VirtualVolume, VirtualVolumeReader};

#[derive(Clone)]
pub(crate) struct LivePart {
    pub(crate) view: VirtualVolume,
    pub(crate) length: Option<u64>,
    pub(crate) complete: bool,
}

struct Part {
    value: LivePart,
    ranges: Vec<(u64, u64)>,
}

struct Snapshot {
    parts: BTreeMap<u32, Part>,
    roster_closed: bool,
    _memory: ProcessMemoryPermit,
}

#[derive(Default)]
struct State {
    snapshot: Option<Arc<Snapshot>>,
    aborted: Option<String>,
    #[cfg(test)]
    waiters: usize,
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    #[cfg(test)]
    waiting_changed: Condvar,
}

#[derive(Clone, Default)]
pub(crate) struct LiveArchiveInput {
    shared: Arc<Shared>,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn covered(ranges: &[(u64, u64)], start: u64, end: u64) -> bool {
    let mut cursor = start;
    for &(a, b) in ranges {
        if b <= cursor {
            continue;
        }
        if a > cursor {
            return false;
        }
        cursor = b;
        if cursor >= end {
            return true;
        }
    }
    start == end
}

impl LiveArchiveInput {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn publish(
        &self,
        parts: BTreeMap<u32, LivePart>,
        roster_closed: bool,
        memory: &Arc<ProcessMemoryBudget>,
    ) -> Result<(), String> {
        if parts.len() > 65_536 {
            return Err("archive input volume limit exceeded".into());
        }
        let mut prepared = BTreeMap::new();
        let mut retained = 0u64;
        for (index, mut part) in parts {
            if part.view.volume_index != index
                || (part.complete && part.length.is_none())
                || part.length.is_some_and(|length| part.view.len > length)
            {
                return Err("invalid live archive input bounds".into());
            }
            // Holds are speculative parser input, not placed source bytes.
            part.view.retain_placed_only();
            let ranges = part.view.readable_ranges();
            if ranges.last().is_some_and(|&(_, end)| end > part.view.len) {
                return Err("archive coverage exceeds its image".into());
            }
            retained = retained
                .checked_add(part.view.retained_bytes() as u64)
                .and_then(|value| value.checked_add(part.view.retained_metadata_bytes() as u64))
                .and_then(|value| {
                    value.checked_add(
                        (ranges.capacity() as u64)
                            .saturating_mul(16)
                            .saturating_add(256),
                    )
                })
                .ok_or("archive input memory accounting overflow")?;
            prepared.insert(
                index,
                Part {
                    value: part,
                    ranges,
                },
            );
        }
        let permit = memory.try_reserve_retained(retained)?;
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| "archive input lock poisoned")?;
        if let Some(error) = &state.aborted {
            return Err(error.clone());
        }
        if let Some(previous) = &state.snapshot {
            if previous.roster_closed
                && (!roster_closed || previous.parts.keys().ne(prepared.keys()))
            {
                return Err("closed archive input roster changed".into());
            }
            for (index, old) in &previous.parts {
                let next = prepared
                    .get(index)
                    .ok_or("archive input volume disappeared")?;
                if old
                    .value
                    .length
                    .is_some_and(|length| next.value.length != Some(length))
                    || old.value.view.len > next.value.view.len
                    || (old.value.complete && !next.value.complete)
                    || old
                        .ranges
                        .iter()
                        .any(|&(start, end)| !covered(&next.ranges, start, end))
                {
                    return Err("archive input publication regressed".into());
                }
            }
        }
        state.snapshot = Some(Arc::new(Snapshot {
            parts: prepared,
            roster_closed,
            _memory: permit,
        }));
        // The immutable view is selected under the same lock as the wakeup.
        self.shared.changed.notify_all();
        Ok(())
    }

    pub(crate) fn abort(&self, message: impl Into<String>) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.aborted.get_or_insert_with(|| message.into());
        self.shared.changed.notify_all();
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, State>> {
        self.shared
            .state
            .lock()
            .map_err(|_| io::Error::other("archive input lock poisoned"))
    }

    fn check(state: &State) -> io::Result<()> {
        if let Some(error) = &state.aborted {
            return Err(io::Error::other(error.clone()));
        }
        Ok(())
    }

    fn wait<'a>(&self, state: MutexGuard<'a, State>) -> io::Result<MutexGuard<'a, State>> {
        #[cfg(test)]
        let state = {
            let mut state = state;
            state.waiters += 1;
            self.shared.waiting_changed.notify_all();
            state
        };
        let state = self
            .shared
            .changed
            .wait(state)
            .map_err(|_| io::Error::other("archive input lock poisoned"))?;
        #[cfg(test)]
        let state = {
            let mut state = state;
            state.waiters -= 1;
            self.shared.waiting_changed.notify_all();
            state
        };
        Ok(state)
    }

    pub(crate) fn wait_volume(&self, index: usize) -> io::Result<bool> {
        let index = u32::try_from(index).map_err(|_| invalid("archive volume index overflow"))?;
        let mut state = self.lock()?;
        loop {
            Self::check(&state)?;
            if let Some(snapshot) = &state.snapshot {
                if snapshot.parts.contains_key(&index) {
                    return Ok(true);
                }
                if snapshot.roster_closed {
                    return if snapshot
                        .parts
                        .last_key_value()
                        .is_none_or(|(&last, _)| index > last)
                    {
                        Ok(false)
                    } else {
                        Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "closed archive roster contains a missing volume",
                        ))
                    };
                }
            }
            state = self.wait(state)?;
        }
    }

    fn length(&self, index: u32) -> io::Result<u64> {
        let mut state = self.lock()?;
        loop {
            Self::check(&state)?;
            if let Some(snapshot) = &state.snapshot {
                if let Some(length) = snapshot
                    .parts
                    .get(&index)
                    .and_then(|part| part.value.length)
                {
                    return Ok(length);
                }
                if snapshot.roster_closed && !snapshot.parts.contains_key(&index) {
                    return Err(invalid("archive volume is absent"));
                }
            }
            state = self.wait(state)?;
        }
    }

    fn total(&self) -> io::Result<u64> {
        let mut state = self.lock()?;
        loop {
            Self::check(&state)?;
            if let Some(snapshot) = &state.snapshot
                && snapshot.roster_closed
            {
                let mut total = 0u64;
                let mut known = true;
                for (position, (&index, part)) in snapshot.parts.iter().enumerate() {
                    if index as usize != position {
                        return Err(invalid("closed archive roster contains a missing volume"));
                    }
                    let Some(length) = part.value.length else {
                        known = false;
                        break;
                    };
                    total = total
                        .checked_add(length)
                        .ok_or_else(|| invalid("archive input length overflow"))?;
                }
                if known {
                    return Ok(total);
                }
            }
            state = self.wait(state)?;
        }
    }

    fn locate(&self, position: u64) -> io::Result<Option<(u32, u64)>> {
        let mut state = self.lock()?;
        loop {
            Self::check(&state)?;
            if let Some(snapshot) = &state.snapshot {
                let mut offset = position;
                let mut index = 0u32;
                loop {
                    let Some(part) = snapshot.parts.get(&index) else {
                        if snapshot.roster_closed {
                            return if snapshot
                                .parts
                                .last_key_value()
                                .is_none_or(|(&last, _)| index > last)
                            {
                                Ok(None)
                            } else {
                                Err(invalid("closed archive roster contains a missing volume"))
                            };
                        }
                        break;
                    };
                    match part.value.length {
                        Some(length) if offset >= length => {
                            offset -= length;
                            index = index
                                .checked_add(1)
                                .ok_or_else(|| invalid("archive volume index overflow"))?;
                        }
                        _ => return Ok(Some((index, offset))),
                    }
                }
            }
            state = self.wait(state)?;
        }
    }

    pub(crate) fn reader(&self) -> LiveArchiveReader {
        LiveArchiveReader {
            input: self.clone(),
            position: 0,
            current: None,
        }
    }
}

impl unrar_rs::VolumeProvider for LiveArchiveInput {
    fn get_volume(
        &self,
        index: usize,
    ) -> Result<Box<dyn unrar_rs::ReadSeek>, unrar_rs::VolumeProviderError> {
        let present = self.wait_volume(index).map_err(|error| {
            unrar_rs::VolumeProviderError::Unavailable {
                volume: index,
                reason: error.to_string(),
            }
        })?;
        if !present {
            return Err(unrar_rs::VolumeProviderError::Unavailable {
                volume: index,
                reason: "archive volume is past the closed roster".into(),
            });
        }
        Ok(Box::new(PartReader {
            input: self.clone(),
            index: index as u32,
            position: 0,
            source: None,
        }))
    }
}

struct PartReader {
    input: LiveArchiveInput,
    index: u32,
    position: u64,
    source: Option<(Arc<Snapshot>, VirtualVolumeReader<true>)>,
}

impl Read for PartReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let (snapshot, available) = {
            let mut state = self.input.lock()?;
            loop {
                LiveArchiveInput::check(&state)?;
                if let Some(snapshot) = &state.snapshot
                    && let Some(part) = snapshot.parts.get(&self.index)
                {
                    if part
                        .value
                        .length
                        .is_some_and(|length| self.position >= length)
                    {
                        return Ok(0);
                    }
                    if let Some(&(_, end)) = part
                        .ranges
                        .iter()
                        .find(|&&(start, end)| start <= self.position && self.position < end)
                    {
                        break (Arc::clone(snapshot), end - self.position);
                    }
                    if part.value.complete {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "completed archive input contains a hole",
                        ));
                    }
                }
                state = self.input.wait(state)?;
            }
        };
        if self
            .source
            .as_ref()
            .is_none_or(|(old, _)| !Arc::ptr_eq(old, &snapshot))
        {
            let volume = snapshot.parts[&self.index].value.view.clone();
            self.source = Some((
                snapshot,
                VirtualVolumeReader::<true>::new(volume, Arc::default()),
            ));
        }
        let reader = &mut self
            .source
            .as_mut()
            .expect("selected immutable archive input")
            .1;
        reader.seek(SeekFrom::Start(self.position))?;
        let wanted = out
            .len()
            .min(usize::try_from(available).unwrap_or(usize::MAX));
        let read = reader.read(&mut out[..wanted])?;
        LiveArchiveInput::check(&*self.input.lock()?)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "archive backing ended inside placed coverage",
            ));
        }
        self.position = self
            .position
            .checked_add(read as u64)
            .ok_or_else(|| invalid("archive position overflow"))?;
        Ok(read)
    }
}

impl Seek for PartReader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        LiveArchiveInput::check(&*self.input.lock()?)?;
        self.position = match from {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(delta) => self.position.checked_add_signed(delta),
            SeekFrom::End(delta) => self.input.length(self.index)?.checked_add_signed(delta),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "archive seek overflow"))?;
        Ok(self.position)
    }
}

pub(crate) struct LiveArchiveReader {
    input: LiveArchiveInput,
    position: u64,
    current: Option<PartReader>,
}

impl Read for LiveArchiveReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            let Some((index, offset)) = self.input.locate(self.position)? else {
                return Ok(0);
            };
            if self
                .current
                .as_ref()
                .is_none_or(|reader| reader.index != index)
            {
                self.current = Some(PartReader {
                    input: self.input.clone(),
                    index,
                    position: offset,
                    source: None,
                });
            }
            let reader = self.current.as_mut().expect("located archive part");
            reader.position = offset;
            let count = reader.read(out)?;
            if count == 0 {
                continue;
            }
            self.position = self
                .position
                .checked_add(count as u64)
                .ok_or_else(|| invalid("archive position overflow"))?;
            return Ok(count);
        }
    }
}

impl Seek for LiveArchiveReader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        LiveArchiveInput::check(&*self.input.lock()?)?;
        self.position = match from {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(delta) => self.position.checked_add_signed(delta),
            SeekFrom::End(delta) => self.input.total()?.checked_add_signed(delta),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "archive seek overflow"))?;
        Ok(self.position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::direct_store::ByteRanges;

    fn part(
        path: &std::path::Path,
        index: u32,
        length: Option<u64>,
        complete: bool,
        ranges: &[(u64, u64)],
    ) -> LivePart {
        let mut covered = ByteRanges::new();
        for &(start, end) in ranges {
            covered.insert(start, end - start);
        }
        LivePart {
            length,
            complete,
            view: VirtualVolume {
                backing_leases: None,
                volume_index: index,
                envelope: path.to_owned(),
                extents: Vec::new(),
                partials: Arc::default(),
                nested: Default::default(),
                covered: covered.clone(),
                envelope_covered: covered,
                held: Arc::default(),
                len: length.unwrap_or_else(|| ranges.last().map_or(0, |&(_, end)| end)),
                ciphers: Arc::default(),
            },
        }
    }

    fn blocked(input: &LiveArchiveInput) {
        let mut state = input.shared.state.lock().unwrap();
        while state.waiters == 0 {
            state = input.shared.waiting_changed.wait(state).unwrap();
        }
    }

    #[test]
    fn blocked_reader_observes_publication_then_seeks_backwards() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source");
        std::fs::write(&path, b"abcdefgh").unwrap();
        let input = LiveArchiveInput::new();
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        input
            .publish(
                BTreeMap::from([(0, part(&path, 0, None, false, &[(0, 3)]))]),
                false,
                &memory,
            )
            .unwrap();
        let mut reader = input.reader();
        let worker = std::thread::spawn(move || {
            let mut bytes = [0; 8];
            reader.read_exact(&mut bytes).unwrap();
            (reader, bytes)
        });
        blocked(&input);
        input
            .publish(
                BTreeMap::from([(0, part(&path, 0, Some(8), true, &[(0, 8)]))]),
                true,
                &memory,
            )
            .unwrap();
        let (mut reader, bytes) = worker.join().unwrap();
        assert_eq!(bytes, *b"abcdefgh");
        reader.seek(SeekFrom::Start(1)).unwrap();
        let mut bytes = [0; 3];
        reader.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes, *b"bcd");
        assert_eq!(reader.seek(SeekFrom::End(-2)).unwrap(), 6);
    }

    #[test]
    fn missing_next_volume_waits_for_explicit_roster_and_abort_wakes_waiters() {
        let input = LiveArchiveInput::new();
        let waiting = input.clone();
        let worker = std::thread::spawn(move || waiting.wait_volume(1));
        blocked(&input);
        input.abort("input generation retired");
        assert!(worker.join().unwrap().is_err());
        let closed = LiveArchiveInput::new();
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        closed.publish(BTreeMap::new(), true, &memory).unwrap();
        assert!(!closed.wait_volume(0).unwrap());
    }

    #[test]
    fn concatenated_reader_waits_for_unknown_boundaries_and_new_volumes() {
        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("first");
        let second = directory.path().join("second");
        std::fs::write(&first, b"abcd").unwrap();
        std::fs::write(&second, b"efgh").unwrap();
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        let input = LiveArchiveInput::new();
        input
            .publish(
                BTreeMap::from([(0, part(&first, 0, None, false, &[(0, 4)]))]),
                false,
                &memory,
            )
            .unwrap();
        let mut reader = input.reader();
        let worker = std::thread::spawn(move || {
            assert_eq!(reader.seek(SeekFrom::End(-3)).unwrap(), 5);
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).unwrap();
            bytes
        });
        blocked(&input);
        input
            .publish(
                BTreeMap::from([
                    (0, part(&first, 0, Some(4), true, &[(0, 4)])),
                    (1, part(&second, 1, Some(4), true, &[(0, 4)])),
                ]),
                true,
                &memory,
            )
            .unwrap();
        assert_eq!(worker.join().unwrap(), b"fgh");
    }

    #[test]
    fn completed_holes_and_regressing_publications_are_refused() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source");
        std::fs::write(&path, b"abcdefgh").unwrap();
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        let input = LiveArchiveInput::new();
        input
            .publish(
                BTreeMap::from([(0, part(&path, 0, Some(8), true, &[(0, 3), (5, 8)]))]),
                true,
                &memory,
            )
            .unwrap();
        let mut reader = input.reader();
        reader.seek(SeekFrom::Start(3)).unwrap();
        assert_eq!(
            reader.read(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert!(
            input
                .publish(
                    BTreeMap::from([(0, part(&path, 0, Some(8), true, &[(0, 2)]))]),
                    true,
                    &memory
                )
                .is_err()
        );
        assert!(input.publish(BTreeMap::new(), true, &memory).is_err());
    }
}
