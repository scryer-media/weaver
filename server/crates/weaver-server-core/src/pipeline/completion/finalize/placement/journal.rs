use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::PathBuf;

const PREFIX: &str = ".weaver-placement-";
const JOURNAL: &str = "journal.json";
const NEXT_JOURNAL: &str = ".journal-next";

#[cfg(test)]
thread_local! { static PAYLOAD_HASH_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub file_index: u32,
    pub filename: String,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Location {
    Original,
    Staged,
    Installed,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Phase {
    Staging,
    Installation,
    IdentityBinding,
    RollbackVacate,
    RollbackRestore,
    Complete,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    source: String,
    staging: String,
    destination: String,
    size: u64,
    reference: String,
    location: Location,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingMove {
    index: usize,
    to: Location,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u32,
    phase: Phase,
    entries: Vec<Entry>,
    bindings: Vec<Binding>,
    pending: Option<PendingMove>,
}

pub(crate) struct Transaction {
    root: PathBuf,
    staging: PathBuf,
    journal: Journal,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn valid_name(name: &str) -> bool {
    let mut parts = Path::new(name).components();
    matches!(parts.next(), Some(Component::Normal(_)))
        && parts.next().is_none()
        && !name.contains(['/', '\\', ':'])
        && !name.starts_with(PREFIX)
}

// Only copied paths need a content comparison during recovery. Ordinary
// placement keeps a hard-link witness and never rereads the payload.
fn fingerprint(path: &Path) -> io::Result<(u64, [u8; 32])> {
    #[cfg(test)]
    PAYLOAD_HASH_READS.with(|reads| reads.set(reads.get() + 1));
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(invalid("placement payload is not a regular file"));
    }
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 256 * 1024];
    let mut size = 0;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        size += count as u64;
    }
    Ok((size, hash.finalize().into()))
}

#[cfg(unix)]
fn same_file(left: &Path, right: &Path) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let left = fs::metadata(left)?;
    let right = fs::metadata(right)?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(windows)]
fn same_file(left: &Path, right: &Path) -> io::Result<bool> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };
    fn identity(path: &Path) -> io::Result<(u32, u64)> {
        let file = File::open(path)?;
        // SAFETY: this Win32 output structure consists only of integer fields.
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        // SAFETY: the file keeps its handle alive and info is valid writable storage.
        if unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), &mut info) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((
            info.dwVolumeSerialNumber,
            (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
        ))
    }
    Ok(identity(left)? == identity(right)?)
}

#[cfg(not(any(unix, windows)))]
fn same_file(_left: &Path, _right: &Path) -> io::Result<bool> {
    Ok(false)
}

fn sync_payload(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    let file = fs::OpenOptions::new().read(true).write(true).open(path)?;
    #[cfg(not(windows))]
    let file = File::open(path)?;
    file.sync_all()
}

fn sync_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    // Windows does not provide FlushFileBuffers for directory handles. Each
    // payload and journal file is flushed before its successor phase is saved.
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

pub(super) fn prepare(
    dir: &Path,
    plan: &par2_rs::PlacementPlan,
    bindings: Vec<Binding>,
) -> io::Result<Option<Transaction>> {
    if !recover(dir)?.is_empty() {
        return Err(invalid(
            "finish recovered placement identities before starting another placement",
        ));
    }
    let entries = validate_plan(dir, plan)?;
    if entries.is_empty() {
        return Ok(None);
    }
    let entries = entries
        .into_iter()
        .enumerate()
        .map(|(index, entry)| {
            let size = fs::symlink_metadata(dir.join(&entry.current_name))?.len();
            Ok(Entry {
                source: entry.current_name.clone(),
                staging: entry.current_name.clone(),
                destination: entry.correct_name.clone(),
                size,
                reference: format!("{PREFIX}{index}"),
                location: Location::Original,
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    let journal = Journal {
        version: 1,
        phase: Phase::Staging,
        entries,
        bindings,
        pending: None,
    };
    validate_journal(&journal)?;
    let staging = tempfile::Builder::new()
        .prefix(PREFIX)
        .tempdir_in(dir)?
        .keep();
    let transaction = Transaction {
        root: dir.to_path_buf(),
        staging,
        journal,
    };
    transaction.save()?;
    sync_dir(dir)?;
    Ok(Some(transaction))
}

pub(crate) fn begin(
    dir: &Path,
    plan: &par2_rs::PlacementPlan,
    bindings: Vec<Binding>,
) -> io::Result<Option<Transaction>> {
    let Some(mut transaction) = prepare(dir, plan, bindings)? else {
        return Ok(None);
    };
    transaction.run_with_move(rename_no_overwrite)?;
    Ok(Some(transaction))
}

fn validate_journal(journal: &Journal) -> io::Result<()> {
    if journal.version != 1 || journal.entries.is_empty() {
        return Err(invalid("unsupported placement journal"));
    }
    for (index, entry) in journal.entries.iter().enumerate() {
        if entry.reference != format!("{PREFIX}{index}") {
            return Err(invalid("invalid placement witness name"));
        }
        let valid_location = match journal.phase {
            Phase::Staging => entry.location != Location::Installed,
            Phase::Installation => entry.location != Location::Original,
            Phase::IdentityBinding => entry.location == Location::Installed,
            Phase::RollbackVacate => true,
            Phase::RollbackRestore => entry.location != Location::Installed,
            Phase::Complete => {
                journal
                    .entries
                    .iter()
                    .all(|other| other.location == entry.location)
                    && entry.location != Location::Staged
            }
        };
        if !valid_location {
            return Err(invalid("placement phase disagrees with payload locations"));
        }
        if ![&entry.source, &entry.staging, &entry.destination]
            .into_iter()
            .all(|name| valid_name(name))
            || [JOURNAL, NEXT_JOURNAL].contains(&entry.staging.as_str())
            || entry.source == entry.destination
        {
            return Err(invalid("invalid placement journal filename"));
        }
        for previous in &journal.entries[..index] {
            for (a, b) in [
                (&entry.source, &previous.source),
                (&entry.staging, &previous.staging),
                (&entry.destination, &previous.destination),
            ] {
                if paths_equivalent_for_placement(Path::new(a), Path::new(b)) {
                    return Err(invalid("duplicate placement journal path"));
                }
            }
        }
    }
    if let Some(pending) = journal.pending {
        let Some(entry) = journal.entries.get(pending.index) else {
            return Err(invalid("invalid pending placement index"));
        };
        if entry.location == pending.to {
            return Err(invalid("invalid pending placement move"));
        }
    }
    for (index, binding) in journal.bindings.iter().enumerate() {
        if !journal
            .entries
            .iter()
            .any(|entry| entry.destination == binding.filename)
            || journal.bindings[..index]
                .iter()
                .any(|previous| previous.file_index == binding.file_index)
        {
            return Err(invalid("invalid placement identity binding"));
        }
    }
    Ok(())
}

pub(crate) fn recover(dir: &Path) -> io::Result<Vec<Transaction>> {
    let listing = match fs::read_dir(dir) {
        Ok(listing) => listing,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut transactions = Vec::new();
    for item in listing {
        let item = item?;
        if !item.file_name().to_string_lossy().starts_with(PREFIX) {
            continue;
        }
        if !item.file_type()?.is_dir() {
            return Err(invalid(format!(
                "placement recovery requires a regular transaction directory: {}",
                item.path().display()
            )));
        }
        let staging = item.path();
        // A crash between removing the completed journal and its empty
        // directory leaves no payload or unfinished transaction to replay.
        if fs::read_dir(&staging)?.next().is_none() {
            fs::remove_dir(&staging)?;
            sync_dir(dir)?;
            continue;
        }
        if !fs::symlink_metadata(staging.join(JOURNAL))?.is_file() {
            return Err(invalid("placement journal is not a regular file"));
        }
        let bytes = fs::read(staging.join(JOURNAL)).map_err(|e| {
            invalid(format!(
                "cannot read placement journal at {}: {e}; retained payload requires recovery",
                staging.display()
            ))
        })?;
        let journal: Journal = serde_json::from_slice(&bytes).map_err(|e| {
            invalid(format!(
                "invalid placement journal at {}: {e}",
                staging.display()
            ))
        })?;
        validate_journal(&journal)?;
        let next = staging.join(NEXT_JOURNAL);
        if let Ok(metadata) = fs::symlink_metadata(&next) {
            if !metadata.is_file() {
                return Err(invalid("invalid interrupted placement journal write"));
            }
            fs::remove_file(next)?;
        }
        let mut transaction = Transaction {
            root: dir.to_path_buf(),
            staging,
            journal,
        };
        if transaction.journal.phase == Phase::Complete {
            transaction.cleanup()?;
            continue;
        }
        transaction
            .resume(&mut rename_no_overwrite)
            .map_err(|e| transaction.retained_error(e))?;
        for (index, entry) in transaction.journal.entries.iter().enumerate() {
            if !transaction.matches(&transaction.path(index, entry.location), index)? {
                return Err(transaction.retained_error(invalid("placement payload is missing")));
            }
        }
        if transaction.journal.phase == Phase::Complete {
            transaction.cleanup()?;
        } else {
            transactions.push(transaction);
        }
    }
    if transactions.len() > 1 {
        return Err(invalid(
            "multiple unfinished placement identity transactions; retained files require inspection",
        ));
    }
    Ok(transactions)
}

impl Transaction {
    pub(crate) fn bindings(&self) -> &[Binding] {
        &self.journal.bindings
    }
    pub(crate) fn len(&self) -> usize {
        self.journal.entries.len()
    }

    fn save(&self) -> io::Result<()> {
        let next = self.staging.join(NEXT_JOURNAL);
        if let Ok(metadata) = fs::symlink_metadata(&next)
            && !metadata.is_file()
        {
            return Err(invalid("invalid placement journal temporary file"));
        }
        let mut temporary = File::create(&next)?;
        serde_json::to_writer(&mut temporary, &self.journal)?;
        temporary.flush()?;
        temporary.sync_all()?;
        drop(temporary);
        fs::rename(next, self.staging.join(JOURNAL))?;
        sync_dir(&self.staging)
    }

    fn path(&self, index: usize, location: Location) -> PathBuf {
        let entry = &self.journal.entries[index];
        match location {
            Location::Original => self.root.join(&entry.source),
            Location::Staged => self.staging.join(&entry.staging),
            Location::Installed => self.root.join(&entry.destination),
        }
    }

    fn ensure_references(&self) -> io::Result<()> {
        for entry in &self.journal.entries {
            let reference = self.staging.join(&entry.reference);
            match fs::symlink_metadata(&reference) {
                Ok(metadata) if metadata.is_file() && metadata.len() == entry.size => continue,
                Ok(_) => return Err(invalid("placement witness is incomplete or conflicting")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            if entry.location != Location::Original {
                return Err(invalid("placement witness is missing after sources moved"));
            }
            let source = self.root.join(&entry.source);
            match fs::hard_link(&source, &reference) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => return Err(error),
                Err(_) => {
                    let mut destination = fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&reference)?;
                    let copied = io::copy(&mut File::open(&source)?, &mut destination)?;
                    if copied != entry.size {
                        return Err(invalid("placement witness copy is incomplete"));
                    }
                    destination.sync_all()?;
                }
            }
            sync_payload(&reference)?;
        }
        sync_dir(&self.staging)
    }

    fn matches(&self, path: &Path, index: usize) -> io::Result<bool> {
        let entry = &self.journal.entries[index];
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        let reference = self.staging.join(&entry.reference);
        let witness = fs::symlink_metadata(&reference)?;
        if !metadata.is_file()
            || !witness.is_file()
            || metadata.len() != entry.size
            || witness.len() != entry.size
        {
            return Err(invalid(format!(
                "placement payload conflicts with journal: {}",
                path.display()
            )));
        }
        if same_file(path, &reference)? || fingerprint(path)? == fingerprint(&reference)? {
            return Ok(true);
        }
        Err(invalid(format!(
            "placement payload conflicts with retained witness: {}",
            path.display()
        )))
    }

    fn settle_pending(
        &mut self,
        mover: &mut impl FnMut(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<()> {
        let Some(pending) = self.journal.pending else {
            return Ok(());
        };
        let source = self.path(pending.index, self.journal.entries[pending.index].location);
        let destination = self.path(pending.index, pending.to);
        match (
            self.matches(&source, pending.index)?,
            self.matches(&destination, pending.index)?,
        ) {
            (true, false) => mover(&source, &destination)?,
            (true, true) => fs::remove_file(&source)?,
            (false, true) => {}
            (false, false) => return Err(invalid("both placement paths are missing")),
        }
        self.commit_move(pending)
    }

    fn commit_move(&mut self, pending: PendingMove) -> io::Result<()> {
        let source = self.path(pending.index, self.journal.entries[pending.index].location);
        let destination = self.path(pending.index, pending.to);
        let metadata = fs::symlink_metadata(&destination)?;
        if !metadata.is_file()
            || metadata.len() != self.journal.entries[pending.index].size
            || source.try_exists()?
        {
            return Err(invalid("placement move did not complete"));
        }
        sync_payload(&destination)?;
        sync_dir(&self.root)?;
        sync_dir(&self.staging)?;
        self.journal.entries[pending.index].location = pending.to;
        self.journal.pending = None;
        self.save()
    }

    fn move_entry(
        &mut self,
        index: usize,
        to: Location,
        mover: &mut impl FnMut(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<()> {
        let pending = PendingMove { index, to };
        self.journal.pending = Some(pending);
        self.save()?;
        mover(
            &self.path(index, self.journal.entries[index].location),
            &self.path(index, to),
        )?;
        self.commit_move(pending)
    }

    fn resume(&mut self, mover: &mut impl FnMut(&Path, &Path) -> io::Result<()>) -> io::Result<()> {
        if self.journal.phase == Phase::Staging {
            self.ensure_references()?;
        }
        self.settle_pending(mover)?;
        loop {
            let (from, to, next) = match self.journal.phase {
                Phase::Staging => (Location::Original, Location::Staged, Phase::Installation),
                Phase::Installation => (
                    Location::Staged,
                    Location::Installed,
                    Phase::IdentityBinding,
                ),
                Phase::RollbackVacate => (
                    Location::Installed,
                    Location::Staged,
                    Phase::RollbackRestore,
                ),
                Phase::RollbackRestore => (Location::Staged, Location::Original, Phase::Complete),
                Phase::IdentityBinding | Phase::Complete => return Ok(()),
            };
            for index in 0..self.journal.entries.len() {
                if self.journal.entries[index].location == from {
                    self.move_entry(index, to, mover)?;
                }
            }
            self.journal.phase = next;
            self.save()?;
        }
    }

    fn retained_error(&self, error: io::Error) -> io::Error {
        io::Error::new(
            error.kind(),
            format!(
                "{error}; placement recovery pending; retained files at {}",
                self.staging.display()
            ),
        )
    }

    pub(super) fn run_with_move(
        &mut self,
        mut mover: impl FnMut(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<()> {
        if let Err(error) = self.resume(&mut mover) {
            self.journal.phase = Phase::RollbackVacate;
            self.save().map_err(|e| self.retained_error(e))?;
            // An unstarted failed move can be canceled; partially completed
            // moves retain their intent and are reconciled before rollback.
            if let Some(pending) = self.journal.pending {
                let source = self.path(pending.index, self.journal.entries[pending.index].location);
                let destination = self.path(pending.index, pending.to);
                if source.try_exists()? && !destination.try_exists()? {
                    self.journal.pending = None;
                    self.save()?;
                }
            }
            if self.resume(&mut mover).is_ok() {
                self.cleanup()?;
                return Err(error);
            }
            return Err(self.retained_error(error));
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> io::Result<()> {
        if self.journal.phase != Phase::IdentityBinding {
            return Err(invalid("placement identities cannot finish in this phase"));
        }
        self.journal.phase = Phase::Complete;
        self.save()?;
        self.cleanup()
    }

    fn cleanup(&self) -> io::Result<()> {
        // Never recursively remove a transaction: an unexpected retained file
        // is evidence to report, not debris to discard.
        for item in fs::read_dir(&self.staging)? {
            let item = item?;
            if item.file_name() != JOURNAL
                && !self
                    .journal
                    .entries
                    .iter()
                    .any(|entry| item.file_name() == entry.reference.as_str())
            {
                return Err(self.retained_error(invalid("unexpected retained placement file")));
            }
            if !item.file_type()?.is_file() {
                return Err(invalid("unexpected placement witness type"));
            }
        }
        for (index, entry) in self.journal.entries.iter().enumerate() {
            if self.staging.join(&entry.reference).try_exists()?
                && !self.matches(&self.path(index, entry.location), index)?
            {
                return Err(
                    self.retained_error(invalid("cannot remove witness of a missing payload"))
                );
            }
            match fs::remove_file(self.staging.join(&entry.reference)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        fs::remove_file(self.staging.join(JOURNAL))?;
        fs::remove_dir(&self.staging)?;
        sync_dir(&self.root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(unix, windows))]
    #[test]
    fn placement_and_hard_link_recovery_do_not_read_payload_bytes() {
        let dir = tempfile::tempdir().unwrap();
        for (index, name) in ["a.bin", "b.bin", "c.bin"].iter().enumerate() {
            fs::write(dir.path().join(name), [index as u8; 32]).unwrap();
        }
        PAYLOAD_HASH_READS.with(|reads| reads.set(0));
        let transaction = begin(dir.path(), &plan(), vec![]).unwrap().unwrap();
        drop(transaction);
        recover(dir.path())
            .unwrap()
            .pop()
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(
            PAYLOAD_HASH_READS.with(|reads| reads.get()),
            0,
            "ordinary placement must not stall settlement behind another full payload read"
        );
    }

    fn plan() -> par2_rs::PlacementPlan {
        par2_rs::PlacementPlan {
            exact: vec![],
            swaps: vec![],
            unresolved: vec![],
            conflicts: vec![],
            renames: [("a.bin", "b.bin"), ("b.bin", "c.bin"), ("c.bin", "a.bin")]
                .into_iter()
                .enumerate()
                .map(|(index, (source, destination))| par2_rs::PlacementEntry {
                    file_id: par2_rs::FileId::from_bytes([index as u8; 16]),
                    current_name: source.into(),
                    correct_name: destination.into(),
                })
                .collect(),
        }
    }

    // Invoked in a child test process so no unwinding or destructor can make
    // the simulated crash safer than a forced process exit.
    #[test]
    fn placement_crash_child() {
        let Some(root) = std::env::var_os("WEAVER_PLACEMENT_CRASH_ROOT") else {
            return;
        };
        let boundary: usize = std::env::var("WEAVER_PLACEMENT_CRASH_BOUNDARY")
            .unwrap()
            .parse()
            .unwrap();
        let plan = plan();
        let bindings = plan
            .renames
            .iter()
            .enumerate()
            .map(|(index, entry)| Binding {
                file_index: index as u32,
                filename: entry.correct_name.clone(),
            })
            .collect();
        let mut transaction = prepare(Path::new(&root), &plan, bindings).unwrap().unwrap();
        let mut moves = 0;
        transaction
            .run_with_move(|source, destination| {
                rename_no_overwrite(source, destination)?;
                moves += 1;
                if moves == boundary {
                    std::process::exit(77);
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(boundary, 7);
        std::process::exit(77);
    }

    #[test]
    fn placement_replays_duplicate_links_and_interrupted_rollback() {
        for copied in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            for (index, name) in ["a.bin", "b.bin", "c.bin"].iter().enumerate() {
                fs::write(dir.path().join(name), [index as u8; 32]).unwrap();
            }
            let mut transaction = prepare(dir.path(), &plan(), vec![]).unwrap().unwrap();
            let pending = PendingMove {
                index: 0,
                to: Location::Staged,
            };
            transaction.journal.pending = Some(pending);
            transaction.save().unwrap();
            let source = transaction.path(0, Location::Original);
            let destination = transaction.path(0, Location::Staged);
            if copied {
                fs::copy(&source, &destination).unwrap();
            } else {
                fs::hard_link(&source, &destination).unwrap();
            }
            drop(transaction);
            let mut recovered = recover(dir.path()).unwrap().pop().unwrap();
            // Interrupt rollback after vacating one installed destination.
            recovered.journal.phase = Phase::RollbackVacate;
            recovered.save().unwrap();
            recovered
                .move_entry(0, Location::Staged, &mut rename_no_overwrite)
                .unwrap();
            drop(recovered);
            assert!(recover(dir.path()).unwrap().is_empty());
            for (index, name) in ["a.bin", "b.bin", "c.bin"].iter().enumerate() {
                assert_eq!(fs::read(dir.path().join(name)).unwrap(), [index as u8; 32]);
            }
        }
    }

    #[test]
    fn placement_preserves_partial_copies_collisions_and_corrupt_journals() {
        for fault in ["copy", "collision", "journal"] {
            let dir = tempfile::tempdir().unwrap();
            for (index, name) in ["a.bin", "b.bin", "c.bin"].iter().enumerate() {
                fs::write(dir.path().join(name), [index as u8; 32]).unwrap();
            }
            let mut transaction = prepare(dir.path(), &plan(), vec![]).unwrap().unwrap();
            transaction.journal.pending = Some(PendingMove {
                index: 0,
                to: Location::Staged,
            });
            transaction.save().unwrap();
            let staging = transaction.staging.clone();
            match fault {
                "copy" => fs::write(staging.join("a.bin"), [0; 3]).unwrap(),
                "collision" => fs::write(staging.join("a.bin"), [9; 32]).unwrap(),
                _ => fs::write(staging.join(JOURNAL), b"{bad journal").unwrap(),
            }
            drop(transaction);
            assert!(recover(dir.path()).is_err());
            for (index, name) in ["a.bin", "b.bin", "c.bin"].iter().enumerate() {
                assert_eq!(fs::read(dir.path().join(name)).unwrap(), [index as u8; 32]);
            }
            assert!(staging.exists());
        }
    }

    #[cfg(windows)]
    #[test]
    fn placement_recovers_case_only_name() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("Payload.bin"), b"payload").unwrap();
        let mut plan = plan();
        plan.renames.truncate(1);
        plan.renames[0].current_name = "Payload.bin".into();
        plan.renames[0].correct_name = "payload.bin".into();
        let transaction = begin(dir.path(), &plan, vec![]).unwrap().unwrap();
        drop(transaction);
        recover(dir.path())
            .unwrap()
            .pop()
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(
            fs::read(dir.path().join("payload.bin")).unwrap(),
            b"payload"
        );
    }
}
