//! Authenticated PAR3 carrier discovery. All scanning runs on blocking workers;
//! the actor retains packet locations and incomplete metadata between arrivals.

use super::sources::PublishedSources;
use crate::jobs::ids::{JobId, NzbFileId};
use crate::pipeline::Pipeline;
use par3_rs::ingest::{PacketScanner, ScanEvent};
use par3_rs::runtime::{EngineError, EngineResult, ExecutionOptions, HandleBudget};
use par3_rs::source::{DiskSourceAccess, SourceAccess, SourceId, SourceSnapshot};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use weaver_model::files::FileRole;

const MAX_CARRIERS: usize = 4096;
const MAX_SETS: usize = 256;

fn execution_options() -> ExecutionOptions {
    static HANDLES: OnceLock<HandleBudget> = OnceLock::new();
    let mut options = ExecutionOptions::default();
    options.memory = budget::budgets().native.clone();
    // What a session retains follows its set's block count, not its byte size
    // and not the host's RAM: measured against the engine's own ledger, the
    // same block count retained identical bytes at 512 B and at 64 KiB blocks.
    // The measured line is about 48 bytes per block plus roughly 90 KB fixed —
    // 0.29 MB at 4 096 blocks, 0.88 MB at 16 384, 3.25 MB at 65 531, and a few
    // hundred KB for small sets. Half the native budget therefore admits sets
    // of order ten million blocks, far above anything measured here, and this
    // is a ceiling rather than a reservation: a session that never approaches
    // it costs nothing, while a lower one would refuse large sets outright.
    options.retained_bytes = options.memory.limit() / 2;
    options.handles = HANDLES.get_or_init(|| HandleBudget::new(128)).clone();
    options.open_handles = 128;
    options.workers = 1;
    options.stripe_bytes = 1 << 20;
    options
}

fn disk_source(
    source: SourceId,
    path: PathBuf,
    options: &ExecutionOptions,
) -> EngineResult<Arc<dyn SourceAccess>> {
    #[cfg(windows)]
    {
        disk_windows::open(source, path, options)
    }
    #[cfg(not(windows))]
    {
        let mut disk = DiskSourceAccess::with_options(options.clone());
        disk.insert(source, path);
        Ok(Arc::new(disk))
    }
}

struct Carrier {
    backing: SourceSnapshot,
    published: SourceSnapshot,
    path: Option<PathBuf>,
    scanner: PacketScanner,
    scan_start: u64,
    revision: u64,
    needed: Option<u64>,
    resume: Option<u64>,
    /// What this carrier's own scan has found so far.
    scan: carriers::CarrierScan,
}

struct DiskPublication {
    path: PathBuf,
    name: String,
    backing: SourceSnapshot,
    published: SourceSnapshot,
}

pub(in crate::pipeline) struct Par3Job {
    options: ExecutionOptions,
    sources: PublishedSources,
    carriers: BTreeMap<SourceId, Carrier>,
    sets: BTreeMap<par3_rs::InputSetId, assessment::SetSession>,
    dormant_views: BTreeMap<par3_rs::InputSetId, assessment::AssessmentView>,
    bindings: BTreeMap<String, SourceId>,
    disk_publications: BTreeMap<SourceId, DiskPublication>,
    name_search: placement::NameSearch,
    donor_search: donors::Cache,
    publication_memory: BTreeMap<SourceId, assessment::ViewReservation>,
    virtual_readers: Arc<virtual_source::ReaderCache>,
    /// Carrier-scan tallies. Plain integers advanced by the scan loop and
    /// folded into the process metrics once, at the work unit's handback.
    packets_authenticated: u64,
    packets_rejected: u64,
    ranges_unavailable: u64,
    /// Option packets that File, Directory and Root packets point at.
    referenced_options: std::collections::BTreeSet<par3_rs::Fingerprint>,
    /// Whether any authenticated Root declares the set's paths absolute.
    absolute_paths: bool,
    /// Each admitted set's input block size, from its own Start packet. A File
    /// packet cannot be read without it. Bounded by the set ceiling, because
    /// an entry is only made for a set that was admitted.
    set_block_sizes: std::collections::BTreeMap<par3_rs::InputSetId, u64>,
}

/// What one authenticated metadata packet says about option packets, captured
/// before the packet is handed to its set.
enum PacketNote {
    Nothing,
    /// This packet points at option packets, and may declare absolute paths.
    References {
        hashes: Vec<par3_rs::Fingerprint>,
        absolute: bool,
    },
}

/// What one job's carriers said about option packets. Weaver applies no
/// option packet, so this exists to be reported, never to change a plan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::pipeline) struct OptionPacketTally {
    /// Distinct link and permission packets the resolved sets retained. The
    /// engine keeps them verbatim and interprets none of them, so this is the
    /// count it already holds rather than one weaver keeps beside it.
    pub present: u64,
    /// Distinct option packets File, Directory and Root packets point at.
    pub referenced: u64,
    /// Pointers naming an option packet nothing authenticated.
    pub unresolved: u64,
    /// Whether a Root declared the set's paths absolute.
    pub absolute_paths: bool,
}

impl OptionPacketTally {
    /// Whether there is anything worth saying about this job's options.
    pub fn is_silent(&self) -> bool {
        self.present == 0 && self.referenced == 0 && !self.absolute_paths
    }
}

impl std::fmt::Display for OptionPacketTally {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} option packet(s) ignored, {} referenced, {} reference(s) unresolved",
            self.present, self.referenced, self.unresolved
        )?;
        if self.absolute_paths {
            f.write_str(", set declares absolute paths")?;
        }
        Ok(())
    }
}

/// The most option-packet pointers one job remembers. Past this the reference
/// count stops rising, so a set with a hostile number of pointers cannot grow
/// this set without bound. The option packets themselves are not counted here:
/// the engine already holds each resolved set's own tally.
const MAX_OPTION_HASHES: usize = 4096;

impl PacketNote {
    /// Read what one authenticated packet says about option packets.
    ///
    /// A File packet's body cannot be parsed without its set's block size, so
    /// the scanner retains it verbatim. `block_size` is what this job has
    /// learned from that set's Start packet, and is `None` until the Start
    /// packet authenticates: a File packet that arrives ahead of its own Start
    /// contributes no reference, which under-reports option pointers and never
    /// over-reports them.
    fn of(packet: &par3_rs::packet::Packet, block_size: Option<u64>) -> Self {
        use par3_rs::packet::{PacketBody, PacketType, file::FilePacket};
        match packet.body() {
            PacketBody::Opaque {
                packet_type: PacketType::File,
                body,
            } => block_size
                .and_then(|block_size| FilePacket::parse(body, block_size).ok())
                .filter(|file| !file.option_hashes.is_empty())
                .map_or(Self::Nothing, |file| Self::References {
                    hashes: file.option_hashes,
                    absolute: false,
                }),
            PacketBody::File(file) if !file.option_hashes.is_empty() => Self::References {
                hashes: file.option_hashes.clone(),
                absolute: false,
            },
            PacketBody::Directory(directory) if !directory.option_hashes.is_empty() => {
                Self::References {
                    hashes: directory.option_hashes.clone(),
                    absolute: false,
                }
            }
            PacketBody::Root(root) if !root.option_hashes.is_empty() || root.is_absolute_path() => {
                Self::References {
                    hashes: root.option_hashes.clone(),
                    absolute: root.is_absolute_path(),
                }
            }
            _ => Self::Nothing,
        }
    }

    fn apply(
        self,
        referenced: &mut std::collections::BTreeSet<par3_rs::Fingerprint>,
        absolute: &mut bool,
    ) {
        match self {
            Self::Nothing => {}
            Self::References {
                hashes,
                absolute: declared,
            } => {
                *absolute |= declared;
                for hash in hashes {
                    if referenced.len() >= MAX_OPTION_HASHES {
                        break;
                    }
                    referenced.insert(hash);
                }
            }
        }
    }
}

impl Default for Par3Job {
    fn default() -> Self {
        Self {
            options: execution_options(),
            sources: PublishedSources::default(),
            carriers: BTreeMap::new(),
            sets: BTreeMap::new(),
            dormant_views: BTreeMap::new(),
            bindings: BTreeMap::new(),
            disk_publications: BTreeMap::new(),
            name_search: placement::NameSearch::default(),
            donor_search: donors::Cache::default(),
            publication_memory: BTreeMap::new(),
            virtual_readers: Arc::default(),
            packets_authenticated: 0,
            packets_rejected: 0,
            ranges_unavailable: 0,
            referenced_options: std::collections::BTreeSet::new(),
            absolute_paths: false,
            set_block_sizes: std::collections::BTreeMap::new(),
        }
    }
}

impl Par3Job {
    fn publish_file(
        &mut self,
        source: SourceId,
        path: PathBuf,
        name: String,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> EngineResult<()> {
        bindings::check_source(source)?;
        if !self.disk_publications.contains_key(&source)
            && self.disk_publications.len() >= MAX_CARRIERS
        {
            return Err(budget::host_limit("PAR3 disk publications"));
        }
        let access = disk_source(source, path.clone(), &self.options)?;
        let backing = access.snapshot(source)?.ok_or(EngineError::Unavailable {
            source_id: source,
            offset: 0,
        })?;
        let mut continuity = None;
        if let Some(old) = self.disk_publications.get(&source)
            && old.path == path
            && old.name == name
            && old.backing == backing
            && self.bindings.get(&name) == Some(&source)
        {
            if self
                .sources
                .matches_publication(source, old.published, backing, &ranges)?
            {
                return Ok(());
            }
            if self
                .sources
                .can_extend_publication(source, old.published, backing, &ranges)?
            {
                continuity = Some((old.published, backing));
            }
        }
        // Queue/publication leases already charge twice for paths and names:
        // the source/binding owns one copy and this identity record the other.
        let published = self.publish_access(source, access, name.clone(), ranges, continuity)?;
        self.disk_publications.insert(
            source,
            DiskPublication {
                path,
                name,
                backing,
                published,
            },
        );
        Ok(())
    }

    fn publish_access(
        &mut self,
        source: SourceId,
        access: Arc<dyn SourceAccess>,
        name: String,
        ranges: Vec<std::ops::Range<u64>>,
        continuity: Option<(SourceSnapshot, SourceSnapshot)>,
    ) -> EngineResult<SourceSnapshot> {
        bindings::check_source(source)?;
        if !self.bindings.contains_key(&name) && self.bindings.len() >= MAX_CARRIERS {
            return Err(budget::host_limit("PAR3 source bindings"));
        }
        let snapshot = access.snapshot(source)?.ok_or(EngineError::Unavailable {
            source_id: source,
            offset: 0,
        })?;
        // Only decoded placements committed by assembly are published. A file's
        // apparent length, including sparse zeroes, supplies no coverage proof.
        let published = if let Some((published, backing)) = continuity {
            self.sources.arrive_unchanged(
                source,
                access,
                snapshot.len,
                ranges,
                published,
                backing,
            )?
        } else {
            self.sources.replace(source, access, snapshot.len, ranges)?
        };
        self.disk_publications.remove(&source);
        for set in self.sets.values_mut() {
            if continuity.is_some() {
                set.source_arrived(source, &self.options)?;
            } else {
                set.invalidate(source);
            }
        }
        // Retire both host and native names when this stable source is rebound.
        self.retire_name_bindings(source, &name)?;
        self.bindings.retain(|_, bound| *bound != source);
        self.bindings.insert(name, source);
        Ok(published)
    }

    fn publish_virtual(
        &mut self,
        source: SourceId,
        image: virtual_source::VirtualInput,
        name: String,
    ) -> EngineResult<()> {
        let ranges = image
            .volume
            .readable_ranges()
            .into_iter()
            .map(|(start, end)| start..end)
            .collect();
        let access = virtual_source::VirtualSource::new(
            source,
            image,
            self.options.clone(),
            Arc::clone(&self.virtual_readers),
        )?;
        self.publish_access(source, Arc::new(access), name, ranges, None)
            .map(|_| ())
    }

    /// Keep bounded scheduling facts while releasing idle native analysis.
    /// Only recovery-waiting jobs are eligible; no queued repair loses its plan.
    fn evict_native_sessions(&mut self) {
        for (id, mut set) in std::mem::take(&mut self.sets) {
            if let Some(view) = set.view.take() {
                self.dormant_views.insert(id, view);
            }
        }
        for carrier in self.carriers.values_mut() {
            carrier.revision = 0;
            carrier.resume = Some(carrier.scan_start);
        }
        self.name_search = placement::NameSearch::default();
        self.donor_search.evict();
    }

    fn assess(&mut self) -> EngineResult<()> {
        if !self.dormant_views.is_empty() {
            // Reopen from currently published carrier ranges on this worker.
            // Reauthenticate packets and source bytes instead of inventing
            // serialized codec state or treating cached UI facts as evidence.
            let _reservation = assessment::ViewReservation::acquire(self.carriers.len() * 8)?;
            let carriers: Vec<_> = self.carriers.keys().copied().collect();
            for source in carriers {
                self.scan(source)?;
            }
            self.dormant_views.clear();
        }
        self.validate_shared_layouts()?;
        for set in self.sets.values_mut() {
            set.view = None;
            if let Some(layout) = set.native.layout()? {
                for file in layout.files() {
                    if let Some(&source) = self.bindings.get(&file.path) {
                        set.native.bind_file(&file.path, source)?;
                    }
                }
            }
            set.assess()?;
        }
        self.discover_name()?;
        if self.name_search.source().is_none() {
            self.discover_donors()?;
        }
        Ok(())
    }

    fn repair(
        &mut self,
        id: par3_rs::InputSetId,
        output: &std::path::Path,
    ) -> EngineResult<par3_rs::session_repair::SessionRepairReport> {
        use super::backend::{Par3RepairRequest, RepairBackend};
        let set = self
            .sets
            .get_mut(&id)
            .ok_or(EngineError::InvalidState("unknown PAR3 set"))?;
        set.view = None;
        if let Some(layout) = set.native.layout()?
            && layout.files().iter().any(|file| {
                file.extents
                    .iter()
                    .any(|extent| matches!(extent.kind, par3_rs::layout::ExtentKind::Unprotected))
            })
        {
            let matrix = set.cauchy_matrix.ok_or(EngineError::InvalidState(
                "embedded recovery matrix unavailable",
            ))?;
            return inside::repair(&mut set.native, matrix, output, &self.options);
        }
        set.native.execute(Par3RepairRequest {
            output,
            backup: false,
        })
    }

    /// Disk carrier publication. Live downloads supply committed ranges.
    /// Completed carriers restored without placements use their actual disk
    /// extent; every admitted packet still requires authentication.
    fn scan_file(
        &mut self,
        source: SourceId,
        path: PathBuf,
        ranges: Option<Vec<std::ops::Range<u64>>>,
    ) -> EngineResult<()> {
        self.scan_file_from(source, path, ranges, 0)
    }

    fn scan_embedded(
        &mut self,
        source: SourceId,
        path: PathBuf,
        name: String,
        ranges: Option<Vec<std::ops::Range<u64>>>,
        start: u64,
    ) -> EngineResult<()> {
        bindings::check_source(source)?;
        if !self.bindings.contains_key(&name) && self.bindings.len() >= MAX_CARRIERS {
            return Err(budget::host_limit("PAR3 source bindings"));
        }
        self.retire_name_bindings(source, &name)?;
        self.bindings.retain(|_, bound| *bound != source);
        self.bindings.insert(name.clone(), source);
        self.scan_file_from(source, path, ranges, start)?;
        // Damaged framing may find only the metadata following a large
        // recovery packet. Its authenticated unprotected extent tells us
        // where the carrier really begins, without scanning archive payloads.
        let mut earliest = start;
        for set in self.sets.values_mut() {
            if let Some(layout) = set.native.layout()? {
                for file in layout.files().iter().filter(|file| file.path == name) {
                    for extent in &file.extents {
                        if matches!(extent.kind, par3_rs::layout::ExtentKind::Unprotected) {
                            earliest = earliest.min(extent.range.start);
                        }
                    }
                }
            }
        }
        let carrier = self
            .carriers
            .get_mut(&source)
            .expect("published embedded carrier");
        if earliest < carrier.scan_start {
            // Remember the authenticated floor for unchanged publications. A
            // source generation change creates a new carrier and drops it.
            let rewind = carrier
                .resume
                .take()
                .map_or(earliest, |old| old.min(earliest));
            carrier.scanner.seek(rewind)?;
            carrier.scan_start = earliest;
            carrier.revision = 0;
            self.scan(source)?;
        }
        Ok(())
    }

    fn scan_file_from(
        &mut self,
        source: SourceId,
        path: PathBuf,
        ranges: Option<Vec<std::ops::Range<u64>>>,
        start: u64,
    ) -> EngineResult<()> {
        bindings::check_source(source)?;
        let access = disk_source(source, path.clone(), &self.options)?;
        let snapshot = access.snapshot(source)?.ok_or(EngineError::Unavailable {
            source_id: source,
            offset: 0,
        })?;
        let ranges = ranges.unwrap_or_else(|| {
            if snapshot.len == 0 {
                Vec::new()
            } else {
                std::iter::once(0..snapshot.len).collect()
            }
        });
        if let Some(carrier) = self.carriers.get(&source)
            && carrier.path.as_ref() == Some(&path)
            && carrier.backing == snapshot
        {
            if self
                .sources
                .matches_publication(source, carrier.published, snapshot, &ranges)?
            {
                return self.scan(source);
            }
            if self
                .sources
                .can_extend_publication(source, carrier.published, snapshot, &ranges)?
            {
                self.sources.arrive_unchanged(
                    source,
                    access,
                    snapshot.len,
                    ranges,
                    carrier.published,
                    snapshot,
                )?;
                return self.scan(source);
            }
        }
        self.publish_carrier(source, access, snapshot.len, ranges, false)?;
        let carrier = self.carriers.get_mut(&source).expect("published carrier");
        carrier.path = Some(path);
        carrier.scanner.seek(start)?;
        carrier.scan_start = start;
        self.scan(source)
    }

    fn publish_carrier(
        &mut self,
        source: SourceId,
        access: Arc<dyn SourceAccess>,
        len: u64,
        ranges: Vec<std::ops::Range<u64>>,
        arrival: bool,
    ) -> EngineResult<()> {
        bindings::check_source(source)?;
        if !self.carriers.contains_key(&source) && self.carriers.len() >= MAX_CARRIERS {
            return Err(budget::host_limit("job carrier count"));
        }
        let backing = access.snapshot(source)?.ok_or(EngineError::Unavailable {
            source_id: source,
            offset: 0,
        })?;
        let published = if arrival {
            self.sources.arrive(source, access, len, ranges)?
        } else {
            let published = self.sources.replace(source, access, len, ranges)?;
            self.carriers.remove(&source);
            published
        };
        self.disk_publications.remove(&source);
        if let Some(carrier) = self.carriers.get_mut(&source) {
            carrier.backing = backing;
            carrier.published = published;
            carrier.path = None;
        } else {
            let scanner = PacketScanner::new(
                Arc::new(self.sources.clone()),
                source,
                self.options.clone(),
                par3_rs::ScanLimits::default(),
            )?;
            self.carriers.insert(
                source,
                Carrier {
                    backing,
                    published,
                    path: None,
                    scanner,
                    scan_start: 0,
                    revision: 0,
                    needed: None,
                    resume: None,
                    scan: carriers::CarrierScan::default(),
                },
            );
        }
        Ok(())
    }

    fn scan(&mut self, source: SourceId) -> EngineResult<()> {
        let revision = self
            .sources
            .revision(source)?
            .ok_or(EngineError::InvalidState("unpublished carrier"))?;
        let carrier = self
            .carriers
            .get_mut(&source)
            .ok_or(EngineError::InvalidState("unknown carrier"))?;
        if revision == carrier.revision {
            return Ok(());
        }
        if let Some(resume) = carrier.resume.take() {
            carrier.scanner.seek(resume)?;
        }
        carrier.needed = None;
        loop {
            match carrier.scanner.poll()? {
                ScanEvent::Packet(packet) => {
                    let id = packet.input_set_id();
                    let kind = carriers::Par3PacketKind::of(&packet);
                    let origin = packet.origin();

                    if !self.sets.contains_key(&id) {
                        if self.sets.len() >= MAX_SETS {
                            return Err(budget::host_limit("job PAR3 set count"));
                        }
                        self.sets.insert(
                            id,
                            assessment::SetSession::new(
                                id,
                                self.sources.clone(),
                                self.options.clone(),
                            )?,
                        );
                    }
                    // A Start packet is what makes every File packet of its
                    // own set readable, so remember its block size before the
                    // packet moves into the set.
                    if let Some(metadata) = packet.metadata()
                        && let par3_rs::packet::PacketBody::Start(start) = metadata.body()
                    {
                        self.set_block_sizes.insert(id, start.block_size);
                    }
                    let note = packet.metadata().map_or(PacketNote::Nothing, |metadata| {
                        PacketNote::of(metadata, self.set_block_sizes.get(&id).copied())
                    });
                    // A set that will not admit an authenticated packet has
                    // said something about that packet, not about the rest of
                    // the carrier. Record the refusal and keep scanning, so
                    // one refused copy cannot cost every packet behind it. An
                    // exhausted budget is different: it is about the job, and
                    // scanning further would only deepen it.
                    match self.sets.get_mut(&id).expect("inserted set").merge(packet) {
                        Ok(()) => {
                            carrier.scan.note_packet(kind, origin.offset, origin.length);
                            note.apply(&mut self.referenced_options, &mut self.absolute_paths);
                            self.packets_authenticated += 1;
                        }
                        Err(error) if is_admission_exhausted(&error) => return Err(error),
                        Err(error) => {
                            carrier.scan.note_rejected(origin.offset, origin.length);
                            self.packets_rejected += 1;
                            tracing::debug!(
                                source = source.0,
                                offset = origin.offset,
                                error = %error,
                                "PAR3 set refused an authenticated packet"
                            );
                        }
                    }
                }
                ScanEvent::NeedData { offset } => {
                    carrier.needed = Some(carrier.needed.map_or(offset, |old| old.min(offset)));
                    if let Some(next) = self.sources.next_available(source, offset)?
                        && next.start > offset
                    {
                        // Seeking past a hole discards the pending packet hash.
                        // Otherwise leave it intact for the next arrival.
                        let position = carrier.scanner.position();
                        carrier.resume =
                            Some(carrier.resume.map_or(position, |old| old.min(position)));
                        carrier.scanner.seek(next.start)?;
                        carrier.scan.note_unavailable(next.start);
                        self.ranges_unavailable += 1;
                        continue;
                    }
                    carrier.revision = revision;
                    return Ok(());
                }
                ScanEvent::End => {
                    carrier.scan.note_end(carrier.published.len);
                    carrier.revision = revision;
                    return Ok(());
                }
            }
        }
    }

    /// Every carrier that reported damage, newest scan state, bounded so one
    /// summary can never grow with the carrier count.
    fn damage_report(&self) -> Vec<carriers::CarrierDamage> {
        const SHOWN: usize = 8;
        self.carriers
            .iter()
            .filter(|(_, carrier)| carrier.scan.is_damaged())
            .take(SHOWN)
            .map(|(&source, carrier)| carriers::CarrierDamage {
                source,
                first_damage_offset: carrier.scan.first_damage_offset.unwrap_or_default(),
                damaged_bytes: carrier.scan.damaged_bytes,
                rejected: carrier.scan.rejected,
                unavailable_ranges: carrier.scan.unavailable_ranges,
            })
            .collect()
    }

    /// Authenticated packets of each family across every carrier of this job.
    fn authenticated_families(&self) -> [u64; carriers::Par3PacketKind::COUNT] {
        let mut totals = [0u64; carriers::Par3PacketKind::COUNT];
        for carrier in self.carriers.values() {
            for (total, count) in totals.iter_mut().zip(carrier.scan.authenticated) {
                *total = (*total).saturating_add(count);
            }
        }
        totals
    }

    /// What this job said about option packets: how many each resolved set
    /// retained, how many distinct option packets the metadata points at, and
    /// how many of those pointers name nothing the set holds.
    ///
    /// The packets themselves are the engine's own tally, read off each
    /// resolved set rather than kept a second time here. The pointers are not:
    /// a resolved set exposes the File and Directory packets its Root tree
    /// reaches, so a pointer in an authenticated packet the tree never names
    /// is invisible there and is counted from the scan instead.
    fn option_packet_tally(&self) -> OptionPacketTally {
        let resolved = || self.sets.values().filter_map(|set| set.native.set());
        OptionPacketTally {
            present: resolved().fold(0u64, |count, set| {
                count.saturating_add(set.option_packet_count() as u64)
            }),
            referenced: self.referenced_options.len() as u64,
            unresolved: self
                .referenced_options
                .iter()
                .filter(|hash| !resolved().any(|set| set.option_packet(hash).is_some()))
                .count() as u64,
            absolute_paths: self.absolute_paths,
        }
    }

    /// Readable carrier bytes across this job that produced no authenticated
    /// packet.
    fn damaged_bytes(&self) -> u64 {
        self.carriers.values().fold(0u64, |bytes, carrier| {
            bytes.saturating_add(carrier.scan.damaged_bytes)
        })
    }
}

/// Whether an engine refusal is about the job's exhausted budget rather than
/// about the one packet it was handed.
fn is_admission_exhausted(error: &EngineError) -> bool {
    matches!(
        error,
        EngineError::ResourceLimit(_)
            | EngineError::Cancelled
            | EngineError::OutputInterrupted { .. }
    )
}

impl Pipeline {
    pub(in crate::pipeline) async fn try_load_par3_metadata(
        &mut self,
        job_id: JobId,
        file_id: NzbFileId,
    ) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let Some(file) = state.assembly.file(file_id) else {
            return;
        };
        let prefix = self.file_prefix_16k.get(&file_id);
        let signature = prefix.is_some_and(|prefix| prefix.starts_with(par3_rs::MAGIC));
        let container_signature = prefix.is_some_and(|prefix| {
            prefix.starts_with(b"PK\x03\x04")
                || prefix.starts_with(b"PK\x05\x06")
                || prefix.starts_with(&[0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c])
        });
        if !file.is_complete() && self.job_has_pending_download_pipeline_work(job_id) {
            return;
        }
        let embedded = if !self.par3_inside_probes.contains(file_id)
            && !signature
            && (container_signature
                || matches!(
                    file.role(),
                    FileRole::ZipArchive | FileRole::SevenZipArchive
                ))
            && !state
                .spec
                .files
                .iter()
                .any(|file| matches!(file.role, FileRole::Par2 { .. }))
        {
            let path = state
                .working_dir
                .join(self.current_filename_for_file(job_id, file));
            self.par3_inside_probes.insert(file_id);
            match tokio::task::spawn_blocking(move || inside::probe(path)).await {
                Ok(Ok(start)) => start,
                Ok(Err(error)) => {
                    tracing::warn!(job_id = job_id.0, file_index = file_id.file_index, %error,
                        "embedded PAR3 probe unavailable; continuing without a carrier hint");
                    None
                }
                Err(error) => {
                    tracing::warn!(job_id = job_id.0, file_index = file_id.file_index, %error,
                        "embedded PAR3 probe worker failed; continuing without a carrier hint");
                    None
                }
            }
        } else {
            None
        };
        let carrier = matches!(
            self.jobs[&job_id]
                .assembly
                .file(file_id)
                .expect("completed file")
                .role(),
            FileRole::Par3 { .. }
        ) || signature
            || embedded.is_some();
        let admitted = self
            .par3_runtime
            .as_ref()
            .is_some_and(|runtime| runtime.contains_job(job_id));
        if !carrier {
            if admitted && let Err(error) = self.enqueue_par3_file(job_id, file_id) {
                self.fail_job(job_id, format!("PAR3 source publication failed: {error}"));
            }
            return;
        }
        self.par3_runtime.get_or_insert_with(|| {
            Box::new(work::Coordinator::new(
                self.repair_work_done_tx.clone(),
                Arc::clone(&self.metrics),
            ))
        });
        if let Err(error) = self.enqueue_par3_file_with_inside(job_id, file_id, embedded) {
            self.fail_job(job_id, format!("PAR3 discovery failed: {error}"));
            return;
        }
        if !admitted {
            // Metadata may arrive after every protected file. Snapshot only
            // committed placements; verification itself belongs to the worker.
            let files: Vec<_> = self.jobs[&job_id]
                .assembly
                .files()
                .filter(|file| {
                    !file.role().is_recovery()
                        && file.file_id() != file_id
                        && (file.is_complete()
                            || (0..file.total_segments()).any(|segment| file.has_segment(segment)))
                })
                .take(MAX_CARRIERS + 1)
                .map(|file| file.file_id())
                .collect();
            if files.len() > MAX_CARRIERS {
                self.fail_job(job_id, "PAR3 source count exceeds the job limit".into());
                return;
            }
            for file in files {
                if let Err(error) = self.enqueue_par3_file(job_id, file) {
                    self.fail_job(job_id, format!("PAR3 source publication failed: {error}"));
                    return;
                }
            }
        }
        if let Err(error) = self
            .par3_runtime
            .as_mut()
            .expect("admitted PAR3 job")
            .dispatch()
        {
            self.fail_job(job_id, format!("PAR3 worker dispatch failed: {error}"));
        }
    }

    pub(in crate::pipeline) async fn discover_embedded_par3_at_completion(
        &mut self,
        job_id: JobId,
    ) {
        if self.jobs.get(&job_id).is_none_or(|state| {
            state
                .spec
                .files
                .iter()
                .any(|file| matches!(file.role, FileRole::Par2 { .. }))
        }) {
            return;
        }
        if self.job_has_pending_download_pipeline_work(job_id) {
            return;
        }
        loop {
            let candidate = self.jobs.get(&job_id).and_then(|state| {
                state
                    .assembly
                    .files()
                    .find(|file| {
                        matches!(
                            file.role(),
                            FileRole::ZipArchive | FileRole::SevenZipArchive
                        ) && !self.par3_inside_probes.contains(file.file_id())
                            && (file.is_complete()
                                || (0..file.total_segments()).any(|part| file.has_segment(part)))
                    })
                    .map(|file| file.file_id())
            });
            let Some(file) = candidate else {
                return;
            };
            self.try_load_par3_metadata(job_id, file).await;
            // A source still awaiting publication must not cause a retry loop.
            if !self.par3_inside_probes.contains(file) {
                return;
            }
        }
    }

    fn enqueue_par3_file(&mut self, job_id: JobId, file_id: NzbFileId) -> EngineResult<()> {
        self.enqueue_par3_file_with_inside(job_id, file_id, None)
    }

    fn enqueue_par3_file_with_inside(
        &mut self,
        job_id: JobId,
        file_id: NzbFileId,
        embedded: Option<u64>,
    ) -> EngineResult<()> {
        if self
            .par3_runtime
            .as_ref()
            .is_some_and(|runtime| runtime.is_installing(job_id))
        {
            return Ok(());
        }
        if self.direct_demotion_in_flight.contains_key(&job_id) {
            // The handback republishes committed ranges after reconstruction.
            // Neither the old virtual image nor the growing disk image is a
            // stable publication while that ticket owns the destination.
            return Ok(());
        }
        let state = &self.jobs[&job_id];
        let Some(file) = state.assembly.file(file_id) else {
            return Ok(());
        };
        // Planned outputs have no articles or readable bytes until native
        // installation hands them back. Do not open a destination that is absent.
        if file.is_repair_output() && !file.is_complete() {
            return Ok(());
        }
        let name = self.current_filename_for_file(job_id, file);
        let path = state.working_dir.join(&name);
        let source = SourceId(u64::from(file_id.file_index));
        let embedded = embedded.or_else(|| {
            self.par3_runtime
                .as_ref()
                .and_then(|runtime| runtime.embedded_start(job_id, source))
        });
        let carrier = embedded.is_some()
            || matches!(file.role(), FileRole::Par3 { .. })
            || self
                .file_prefix_16k
                .get(&file_id)
                .is_some_and(|prefix| prefix.starts_with(par3_rs::MAGIC))
            || self
                .par3_runtime
                .as_ref()
                .is_some_and(|runtime| runtime.is_carrier(job_id, source));
        let mut ranges: Vec<std::ops::Range<u64>> = Vec::new();
        for segment in 0..file.total_segments() {
            if !file.has_segment(segment) {
                continue;
            }
            let Some((offset, len)) = file.placement_of(segment) else {
                continue;
            };
            if len == 0 {
                continue;
            }
            let end = offset
                .checked_add(u64::from(len))
                .ok_or(budget::host_limit("PAR3 source offsets"))?;
            if let Some(last) = ranges.last_mut()
                && last.end == offset
            {
                last.end = end;
            } else {
                if ranges.len() >= 262_144 {
                    return Err(budget::host_limit("PAR3 source ranges"));
                }
                ranges.push(offset..end);
            }
        }
        let virtual_volume = if carrier {
            None
        } else {
            self.par3_virtual_volume(file_id)
        };
        if virtual_volume.is_none()
            && let Some(materialized) = self
                .par3_runtime
                .as_ref()
                .expect("admitted PAR3 job")
                .materialized_ranges(job_id, source)?
        {
            // These frontiers belong to a completed demotion handback. Restore
            // progress for other files can use encoded units, so it must never
            // be treated as decoded source availability.
            let persisted = self
                .write_buffers
                .get(&file_id)
                .into_iter()
                .flat_map(|buffer| buffer.persisted_ranges());
            let floor = self
                .pending_file_progress
                .get(&file_id)
                .copied()
                .unwrap_or(0)
                .max(
                    self.persisted_file_progress
                        .get(&file_id)
                        .copied()
                        .unwrap_or(0),
                );
            for range in materialized
                .iter()
                .cloned()
                .chain((floor != 0).then_some(0..floor))
                .chain(persisted.map(|(offset, len)| offset..offset.saturating_add(len as u64)))
            {
                if ranges.len() >= 262_144 {
                    return Err(budget::host_limit("PAR3 source ranges"));
                }
                ranges.push(range);
            }
            ranges.sort_unstable_by_key(|range| range.start);
            ranges.dedup_by(|right, left| {
                if right.start <= left.end {
                    left.end = left.end.max(right.end);
                    true
                } else {
                    false
                }
            });
        }
        // Completed-file restore deliberately omits article placements. Its
        // disk image remains a candidate, with its actual length read by the
        // worker and every protected byte verified afresh. Never apply this
        // fallback to an incomplete source or to a virtual volume's holes.
        let complete_disk_image = file.is_complete()
            && virtual_volume.is_none()
            && (ranges.is_empty()
                || self
                    .par3_runtime
                    .as_ref()
                    .is_some_and(|runtime| runtime.has_complete_disk_image(job_id, source)));
        tracing::trace!(job_id = job_id.0, source = ?source, complete = file.is_complete(),
            virtual_volume = virtual_volume.is_some(), complete_disk_image, ranges = ?ranges,
            "PAR3 committed source publication queued");
        let coordinator = self.par3_runtime.as_mut().expect("admitted PAR3 job");
        if let Some(start) = embedded {
            coordinator.enqueue_embedded(
                job_id,
                source,
                path,
                name,
                if complete_disk_image {
                    None
                } else {
                    Some(ranges)
                },
                start,
            )?;
        } else if carrier && complete_disk_image {
            coordinator.enqueue_complete_carrier(job_id, source, path)?;
        } else if carrier {
            coordinator.enqueue_carrier_ranges(job_id, source, path, ranges)?;
        } else if let Some(volume) = virtual_volume {
            coordinator.enqueue_virtual(job_id, source, volume, name)?;
        } else if complete_disk_image {
            coordinator.enqueue_complete_file(job_id, source, path, name)?;
        } else {
            coordinator.enqueue_file(job_id, source, path, name, ranges)?;
        }
        coordinator.dispatch()
    }

    fn par3_virtual_volume(
        &self,
        file_id: NzbFileId,
    ) -> Option<crate::pipeline::direct_store::provider::VirtualVolume> {
        let set = self
            .direct_store
            .sets_for(file_id.job_id)
            .iter()
            .find(|set| {
                !set.is_demoted() && set.plan().volume_for_file(file_id.file_index).is_some()
            })?;
        let index = set.plan().volume_for_file(file_id.file_index)?;
        if let Some(retained) = set.retained_volumes() {
            return retained
                .iter()
                .find(|volume| volume.volume_index == index)
                .cloned();
        }
        if set.is_finalized() {
            return None;
        }
        // Completion progress may use the NZB's encoded size. The provider's
        // committed decoded coverage is the only source-space length here.
        let len = set.virtual_volume_len(index, 0);
        set.virtual_volumes(&BTreeMap::from([(index, len)]))
            .into_iter()
            .find(|volume| volume.volume_index == index)
    }

    /// Declared carriers and authenticated late discovery can hold archive
    /// checks through either engine's attempt. PAR2-only jobs keep their policy.
    pub(in crate::pipeline) fn par3_direct_checks_available(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            crate::pipeline::direct_store::plan::spec_defers_to_par3(&state.spec)
                || self
                    .par3_runtime
                    .as_ref()
                    .is_some_and(|runtime| runtime.authenticated_set_count(job_id) != 0)
        })
    }

    pub(in crate::pipeline) fn par3_verification_pending(&self, job_id: JobId) -> bool {
        let runtime = self.par3_runtime.as_ref();
        let candidate = runtime.is_some_and(|runtime| runtime.contains_job(job_id))
            || self
                .jobs
                .get(&job_id)
                .is_some_and(|state| state.assembly.has_par3_candidates());
        candidate
            && (self.job_has_pending_download_pipeline_work(job_id)
                || !runtime.is_some_and(|runtime| runtime.verified(job_id)))
    }

    pub(in crate::pipeline) fn invalidate_par3_source_write(&mut self, file_id: NzbFileId) {
        let admitted = self
            .par3_runtime
            .as_ref()
            .is_some_and(|runtime| runtime.contains_job(file_id.job_id));
        if self.par3_inside_probes.is_empty() && !admitted {
            return;
        }
        // A negative framing probe is valid only until the next source write,
        // including jobs where no PAR3 runtime has been admitted yet.
        self.par3_inside_probes.remove(file_id);
        if !admitted {
            return;
        }
        if let Some(index) = self
            .direct_store
            .sets_for(file_id.job_id)
            .iter()
            .position(|set| {
                !set.is_demoted()
                    && !set.is_finalized()
                    && set.plan().volume_for_file(file_id.file_index).is_some()
            })
        {
            self.invalidate_par3_direct_set(file_id.job_id, index);
            return;
        }
        if let Some(coordinator) = self.par3_runtime.as_mut()
            && let Err(error) = coordinator
                .invalidate_source(file_id.job_id, SourceId(u64::from(file_id.file_index)))
        {
            self.fail_job(
                file_id.job_id,
                format!("PAR3 source invalidation failed: {error}"),
            );
        }
    }

    pub(in crate::pipeline) fn invalidate_par3_direct_set(&mut self, job_id: JobId, index: usize) {
        let Some(coordinator) = self
            .par3_runtime
            .as_mut()
            .filter(|runtime| runtime.contains_job(job_id))
        else {
            return;
        };
        let Some(set) = self.direct_store.set(job_id, index) else {
            return;
        };
        // A split member's plaintext partial backs several source volumes.
        // Retire every view of that archive before any destination is written.
        let result =
            set.plan().volumes.values().try_for_each(|file| {
                coordinator.invalidate_source(job_id, SourceId(u64::from(*file)))
            });
        if let Err(error) = result {
            self.fail_job(
                job_id,
                format!("PAR3 virtual source invalidation failed: {error}"),
            );
        }
    }

    pub(in crate::pipeline) fn invalidate_par3_bindings(&mut self, job_id: JobId) {
        self.par3_inside_probes.remove_job(job_id);
        if let Some(coordinator) = self.par3_runtime.as_mut()
            && let Err(error) = coordinator.invalidate_bindings(job_id)
        {
            self.fail_job(job_id, format!("PAR3 binding invalidation failed: {error}"));
        }
    }

    pub(in crate::pipeline) fn refresh_par3_sources(&mut self, job_id: JobId) -> EngineResult<()> {
        let Some(coordinator) = self.par3_runtime.as_ref() else {
            return Ok(());
        };
        if !coordinator.contains_job(job_id)
            || coordinator.is_installing(job_id)
            || self.direct_demotion_in_flight.contains_key(&job_id)
        {
            return Ok(());
        }
        let mut dirty = coordinator.dirty_sources(job_id);
        // An incomplete source may never emit a file-complete event. Metadata
        // can also precede its first byte. Publish quiescent sources without
        // waiting for unrelated files or parked retries to finish.
        if let Some(state) = self.jobs.get(&job_id) {
            for file in state.assembly.files() {
                let source = SourceId(u64::from(file.file_id().file_index));
                if !coordinator.knows_source(job_id, source)
                    && (file.is_complete()
                        || (0..file.total_segments()).any(|part| file.has_segment(part)))
                {
                    if dirty.len() >= MAX_CARRIERS {
                        return Err(budget::host_limit("PAR3 source count"));
                    }
                    dirty.push(source);
                }
            }
        }
        for source in dirty {
            let file_index = u32::try_from(source.0)
                .map_err(|_| EngineError::InvalidState("unknown PAR3 source identity"))?;
            let file_id = NzbFileId { job_id, file_index };
            if self
                .active_downloads_by_file
                .get(&file_id)
                .copied()
                .unwrap_or(0)
                != 0
                || self
                    .active_decodes_by_file
                    .get(&file_id)
                    .copied()
                    .unwrap_or(0)
                    != 0
                || self
                    .pending_decode
                    .iter()
                    .any(|work| work.segment_id.file_id == file_id)
                || self
                    .write_buffers
                    .get(&file_id)
                    .is_some_and(|buffer| buffer.buffered_len() != 0)
            {
                continue;
            }
            self.enqueue_par3_file(job_id, file_id)?;
        }
        Ok(())
    }

    pub(in crate::pipeline) async fn handle_par3_work_done(&mut self, done: work::WorkDone) {
        let Some(coordinator) = self.par3_runtime.as_mut() else {
            return;
        };
        let job_id = coordinator.settle(done);
        let repair = job_id.and_then(|id| coordinator.take_repair_result(id));
        let readback = job_id.and_then(|id| coordinator.take_readback(id));
        if let Err(error) = coordinator.dispatch() {
            tracing::error!(error = %error, "PAR3 worker dispatch failed");
        }
        if let Some(job_id) = job_id
            && self.jobs.contains_key(&job_id)
        {
            for (set, view) in coordinator.assessments(job_id) {
                tracing::debug!(job_id = job_id.0, set = ?set, status = ?view.status,
                    files = view.files.len(), cohorts = view.requirements.len(),
                    "PAR3 retained assessment settled");
                if tracing::enabled!(tracing::Level::TRACE) {
                    for file in view.files.iter().filter(|file| !file.complete) {
                        tracing::trace!(
                            job_id = job_id.0,
                            path = %file.path,
                            source = ?file.source,
                            unresolved_ranges = file.unresolved.len(),
                            unresolved_bytes = file.unresolved.iter().fold(0u64, |bytes, range| {
                                bytes.saturating_add(range.end.saturating_sub(range.start))
                            }),
                            "PAR3 source remains incomplete"
                        );
                    }
                }
            }
            tracing::debug!(
                job_id = job_id.0,
                sets = coordinator.authenticated_set_count(job_id),
                "PAR3 carrier worker settled"
            );
            if self.par3_direct_checks_available(job_id) {
                for index in 0..self.direct_store.sets_for(job_id).len() {
                    if let Some(set) = self.direct_store.set_mut(job_id, index)
                        && !set.is_demoted()
                        && !set.is_finalized()
                    {
                        set.router.note_par3_available(true);
                    }
                }
            }
            self.schedule_job_completion_check(job_id);
        }
        if let (Some(job_id), Some(result)) = (job_id, repair) {
            self.finish_par3_repair(job_id, result).await;
        }
        if let (Some(job_id), Some(result)) = (job_id, readback) {
            self.apply_par3_readback(job_id, result).await;
        }
        // This handback released a share of the PAR3 budget, so every job
        // parked on memory gets another completion check — not only the job
        // that handed back. Without this a parked job would wait forever for
        // a check that nothing else schedules.
        let parked = self
            .par3_runtime
            .as_ref()
            .map(|coordinator| coordinator.jobs_awaiting_memory())
            .unwrap_or_default();
        for job_id in parked {
            self.schedule_job_completion_check(job_id);
        }
    }
}

mod acquisition;
mod assessment;
mod bindings;
pub(in crate::pipeline) mod budget;
pub(in crate::pipeline) mod carriers;
pub(in crate::pipeline) mod cohorts;
mod completion;
mod coordination;
#[cfg(windows)]
mod disk_windows;
mod donors;
mod identity;
pub(in crate::pipeline) mod inside;
pub(in crate::pipeline) mod outcome;
mod outputs;
pub(in crate::pipeline) mod paths;
mod placement;
mod readback;
pub(in crate::pipeline) mod virtual_source;
pub(in crate::pipeline) mod work;

#[cfg(test)]
mod tests;
