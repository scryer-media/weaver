use super::*;
use crate::jobs::assembly::{FileAssembly, JobAssembly};
use crate::pipeline::SegmentTerminalState;
use crate::pipeline::direct_store::nested::seek::{Pull, Seek, SeekableSource};
use crate::pipeline::direct_store::router::HeaderProbe as DirectHeaderProbe;
use crate::pipeline::download::queue::{DownloadQueue, HoldOwner};

// The queue priority a pulled article is held at: behind the PAR2 index (0)
// and a named first RAR volume (1), ahead of the RAR unlock boost (3) and
// every later volume. Promoted PAR2 recovery carries the same number but is
// served in the completion-critical class, which leads this one whatever the
// number. A whole 7z file is queued at it too, so a pull reorders nothing
// within one. The queue keeps a held article out of the unlock plan's reach,
// so no boost overwrites it.
const PULLED_PRIORITY: u32 = 2;

// How far apart one NZB file's articles lie in the decoded bytes of its
// volume, as far as anything can tell before they land.
//
// The NZB states each article's *encoded* size, and yEnc runs 2-4% over the
// decoded bytes, so declared offsets drift further from the truth the deeper
// into a volume they go: at the end of a large volume they name an article
// several before the one that holds the byte. A seek mapped that way pulls
// articles that do not hold its range, the want never lands, and the bytes
// wait for the in-order flow after all.
#[derive(Clone, Copy)]
enum Spacing {
    // The volume's own decoded articles state its part size, which places
    // every article exactly: a poster cuts every article but the last to it.
    Part(u64),
    // A sibling volume's part size. The same poster cut every volume of the
    // set to one part size, nearly always; nearly is why it is an estimate.
    SiblingPart(u64),
    // Nothing of the set has decoded: the declared bounds, scaled by the
    // decoded-to-declared ratio of an article decoded elsewhere in the job.
    Scaled { decoded: u64, declared: u64 },
    // Nothing has decoded anywhere: the declared bounds as they stand.
    Declared,
}

// Where one NZB file's articles sit in the decoded bytes of its volume.
//
// An article that has landed has its decoded place, and [`Spacing`] places
// the rest. While that is only an estimate, each article is taken to reach
// one neighbour further either way, so the estimate's error pulls one
// article too many rather than the wrong one.
//
// Nothing reads a byte on the strength of where this puts it: a miss costs
// time, never correctness.
struct ArticleSpans<'a> {
    file: &'a FileAssembly,
    spacing: Spacing,
}

// The part size `file`'s decoded articles state, once any has landed. Every
// article but the last is one part long, and the last starts where the
// parts before it end.
fn part_size(file: &FileAssembly) -> Option<u64> {
    if file.placed_segment_count() == 0 {
        return None;
    }
    let last = file.total_segments().saturating_sub(1);
    (0..file.total_segments()).find_map(|segment| {
        let (offset, len) = file.placement_of(segment)?;
        let (segment, len) = (u64::from(segment), u64::from(len));
        if segment < u64::from(last) {
            (len != 0 && offset == segment * len).then_some(len)
        } else {
            (segment != 0 && offset != 0 && offset % segment == 0).then(|| offset / segment)
        }
    })
}

// Decoded bytes over declared bytes of an article decoded anywhere in the
// job. Never a final article: it is short, and its fixed encoding overhead
// would skew the ratio for every full-size one.
fn decoded_scale(assembly: &JobAssembly) -> Option<(u64, u64)> {
    assembly
        .files()
        .filter(|file| file.placed_segment_count() != 0)
        .find_map(|file| {
            (0..file.total_segments().saturating_sub(1)).find_map(|segment| {
                let (_, len) = file.placement_of(segment)?;
                let (start, end) = file.segment_bounds(segment)?;
                (len != 0 && end > start).then_some((u64::from(len), end - start))
            })
        })
}

impl<'a> ArticleSpans<'a> {
    // `set` maps each of the set's volumes to its NZB file index, which is
    // where a sibling's part size is looked for.
    fn of(assembly: &JobAssembly, file: &'a FileAssembly, set: &BTreeMap<u32, u32>) -> Self {
        let job_id = file.file_id().job_id;
        let spacing = part_size(file)
            .map(Spacing::Part)
            .or_else(|| {
                set.values()
                    .filter_map(|&file_index| assembly.file(NzbFileId { job_id, file_index }))
                    .find_map(part_size)
                    .map(Spacing::SiblingPart)
            })
            .or_else(|| {
                decoded_scale(assembly)
                    .map(|(decoded, declared)| Spacing::Scaled { decoded, declared })
            })
            .unwrap_or(Spacing::Declared);
        Self { file, spacing }
    }

    // Whether article `segment`'s bounds are known rather than estimated.
    fn exact(&self, segment: u32) -> bool {
        matches!(self.spacing, Spacing::Part(_)) || self.file.placement_of(segment).is_some()
    }

    // Article `segment`'s decoded bytes, exact once it has landed.
    fn bounds(&self, segment: u32) -> Option<(u64, u64)> {
        if let Some((offset, len)) = self.file.placement_of(segment) {
            return Some((offset, offset.saturating_add(u64::from(len))));
        }
        let last = segment + 1 >= self.file.total_segments();
        let (start, end) = match self.spacing {
            Spacing::Part(part) | Spacing::SiblingPart(part) => {
                let start = u64::from(segment).saturating_mul(part);
                (start, start.saturating_add(part))
            }
            Spacing::Scaled { decoded, declared } => {
                let (start, end) = self.file.segment_bounds(segment)?;
                let scaled = |offset: u64| {
                    (u128::from(offset) * u128::from(decoded) / u128::from(declared)) as u64
                };
                (scaled(start), scaled(end))
            }
            Spacing::Declared => self.file.segment_bounds(segment)?,
        };
        Some((start, if last { u64::MAX } else { end }))
    }

    // The bytes article `segment` may hold: its bounds, reaching one
    // neighbour further either way while they are only an estimate.
    fn reach(&self, segment: u32) -> (u64, u64) {
        let bounds = |segment| self.bounds(segment).unwrap_or((u64::MAX, u64::MAX));
        if self.exact(segment) {
            return bounds(segment);
        }
        let final_segment = self.file.total_segments().saturating_sub(1);
        (
            bounds(segment.saturating_sub(1)).0,
            bounds((segment + 1).min(final_segment)).1,
        )
    }

    // Every article that may hold a byte of `seek`, in order.
    fn covering(&self, seek: &Seek) -> Vec<u32> {
        let total = self.file.total_segments();
        // Articles tile the volume in order, so the first that reaches past
        // the seek's start is found by halving.
        let (mut low, mut high) = (0, total);
        while low < high {
            let middle = low + (high - low) / 2;
            if self.reach(middle).1 <= seek.range.start {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        (low..total)
            .take_while(|&segment| seek.overlaps(self.reach(segment)))
            .collect()
    }

    // The article of `covering` best placed to hold the seek's first byte:
    // the one whose bounds, not its reach, take it in.
    fn holding(&self, covering: &[u32], seek: &Seek) -> Option<u32> {
        let start = seek.range.start;
        covering.iter().copied().find(|&segment| {
            self.bounds(segment)
                .is_some_and(|(from, to)| from <= start && start < to)
        })
    }
}

// Usenet as a [`SeekableSource`]: a volume is an NZB file, and its articles
// are placed by [`ArticleSpans`]. Pulling holds the articles in the queue
// ahead of the in-order flow; one leased to a connection is out of the
// queue's reach, and takes the hold if it comes back.
struct ArticleSource<'a> {
    job_id: JobId,
    // Volume to NZB file index.
    files: &'a BTreeMap<u32, u32>,
    assembly: &'a JobAssembly,
    queue: &'a mut DownloadQueue,
    // Whose seeks these are, which is who may release their holds.
    owner: HoldOwner,
    // Articles every server has been asked for and that will never arrive.
    terminal: &'a HashMap<SegmentId, SegmentTerminalState>,
    // Every article the seeks asked so far hold.
    held: HashSet<SegmentId>,
}

impl SeekableSource for ArticleSource<'_> {
    fn pull(&mut self, seek: &Seek) -> Pull {
        let Some(&file_index) = self.files.get(&seek.volume) else {
            return Pull::Moved(0);
        };
        let file_id = NzbFileId {
            job_id: self.job_id,
            file_index,
        };
        let Some(file) = self.assembly.file(file_id) else {
            return Pull::Moved(0);
        };
        let (lost, wanted): (Vec<SegmentId>, Vec<SegmentId>) =
            ArticleSpans::of(self.assembly, file, self.files)
                .covering(seek)
                .into_iter()
                .map(|segment_number| SegmentId {
                    file_id,
                    segment_number,
                })
                .partition(|segment| self.terminal.contains_key(segment));
        // Bytes no server has are lost here, not refused: PAR2 may rebuild
        // them. Anything queued, in flight or landed still answers the seek.
        if wanted.is_empty() && !lost.is_empty() {
            return Pull::Lost;
        }
        self.held.extend(wanted.iter().copied());
        Pull::Moved(self.queue.hold(self.owner, wanted, PULLED_PRIORITY))
    }
}

pub(in crate::pipeline::download) struct DirectStoreAdmission {
    job_id: JobId,
    files: HashSet<u32>,
    // Room the set had when the handout began: its holds admission room
    // with every byte already on its way to it counted.
    room: u64,
    // The same room judged by the set's own limit alone.
    own_room: u64,
    // What the handout being built has leased of the set so far.
    leased: u64,
    available: u64,
    // `own_room` less what the handout has leased.
    own_available: u64,
    probes: Vec<SegmentId>,
    probe_lease: ProbeLease,
    // One queued article per byte range a nested archive wants of the set
    // and has nothing in flight for. These are pulled ahead of the
    // in-order flow, and are of no use unless they also get past room the
    // in-order flow has already filled: a child waiting on its map must not
    // stall behind the volumes it is meant to let the set skip.
    pulled: Vec<SegmentId>,
    // The article an idle set with a settled layout routes next: the
    // lowest queued article of its earliest unfinished volume. Judged by
    // the set's own room only, so what other sets hold can slow this set to
    // one article at a time but never stop it.
    frontier: Option<SegmentId>,
}

// What leasing one of the set's articles does to its header probes.
#[derive(Clone, Copy)]
enum ProbeLease {
    // The earliest-volume probe is only asked of an idle set; anything of
    // the set out on a lane closes it.
    CloseAll,
    // A container probes each end until that end's volume has an article
    // in flight; leasing one closes only the probe of that volume.
    CloseFile,
}

impl DirectStoreAdmission {
    pub(in crate::pipeline::download) fn allows(&self, work: &DownloadWork) -> bool {
        !self.files.contains(&work.segment_id.file_id.file_index)
            || work.byte_estimate as u64 <= self.available
            || self.probes.contains(&work.segment_id)
            || self.pulled.contains(&work.segment_id)
            || (self.frontier == Some(work.segment_id)
                && work.byte_estimate as u64 <= self.own_available)
    }

    // Whether this set could admit *any* queued article of `file_index`,
    // given the smallest `byte_estimate` among them.
    //
    // [`Self::allows`] admits an article of one of the set's files when its
    // estimate fits the room left, and past the room when it is one of the
    // set's probes, one of its pulled articles, or its frontier. When even
    // the smallest article does not fit, only those can get through, and any
    // of them in this file keeps the answer `true`. They are matched by file
    // index alone, which can only make the answer more permissive than
    // [`Self::allows`].
    pub(in crate::pipeline::download) fn may_admit_from_file(
        &self,
        file_index: u32,
        smallest: u32,
    ) -> bool {
        !self.files.contains(&file_index)
            || smallest as u64 <= self.available
            || self
                .probes
                .iter()
                .any(|probe| probe.file_id.file_index == file_index)
            || self
                .pulled
                .iter()
                .any(|pulled| pulled.file_id.file_index == file_index)
            || self
                .frontier
                .is_some_and(|frontier| frontier.file_id.file_index == file_index)
    }

    // Charge an article the handout being built has just taken, so the
    // next one is admitted against what the set will hold with it.
    pub(in crate::pipeline::download) fn note_leased(&mut self, work: &DownloadWork) {
        let file = work.segment_id.file_id;
        if file.job_id != self.job_id || !self.files.contains(&file.file_index) {
            return;
        }
        // The holds admission room falls byte for byte with what is
        // committed to the set, down to zero, so charging the lease against
        // the room taken at the start equals asking the router again with
        // the lease counted as incoming.
        self.leased = self.leased.saturating_add(work.byte_estimate as u64);
        self.available = self.room.saturating_sub(self.leased);
        self.own_available = self.own_room.saturating_sub(self.leased);
        // Anything of the set out on a lane makes it busy, and a busy set
        // waits for that article rather than reaching past the shared room
        // for another.
        self.frontier = None;
        match self.probe_lease {
            ProbeLease::CloseAll => self.probes.clear(),
            ProbeLease::CloseFile => self
                .probes
                .retain(|probe| probe.file_id.file_index != file.file_index),
        }
        // A want has its one article on the way once that article leases;
        // the others' articles are still owed theirs.
        self.pulled.retain(|pulled| *pulled != work.segment_id);
    }
}

impl Pipeline {
    // Holds the articles `seeks` of a set's volumes need ahead of the
    // in-order flow for `owner`, and returns how many moved along with every
    // article the seeks hold. `files` maps each volume to its NZB file index.
    //
    // A seek whose bytes no server has holds nothing: the bytes it wanted
    // are repair's to rebuild or the in-order flow's to fail.
    fn hold_seeks(
        &mut self,
        owner: HoldOwner,
        job_id: JobId,
        files: &BTreeMap<u32, u32>,
        seeks: &[Seek],
    ) -> (usize, HashSet<SegmentId>) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return (0, HashSet::new());
        };
        let mut source = ArticleSource {
            job_id,
            files,
            assembly: &state.assembly,
            queue: &mut state.download_queue,
            owner,
            terminal: &self.segment_terminal_states,
            held: HashSet::new(),
        };
        let moved = seeks
            .iter()
            .map(|seek| match source.pull(seek) {
                Pull::Moved(moved) => moved,
                Pull::Lost => 0,
            })
            .sum();
        (moved, source.held)
    }

    // [`Self::hold_seeks`] for a caller whose articles stay held while the
    // job runs, and returns how many moved.
    pub(in crate::pipeline) fn pull_seeks(
        &mut self,
        job_id: JobId,
        files: &BTreeMap<u32, u32>,
        seeks: &[Seek],
    ) -> usize {
        self.hold_seeks(HoldOwner::Job, job_id, files, seeks).0
    }

    // Holds what one set's nested archives want of its volumes, and lets go
    // of what they no longer want. A nested child's front and its map ride
    // the same pull the root's own probes get, through every stored archive
    // between it and the wire.
    pub(in crate::pipeline) fn pull_nested_seeks(&mut self, job_id: JobId, set_name: &str) {
        let Some(set) = self
            .direct_store
            .sets_mut(job_id)
            .iter_mut()
            .find(|set| set.plan().set_name == set_name)
        else {
            return;
        };
        let seeks = set.nested_seeks();
        let files = set.plan().volumes.clone();
        let (moved, held) = self.hold_seeks(HoldOwner::NestedWants, job_id, &files, &seeks);
        // A want that moved on, or a child that stopped asking, leaves its
        // articles held for nothing; they go back to their place unless
        // another holder keeps them. Another set's nested wants are its own.
        let set_files: HashSet<u32> = files.values().copied().collect();
        let released = self.jobs.get_mut(&job_id).map_or(0, |state| {
            state
                .download_queue
                .release_held(HoldOwner::NestedWants, |segment| {
                    held.contains(&segment) || !set_files.contains(&segment.file_id.file_index)
                })
        });
        if moved > 0 || released > 0 {
            debug!(
                job_id = job_id.0,
                set_name, moved, released, "moved nested archive bytes ahead of the in-order flow"
            );
        }
    }

    // The article each of a set's nested wants admits next, while none of
    // the articles that may hold its bytes is already in flight. That bounds
    // a want to one article on the way, and lets the next one go only once
    // it lands and the want shrinks.
    fn nested_pulled_articles(
        &self,
        state: &JobState,
        job_id: JobId,
        set: &crate::pipeline::direct_store::set::DirectSet,
        owns: &dyn Fn(NzbFileId) -> bool,
    ) -> Vec<SegmentId> {
        if !set.has_nested_checkpoint_authority() || set.is_delegated() {
            return Vec::new();
        }
        let Some(graph) = set.nested_graph() else {
            return Vec::new();
        };
        let seeks = graph.seeks(&set.router);
        if seeks.is_empty() {
            return Vec::new();
        }
        let inflight: HashSet<SegmentId> = self
            .rate_limit_reservations
            .keys()
            .chain(self.active_decode_bytes.keys())
            .copied()
            .chain(self.pending_decode.iter().map(|work| work.segment_id))
            .filter(|segment| owns(segment.file_id))
            .collect();
        let mut pulled = Vec::new();
        for seek in &seeks {
            let Some(&file_index) = set.plan().volumes.get(&seek.volume) else {
                continue;
            };
            let file_id = NzbFileId { job_id, file_index };
            let Some(file) = state.assembly.file(file_id) else {
                continue;
            };
            let spans = ArticleSpans::of(&state.assembly, file, &set.plan().volumes);
            let covering = spans.covering(seek);
            let on_its_way = covering.iter().any(|&segment_number| {
                inflight.contains(&SegmentId {
                    file_id,
                    segment_number,
                })
            });
            if on_its_way || state.download_queue.queued_count_for_file(file_id) == 0 {
                continue;
            }
            // The article that holds the first byte goes first: a neighbour
            // taken in only because the estimate may be off would make it
            // wait. The pull on every commit has held it by now, which is
            // what makes asking after it cheap.
            let next = spans
                .holding(&covering, seek)
                .map(|segment_number| SegmentId {
                    file_id,
                    segment_number,
                })
                .filter(|segment| state.download_queue.holds_queued(*segment))
                .or_else(|| {
                    state
                        .download_queue
                        .peek_first_matching(|work| {
                            work.segment_id.file_id == file_id
                                && covering.contains(&work.segment_id.segment_number)
                        })
                        .map(|work| work.segment_id)
                });
            if let Some(segment) = next
                && !pulled.contains(&segment)
            {
                pulled.push(segment);
            }
        }
        pulled
    }

    // Every article the job's sets admit past their room for nested wants.
    #[cfg(test)]
    pub(in crate::pipeline) fn direct_store_pulled_for_test(
        &self,
        job_id: JobId,
    ) -> Vec<SegmentId> {
        self.direct_store_admission(job_id)
            .into_iter()
            .flat_map(|admission| admission.pulled)
            .collect()
    }

    // Per-set disk retention is separate from shared RAM pressure. Include
    // arrivals already committed to the wire/decode pipeline before allowing
    // another article to start; work leased by the handout being built is
    // charged as it is taken, through [`DirectStoreAdmission::note_leased`].
    pub(super) fn direct_store_admission(&self, job_id: JobId) -> Vec<DirectStoreAdmission> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        self.direct_store
            .sets_for(job_id)
            .iter()
            .filter(|set| !set.is_demoted() && !set.is_finalized())
            .map(|set| {
                let files: HashSet<u32> = set.plan().files.keys().copied().collect();
                let owns =
                    |file: NzbFileId| file.job_id == job_id && files.contains(&file.file_index);
                let wire = self
                    .rate_limit_reservations
                    .iter()
                    .filter(|(segment, _)| owns(segment.file_id))
                    .map(|(_, bytes)| *bytes);
                let decoding = self
                    .active_decode_bytes
                    .iter()
                    .filter(|(segment, _)| owns(segment.file_id))
                    .map(|(_, bytes)| *bytes);
                let pending = self
                    .pending_decode
                    .iter()
                    .filter(|work| owns(work.segment_id.file_id))
                    .map(|work| work.raw.len() as u64);
                // Released lane results are tracked per job, so charge them
                // conservatively to each set until their actor event arrives.
                let released = self
                    .pending_released_download_result_bytes_by_job
                    .get(&job_id)
                    .copied()
                    .unwrap_or(0);
                let incoming = wire
                    .chain(decoding)
                    .chain(pending)
                    .fold(released, u64::saturating_add);
                let busy = incoming != 0
                    || self
                        .active_downloads_by_file
                        .iter()
                        .any(|(file, count)| owns(*file) && *count != 0)
                    || self
                        .active_decodes_by_file
                        .iter()
                        .any(|(file, count)| owns(*file) && *count != 0);
                let rooms = set.router.holds_admission_rooms(incoming);
                let room = rooms.shared;

                // Permit queued articles past the limit to resolve the layout.
                // Ordinals need not match yEnc offsets: serialized progress
                // also handles rotated NZBs and headers spanning several
                // articles. If an earlier volume has a retry pending, wait for
                // it instead of spending probe room on later volumes.
                // Terminally missing articles have no retry; keep progressing
                // toward the existing repair/demotion path.
                let unresolved = |file: &u32| {
                    state.download_queue.queued_count_for_file(NzbFileId {
                        job_id,
                        file_index: *file,
                    }) != 0
                        || self.pending_retries_by_segment.iter().any(|(id, count)| {
                            id.file_id.job_id == job_id
                                && id.file_id.file_index == *file
                                && *count != 0
                        })
                };
                // Which end of which volume resolves this set's layout. For RAR
                // it is always the front of the earliest volume whose headers
                // are unread; a container is read at both ends, because its
                // volumes state their lengths one article each and its map sits
                // at the very end of the last one.
                let probe_lease = match set.header_probe() {
                    DirectHeaderProbe::Earliest => ProbeLease::CloseAll,
                    _ => ProbeLease::CloseFile,
                };
                let probes = match set.header_probe() {
                    DirectHeaderProbe::Settled => Vec::new(),
                    DirectHeaderProbe::Earliest if busy => Vec::new(),
                    DirectHeaderProbe::Earliest => set
                        .plan()
                        .volumes
                        .iter()
                        .filter(|(_, file)| unresolved(file))
                        .min_by_key(|(volume, _)| {
                            (!set.router.volume_needs_header(**volume), **volume)
                        })
                        .and_then(|(_, file)| {
                            state.download_queue.peek_first_matching(|work| {
                                work.segment_id.file_id.file_index == *file
                            })
                        })
                        .map(|work| vec![work.segment_id])
                        .unwrap_or_default(),
                    // Released lane results are only attributable to the job,
                    // so while any are outstanding nothing here can tell which
                    // volume they answer; wait for them rather than probe a
                    // volume whose article is already on its way back.
                    DirectHeaderProbe::Container { .. } if released != 0 => Vec::new(),
                    // Two articles, at the two ends of the set, and independent
                    // of one another: volume zero's front carries the part size
                    // and the start header, the tail carries the map. Asking
                    // for both together is what keeps a set from staging every
                    // volume ahead of the last one before it can read its map.
                    // The bound is one article in flight per end.
                    DirectHeaderProbe::Container { front, tail, .. } => {
                        let mut inflight: HashSet<u32> = HashSet::new();
                        let mut note = |file: NzbFileId| {
                            if owns(file) {
                                inflight.insert(file.file_index);
                            }
                        };
                        for segment in self.rate_limit_reservations.keys() {
                            note(segment.file_id);
                        }
                        for segment in self.active_decode_bytes.keys() {
                            note(segment.file_id);
                        }
                        for work in &self.pending_decode {
                            note(work.segment_id.file_id);
                        }
                        for (file, count) in &self.active_downloads_by_file {
                            if *count != 0 {
                                note(*file);
                            }
                        }
                        for (file, count) in &self.active_decodes_by_file {
                            if *count != 0 {
                                note(*file);
                            }
                        }
                        let probeable = |volume: &u32| {
                            set.plan()
                                .volumes
                                .get(volume)
                                .filter(|file| unresolved(file) && !inflight.contains(file))
                                .copied()
                        };
                        let mut probes = Vec::with_capacity(2);
                        if let Some(file) = front.as_ref().and_then(probeable)
                            && let Some(work) = state.download_queue.peek_first_matching(|work| {
                                work.segment_id.file_id.file_index == file
                            })
                        {
                            probes.push(work.segment_id);
                        }
                        if let Some(file) = tail.as_ref().and_then(probeable)
                            && let Some(work) = state.download_queue.peek_last_matching(|work| {
                                work.segment_id.file_id.file_index == file
                            })
                            && !probes.contains(&work.segment_id)
                        {
                            probes.push(work.segment_id);
                        }
                        probes
                    }
                };
                // What the set's nested archives want of its volumes, an
                // article per want. Released lane results cannot be told
                // apart by volume, so like the container probes this waits
                // for them rather than ask for an article already on its way.
                let pulled = if released == 0 {
                    self.nested_pulled_articles(state, job_id, set, &owns)
                } else {
                    Vec::new()
                };
                // The shared room bounds how far holds may grow, not whether
                // a set moves at all. Other sets' holds — a paused job's, one
                // waiting on a retried gap — can leave less shared room than
                // one article, and a settled set has no probe to fall back on,
                // so an idle one is always given the article it routes next.
                // Idle is what keeps this from growing holds past the shared
                // total: one article at a time, each landing before the next
                // is asked for. With nothing of the set in flight, the lowest
                // article still queued in the earliest unfinished volume is
                // the earliest byte the set lacks; while that volume has a
                // retry pending, the retry is, and the set waits for it.
                let frontier = (matches!(set.header_probe(), DirectHeaderProbe::Settled)
                    && !busy
                    && rooms.shared < rooms.own)
                    .then(|| {
                        set.plan()
                            .volumes
                            .values()
                            .copied()
                            .find(|file| unresolved(file))
                    })
                    .flatten()
                    .filter(|file| {
                        !self.pending_retries_by_segment.iter().any(|(id, count)| {
                            id.file_id.job_id == job_id
                                && id.file_id.file_index == *file
                                && *count != 0
                        })
                    })
                    .and_then(|file| {
                        state
                            .download_queue
                            .peek_lowest_matching(|work| work.segment_id.file_id.file_index == file)
                            .map(|work| work.segment_id)
                    });
                DirectStoreAdmission {
                    job_id,
                    files,
                    room,
                    own_room: rooms.own,
                    leased: 0,
                    available: room,
                    own_available: rooms.own,
                    probes,
                    probe_lease,
                    pulled,
                    frontier,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::assembly::FileAssembly;
    use crate::jobs::ids::MessageId;
    use weaver_model::files::FileRole;

    const JOB: JobId = JobId(7);

    fn segment(file_index: u32, segment_number: u32) -> SegmentId {
        SegmentId {
            file_id: NzbFileId {
                job_id: JOB,
                file_index,
            },
            segment_number,
        }
    }

    fn work(file_index: u32, segment_number: u32, priority: u32) -> DownloadWork {
        DownloadWork {
            segment_id: segment(file_index, segment_number),
            message_id: MessageId::new(&format!("{file_index}-{segment_number}@example.com")),
            groups: Arc::from(vec!["alt.binaries.test".to_string()]),
            priority,
            byte_estimate: 100,
            retry_count: 0,
            is_recovery: false,
            completion_critical: false,
            exclude_servers: vec![],
            avoid_server: None,
        }
    }

    fn volume(file_index: u32, declared: Vec<u32>) -> FileAssembly {
        FileAssembly::new(
            NzbFileId {
                job_id: JOB,
                file_index,
            },
            format!("set.part{file_index}.rar"),
            FileRole::RarVolume {
                volume_number: file_index - 4,
            },
            declared,
        )
    }

    // Two volumes of one set, files 4 and 5, of `segments` articles each.
    fn assembly_of(segments: usize) -> JobAssembly {
        let mut assembly = JobAssembly::new(JOB);
        for file_index in [4, 5] {
            assembly.add_file(volume(file_index, vec![100; segments]));
        }
        assembly
    }

    fn place(assembly: &mut JobAssembly, file_index: u32, segment: u32, offset: u64, len: u32) {
        assembly
            .file_mut(NzbFileId {
                job_id: JOB,
                file_index,
            })
            .expect("the file is in the job")
            .record_placement(segment, offset, len);
    }

    fn queue_of(segments: u32, priority: impl Fn(u32, u32) -> u32) -> DownloadQueue {
        let mut queue = DownloadQueue::new();
        for file_index in [4, 5] {
            for segment in 0..segments {
                queue.push(work(file_index, segment, priority(file_index, segment)));
            }
        }
        queue
    }

    fn priorities_of(queue: &mut DownloadQueue) -> Vec<(u32, u32, u32)> {
        let mut priorities = Vec::new();
        while let Some(work) = queue.pop_first_matching(|_| true) {
            let id = work.segment_id;
            priorities.push((id.file_id.file_index, id.segment_number, work.priority));
        }
        priorities.sort_unstable();
        priorities
    }

    fn pulled_articles(priorities: &[(u32, u32, u32)]) -> Vec<(u32, u32)> {
        priorities
            .iter()
            .filter(|(_, _, priority)| *priority == PULLED_PRIORITY)
            .map(|(file, segment, _)| (*file, *segment))
            .collect()
    }

    const SET: [(u32, u32); 2] = [(0, 4), (1, 5)];

    fn article_source<'a>(
        files: &'a BTreeMap<u32, u32>,
        assembly: &'a JobAssembly,
        queue: &'a mut DownloadQueue,
        terminal: &'a HashMap<SegmentId, SegmentTerminalState>,
    ) -> ArticleSource<'a> {
        ArticleSource {
            job_id: JOB,
            files,
            assembly,
            queue,
            owner: HoldOwner::NestedWants,
            terminal,
            held: HashSet::new(),
        }
    }

    #[test]
    fn a_seek_pulls_exactly_the_articles_its_bytes_overlap() {
        let mut assembly = assembly_of(5);
        // One decoded article states the part size, which places every
        // other article of the volume exactly.
        place(&mut assembly, 4, 0, 0, 100);
        // One already more urgent than a pull, which must stay so.
        let mut queue = queue_of(
            5,
            |file, segment| {
                if (file, segment) == (4, 2) { 1 } else { 10 }
            },
        );
        let files = BTreeMap::from(SET);
        let terminal = HashMap::new();
        let mut source = article_source(&files, &assembly, &mut queue, &terminal);
        let seek = Seek {
            volume: 0,
            range: 150..301,
        };
        assert_eq!(source.pull(&seek), Pull::Moved(2));
        // Asking again finds every article already held.
        assert_eq!(source.pull(&seek), Pull::Moved(0));
        assert_eq!(
            source.pull(&Seek {
                volume: 2,
                range: 0..u64::MAX,
            }),
            Pull::Moved(0)
        );
        assert_eq!(
            source.held,
            HashSet::from([segment(4, 1), segment(4, 2), segment(4, 3)])
        );
        let priorities = priorities_of(&mut queue);
        assert_eq!(pulled_articles(&priorities), vec![(4, 1), (4, 3)]);
        assert!(priorities.contains(&(4, 2, 1)));
    }

    // An article out on a lane when its seek was pulled is out of the
    // queue's reach. If the lane gives it back, it lands at the held
    // priority, though the want that held it never changed.
    #[test]
    fn an_article_given_back_by_a_lane_lands_where_its_hold_puts_it() {
        let mut assembly = assembly_of(5);
        place(&mut assembly, 4, 0, 0, 100);
        let mut queue = queue_of(5, |_, _| 10);
        let leased = queue
            .pop_first_matching(|work| work.segment_id == segment(4, 3))
            .unwrap();
        let files = BTreeMap::from(SET);
        let terminal = HashMap::new();
        let mut source = article_source(&files, &assembly, &mut queue, &terminal);
        assert_eq!(source.pull(&Seek::byte(0, 350)), Pull::Moved(0));
        queue.push(leased);
        assert_eq!(pulled_articles(&priorities_of(&mut queue)), vec![(4, 3)]);
    }

    // A want that moves on lets go of what it held, and each article goes
    // back to where the queue would put it now.
    #[test]
    fn a_released_article_goes_back_to_its_place() {
        let mut assembly = assembly_of(5);
        place(&mut assembly, 4, 0, 0, 100);
        let mut queue = queue_of(5, |_, _| 10);
        let files = BTreeMap::from(SET);
        let terminal = HashMap::new();
        let mut source = article_source(&files, &assembly, &mut queue, &terminal);
        assert_eq!(source.pull(&Seek::byte(0, 250)), Pull::Moved(1));
        assert_eq!(source.pull(&Seek::byte(0, 350)), Pull::Moved(1));
        assert!(queue.holds_queued(segment(4, 2)));
        assert_eq!(
            queue.release_held(HoldOwner::NestedWants, |held| held == segment(4, 3)),
            1
        );
        assert!(!queue.holds_queued(segment(4, 2)));
        assert_eq!(pulled_articles(&priorities_of(&mut queue)), vec![(4, 3)]);
    }

    // yEnc states each article's encoded size: 100 declared bytes here carry
    // 96 decoded ones, so by the twentieth article the declared offsets name
    // the article before the one holding the byte.
    #[test]
    fn a_seek_maps_through_decoded_bytes_not_declared_ones() {
        let files = BTreeMap::from(SET);
        let terminal = HashMap::new();
        // Byte 1550 is decoded article 16 (1536..1632) and declared
        // article 15 (1500..1600).
        let seek = Seek {
            volume: 0,
            range: 1550..1551,
        };

        // With one article of the volume decoded, its part size places the
        // byte exactly.
        let mut assembly = assembly_of(20);
        place(&mut assembly, 4, 0, 0, 96);
        let mut queue = queue_of(20, |_, _| 10);
        let mut source = article_source(&files, &assembly, &mut queue, &terminal);
        assert_eq!(source.pull(&seek), Pull::Moved(1));
        assert_eq!(pulled_articles(&priorities_of(&mut queue)), vec![(4, 16)]);

        // So does the final article alone: it starts where the parts before
        // it end.
        let mut assembly = assembly_of(20);
        place(&mut assembly, 4, 19, 19 * 96, 40);
        let mut queue = queue_of(20, |_, _| 10);
        let mut source = article_source(&files, &assembly, &mut queue, &terminal);
        assert_eq!(source.pull(&seek), Pull::Moved(1));
        assert_eq!(pulled_articles(&priorities_of(&mut queue)), vec![(4, 16)]);

        // With only a sibling volume's article decoded, its part size is
        // taken for this one's too, and the estimate reaches one neighbour
        // either way, which takes in the article holding the byte.
        let mut assembly = assembly_of(20);
        place(&mut assembly, 5, 0, 0, 96);
        let mut queue = queue_of(20, |_, _| 10);
        let mut source = article_source(&files, &assembly, &mut queue, &terminal);
        assert_eq!(source.pull(&seek), Pull::Moved(3));
        assert_eq!(
            pulled_articles(&priorities_of(&mut queue)),
            vec![(4, 15), (4, 16), (4, 17)]
        );
    }

    // A poster's articles do not all encode to one size: escapes vary from
    // article to article, and the last is short, so its fixed overhead is a
    // larger share of it. Neither may throw the estimate past one neighbour.
    #[test]
    fn a_seek_maps_through_a_ratio_that_varies_by_article() {
        let files = BTreeMap::from(SET);
        let terminal = HashMap::new();
        // At 96 decoded bytes an article, byte 1550 is article 16's.
        let seek = Seek::byte(0, 1550);
        // Declared sizes swing 96..104 around 100 for 96 decoded bytes each.
        let declared: Vec<u32> = (0..20).map(|segment| 96 + (segment * 7) % 9).collect();

        // A sibling's part size ignores the declared sizes altogether.
        let mut assembly = JobAssembly::new(JOB);
        assembly.add_file(volume(4, declared.clone()));
        assembly.add_file(volume(5, declared.clone()));
        place(&mut assembly, 5, 0, 0, 96);
        let mut queue = queue_of(20, |_, _| 10);
        let mut source = article_source(&files, &assembly, &mut queue, &terminal);
        assert_eq!(source.pull(&seek), Pull::Moved(3));
        assert_eq!(
            pulled_articles(&priorities_of(&mut queue)),
            vec![(4, 15), (4, 16), (4, 17)]
        );

        // Outside the set, only other files' articles have decoded: the
        // short final one of one, at 60 of 100, and a full one of another,
        // at 96 of 100. The scale comes from the full one, whichever of the
        // two is found first.
        let mut assembly = JobAssembly::new(JOB);
        assembly.add_file(volume(4, declared.clone()));
        assembly.add_file(volume(5, declared));
        assembly.add_file(volume(6, vec![100, 100]));
        assembly.add_file(volume(7, vec![100, 100]));
        place(&mut assembly, 6, 1, 96, 60);
        place(&mut assembly, 7, 0, 0, 96);
        let mut queue = queue_of(20, |_, _| 10);
        let mut source = article_source(&files, &assembly, &mut queue, &terminal);
        assert_eq!(source.pull(&seek), Pull::Moved(3));
        assert_eq!(
            pulled_articles(&priorities_of(&mut queue)),
            vec![(4, 15), (4, 16), (4, 17)]
        );
    }

    #[test]
    fn the_article_whose_bounds_hold_the_first_byte_goes_first() {
        let mut assembly = assembly_of(20);
        place(&mut assembly, 5, 0, 0, 96);
        let files = BTreeMap::from(SET);
        let file = assembly
            .file(NzbFileId {
                job_id: JOB,
                file_index: 4,
            })
            .unwrap();
        let spans = ArticleSpans::of(&assembly, file, &files);
        let seek = Seek::byte(0, 1550);
        let covering = spans.covering(&seek);
        assert_eq!(covering, vec![15, 16, 17]);
        assert_eq!(spans.holding(&covering, &seek), Some(16));
    }

    #[test]
    fn bytes_every_server_lost_are_reported_lost_not_refused() {
        let mut assembly = assembly_of(5);
        place(&mut assembly, 4, 0, 0, 100);
        // The tail article failed everywhere and left the queue.
        let mut queue = DownloadQueue::new();
        for segment in 0..4 {
            queue.push(work(4, segment, 10));
        }
        let files = BTreeMap::from([(0, 4)]);
        let terminal = HashMap::from([(segment(4, 4), SegmentTerminalState::RetriesExhausted)]);
        let mut source = article_source(&files, &assembly, &mut queue, &terminal);
        assert_eq!(source.pull(&Seek::byte(0, 450)), Pull::Lost);
        // A range that also covers a queued article is only late, not gone,
        // and the queued one is what the seek holds.
        assert_eq!(
            source.pull(&Seek {
                volume: 0,
                range: 350..450,
            }),
            Pull::Moved(1)
        );
        assert_eq!(source.held, HashSet::from([segment(4, 3)]));
    }

    // Other sets' holds can leave this set less shared room than one
    // article. A nested child's want must still get its article, or the
    // child waits on the very volumes its map lets the set skip.
    #[test]
    fn a_pulled_article_is_admitted_past_the_shared_room() {
        let pulled = work(4, 4, PULLED_PRIORITY);
        let ordinary = work(4, 1, 10);
        let mut admission = DirectStoreAdmission {
            job_id: JOB,
            files: HashSet::from([4]),
            room: 50,
            own_room: 50,
            leased: 0,
            available: 50,
            own_available: 50,
            probes: Vec::new(),
            probe_lease: ProbeLease::CloseFile,
            pulled: vec![pulled.segment_id],
            frontier: None,
        };
        assert!(admission.allows(&pulled));
        assert!(!admission.allows(&ordinary));
        assert!(admission.may_admit_from_file(4, 100));
        // One article in flight per want: once it leases, the want has its
        // article and admits no more past the room.
        admission.note_leased(&pulled);
        assert!(!admission.allows(&pulled));
        assert!(!admission.may_admit_from_file(4, 100));
    }
}
