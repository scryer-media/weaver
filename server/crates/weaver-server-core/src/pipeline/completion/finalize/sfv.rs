//! SFV verification for jobs with no PAR2 set.
//!
//! A job carrying recovery data is adjudicated by PAR2 and never reaches this
//! module. A job without it completes on the strength of whatever the wire
//! happened to prove, which for uuencode — no per-article checksum exists in
//! that encoding at all — is nothing. Vintage posts, the ones uuencode support
//! unlocks, habitually ship a `.sfv`: one line per posted file, naming it and
//! its CRC32. That listing is the only independent statement of what the
//! payload should be, so where nothing else can rule, it rules.
//!
//! Two properties bound what this can claim. The listing is the *poster's*
//! declaration, not recovery data, so a mismatch is terminal — there is nothing
//! to repair from. And it covers only what it names: a payload file absent from
//! every `.sfv` is left unverified rather than treated as suspect.

use super::*;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use tokio::io::AsyncReadExt;

/// Ceiling on the decoded size of a file this module will read as a checksum
/// listing. A real `.sfv` is a few kilobytes; anything past this is a
/// misclassified payload file and reading it would cost more than the
/// verification is worth.
const MAX_SFV_BYTES: u64 = 1024 * 1024;
/// Per-job ceiling for obfuscated files that can be sampled as possible SFV
/// listings. Named `.sfv` files bypass this discovery arm.
const MAX_OBFUSCATED_SFV_PROBES: usize = 1024;
/// A release can split its checksums across several listings, but more than a
/// handful is not useful evidence and makes completion work unbounded.
const MAX_SFV_LISTINGS_PER_JOB: usize = 16;
/// Aggregate listing bytes accepted for one job. This remains independent of
/// the per-listing ceiling so future changes cannot accidentally unbound the
/// total work.
const MAX_SFV_LISTING_BYTES_PER_JOB: u64 = 16 * 1024 * 1024;

/// SABnzbd's bounded probe length for identifying an obfuscated SFV file.
const SFV_PROBE_BYTES: u64 = 10_000;

/// SABnzbd stops inspecting after this many valid entries: that is enough
/// evidence that the sampled text is an SFV listing, without making malformed
/// content later in a long sample relevant to the decision.
const SFV_PROBE_CONFIDENCE_ENTRIES: usize = 10;

/// Read buffer for the disk arm. Matches the completed-file re-read in the
/// decode worker, which streams the same shape of file for the same reason.
const SFV_READ_BUFFER_BYTES: usize = 256 * 1024;

/// Result of reading an SFV listing with its hard content ceiling.
enum SfvListingRead {
    Contents(Vec<u8>),
    Oversized,
}

#[derive(Debug, Default)]
struct SfvScanBudget {
    probes: usize,
    listings: usize,
    listing_bytes: u64,
    probes_exhausted: bool,
    listings_exhausted: bool,
    listing_bytes_exhausted: bool,
}

impl SfvScanBudget {
    fn take_probe(&mut self) -> bool {
        if self.probes == MAX_OBFUSCATED_SFV_PROBES {
            self.probes_exhausted = true;
            return false;
        }
        self.probes += 1;
        true
    }

    fn take_listing(&mut self) -> bool {
        if self.listings == MAX_SFV_LISTINGS_PER_JOB {
            self.listings_exhausted = true;
            return false;
        }
        self.listings += 1;
        true
    }

    fn listing_read_limit(&self) -> Option<u64> {
        let remaining = MAX_SFV_LISTING_BYTES_PER_JOB.saturating_sub(self.listing_bytes);
        (remaining != 0).then_some(remaining.min(MAX_SFV_BYTES))
    }

    fn record_listing_bytes(&mut self, bytes: u64) -> bool {
        let remaining = MAX_SFV_LISTING_BYTES_PER_JOB.saturating_sub(self.listing_bytes);
        if bytes > remaining {
            self.listing_bytes_exhausted = true;
            return false;
        }
        self.listing_bytes = self.listing_bytes.saturating_add(bytes);
        true
    }

    fn exhausted(&self) -> bool {
        self.probes_exhausted || self.listings_exhausted || self.listing_bytes_exhausted
    }
}

/// Read just enough of an arbitrary completed file to conservatively identify
/// an obfuscated SFV listing. The `take` limit, rather than metadata, is the
/// authoritative bound.
async fn read_sfv_probe(path: &Path) -> std::io::Result<Vec<u8>> {
    let file = tokio::fs::File::open(path).await?;
    let mut reader = file.take(SFV_PROBE_BYTES);
    let mut contents = Vec::new();
    reader.read_to_end(&mut contents).await?;
    Ok(contents)
}

/// Read a selected listing without ever consuming more than one byte past the
/// accepted maximum. That extra byte distinguishes an exact-limit listing from
/// an oversized candidate without trusting the assembly's byte accounting.
async fn read_sfv_listing(path: &Path, max_bytes: u64) -> std::io::Result<SfvListingRead> {
    let file = tokio::fs::File::open(path).await?;
    let mut reader = file.take(max_bytes.saturating_add(1));
    let mut contents = Vec::new();
    reader.read_to_end(&mut contents).await?;
    if contents.len() as u64 > max_bytes {
        Ok(SfvListingRead::Oversized)
    } else {
        Ok(SfvListingRead::Contents(contents))
    }
}

/// The byte allowlist SABnzbd applies before trying to interpret a probe as
/// text: selected ASCII controls plus printable and extended text bytes, but
/// never DEL or binary controls.
fn is_sab_text_byte(byte: u8) -> bool {
    matches!(byte, 7 | 8 | 9 | 10 | 12 | 13 | 27) || (byte >= b' ' && byte != 0x7f)
}

/// SABnzbd's deliberately conservative SFV signature. This is only a
/// discovery check; selected candidates are still parsed by `SfvCatalog`.
fn looks_like_sfv_probe(bytes: &[u8]) -> bool {
    if bytes.iter().any(|byte| !is_sab_text_byte(*byte)) {
        return false;
    }
    let Ok(contents) = std::str::from_utf8(bytes) else {
        return false;
    };

    let mut valid_entries = 0usize;
    for raw_line in contents.split('\n') {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with(';') {
            continue;
        }
        // `is_sfv_file` in SABnzbd requires literal spaces before an exactly
        // eight-digit hexadecimal CRC32; tabs and abbreviated CRCs do not
        // identify an otherwise obfuscated file as a listing.
        let Some((name, checksum)) = line.rsplit_once(' ') else {
            return false;
        };
        if name.trim().is_empty()
            || checksum.len() != 8
            || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return false;
        }
        valid_entries += 1;
        if valid_entries == SFV_PROBE_CONFIDENCE_ENTRIES {
            return true;
        }
    }

    valid_entries > 0
}

/// The union of one job's `.sfv` listings, keyed by lower-cased basename.
///
/// Basenames because a listing is written relative to wherever the release was
/// packed — often with DOS separators — while the job's files are flat in the
/// working directory; lower-cased because the same listing is routinely
/// produced on a case-insensitive filesystem and the case it records is not
/// evidence of anything.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct SfvCatalog {
    entries: BTreeMap<String, u32>,
    /// Basenames listed more than once with *disagreeing* CRCs. The listing
    /// contradicts itself about these, so neither value is evidence: the entry
    /// is withdrawn and the file stays unverified rather than being judged
    /// against a coin flip.
    conflicting: BTreeSet<String>,
    /// Lines that were neither blank, a comment, nor a well-formed
    /// `name checksum` pair. Counted rather than rejected: a stray line in a
    /// listing says nothing about the files the rest of it names.
    unparsable_lines: usize,
    /// Well-formed entries whose basenames are not present in this job. They
    /// are evidence about another release, so count them without retaining
    /// their keys in the catalog.
    unrelated_entries: usize,
}

impl SfvCatalog {
    #[cfg(test)]
    pub(super) fn parse(contents: &str) -> Self {
        let mut catalog = Self::default();
        catalog.extend_with_filter(contents, |_| true);
        catalog
    }

    fn extend_for_completed_files(
        &mut self,
        contents: &str,
        completed_basenames: &HashSet<String>,
    ) {
        self.extend_with_filter(contents, |basename| completed_basenames.contains(basename));
    }

    fn extend_with_filter(&mut self, contents: &str, mut retain_entry: impl FnMut(&str) -> bool) {
        // A listing written on Windows commonly carries a UTF-8 BOM, which
        // would otherwise become part of the first entry's name.
        let contents = contents.strip_prefix('\u{feff}').unwrap_or(contents);
        // `str::lines` splits on `\n` and drops a trailing `\r`, so CRLF
        // listings need no separate handling.
        for raw_line in contents.lines() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }
            // `;` opens a comment line — the header most generators write.
            if line.starts_with(';') {
                continue;
            }
            let Some((name, checksum)) = line.rsplit_once(char::is_whitespace) else {
                self.unparsable_lines = self.unparsable_lines.saturating_add(1);
                continue;
            };
            let name = name.trim_end();
            // Up to eight hex digits: canonical listings pad to eight, and a
            // few generators drop leading zeros. Anything else — a size
            // column, a stray word, an md5 — is not a CRC32 and is refused
            // rather than truncated into one.
            if name.is_empty()
                || checksum.is_empty()
                || checksum.len() > 8
                || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                self.unparsable_lines = self.unparsable_lines.saturating_add(1);
                continue;
            }
            let Ok(crc32) = u32::from_str_radix(checksum, 16) else {
                self.unparsable_lines = self.unparsable_lines.saturating_add(1);
                continue;
            };
            let basename = sfv_basename(name);
            if basename.is_empty() {
                self.unparsable_lines += 1;
                continue;
            }
            if !retain_entry(&basename) {
                self.unrelated_entries = self.unrelated_entries.saturating_add(1);
                continue;
            }
            insert_entry(&mut self.entries, &mut self.conflicting, basename, crc32);
        }
    }

    /// Fold another listing in. A job may post several `.sfv` files covering
    /// different parts of the release; disagreement *between* listings is the
    /// same contradiction as disagreement inside one and is withdrawn the same
    /// way.
    #[cfg(test)]
    pub(super) fn merge(&mut self, other: Self) {
        for (basename, crc32) in other.entries {
            insert_entry(&mut self.entries, &mut self.conflicting, basename, crc32);
        }
        for basename in other.conflicting {
            self.entries.remove(&basename);
            self.conflicting.insert(basename);
        }
        self.unparsable_lines = self.unparsable_lines.saturating_add(other.unparsable_lines);
        self.unrelated_entries = self
            .unrelated_entries
            .saturating_add(other.unrelated_entries);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
}

fn insert_entry(
    entries: &mut BTreeMap<String, u32>,
    conflicting: &mut BTreeSet<String>,
    basename: String,
    crc32: u32,
) {
    if conflicting.contains(&basename) {
        return;
    }
    match entries.get(&basename) {
        // A listing repeated verbatim is not a contradiction.
        Some(existing) if *existing == crc32 => {}
        Some(_) => {
            entries.remove(&basename);
            conflicting.insert(basename);
        }
        None => {
            entries.insert(basename, crc32);
        }
    }
}

/// The last path component, lower-cased. Both separators are honoured because
/// a listing packed on Windows records `subdir\file.rar` and the same release
/// unpacked elsewhere records `subdir/file.rar`.
fn sfv_basename(name: &str) -> String {
    name.rsplit(['/', '\\'])
        .next()
        .unwrap_or(name)
        .trim()
        .to_ascii_lowercase()
}

/// Which arm produced a file's whole-file CRC32.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SfvFileCrc {
    /// Composed from the per-article CRC32s the wire already verified. No
    /// content read happened.
    Combined(u32),
    /// Streamed from the assembled file on disk.
    Read(u32),
}

impl SfvFileCrc {
    fn value(self) -> u32 {
        match self {
            Self::Combined(crc32) | Self::Read(crc32) => crc32,
        }
    }
}

/// Streaming CRC32 of a whole file. The disk arm's only I/O.
fn stream_crc32(path: &Path) -> std::io::Result<u32> {
    let mut file = File::open(path)?;
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    let mut buffer = vec![0u8; SFV_READ_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize() as u32)
}

/// One completed job file, as the SFV arm needs it.
#[derive(Clone)]
struct SfvJobFile {
    file_id: NzbFileId,
    filename: String,
    decoded_bytes: u64,
}

fn sort_obfuscated_sfv_candidates(candidates: &mut [SfvJobFile]) {
    candidates.sort_by(|left, right| {
        left.decoded_bytes
            .cmp(&right.decoded_bytes)
            .then_with(|| left.filename.cmp(&right.filename))
    });
}

impl Pipeline {
    /// Verify a PAR2-less job against whatever `.sfv` listings it downloaded.
    ///
    /// Returns the named cause when a listed file failed — the caller fails the
    /// job with it. `None` means "nothing to say": no listing, no PAR2-less
    /// job, or every listed file matched (in which case the verdict has already
    /// been recorded through the same family PAR2 verdicts use).
    pub(super) async fn verify_par2_less_job_with_sfv(&mut self, job_id: JobId) -> Option<String> {
        // SCOPE. The same "does this job have PAR2" question the completion
        // gate asks of itself a few hundred lines above (`par2_loaded`), asked
        // through the same accessor so the two cannot drift. A job with a
        // parsed set is adjudicated by that set — including when the set says
        // the payload is damaged — and a listing that disagreed with it would
        // have no standing to overrule recovery data.
        if self.par2_set(job_id).is_some() {
            return None;
        }
        // A direct set's source volumes are never written as conventional
        // files: the bytes live in member partials and an envelope until
        // finalization renames them to their extracted destinations. There is
        // no volume on disk to read and no per-file placement record to
        // compose a CRC from, so every listed volume would read as absent.
        // Such a job keeps today's behaviour rather than being failed for the
        // storage strategy it was downloaded with.
        if self
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted())
        {
            debug!(
                job_id = job_id.0,
                "skipping SFV verification — job has direct-store sets"
            );
            return None;
        }
        // One pass per job. The completion gate is re-entered many times over a
        // job's post-processing, and the disk arm is a full read of the payload.
        if !self.sfv_checked.insert(job_id) {
            return None;
        }

        let state = self.jobs.get(&job_id)?;
        let mut named_listings: Vec<SfvJobFile> = Vec::new();
        let mut by_basename: HashMap<String, Option<NzbFileId>> = HashMap::new();
        let mut completed_files: Vec<SfvJobFile> = Vec::new();
        for file in state.assembly.files() {
            if !file.is_complete() {
                continue;
            }
            let file_id = file.file_id();
            let filename = self.current_filename_for_file(job_id, file);
            // Decoded length. The NZB-declared `total_bytes()` is a yEnc-
            // ENCODED figure and must never be compared against bytes on disk.
            let decoded_bytes = file.received_bytes();
            if filename.to_ascii_lowercase().ends_with(".sfv") {
                named_listings.push(SfvJobFile {
                    file_id,
                    filename: filename.clone(),
                    decoded_bytes,
                });
            }
            let basename = sfv_basename(&filename);
            match by_basename.entry(basename) {
                std::collections::hash_map::Entry::Occupied(mut existing) => {
                    // Two job files whose names differ only by case cannot be
                    // told apart by a listing that records neither reliably.
                    existing.insert(None);
                }
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    vacant.insert(Some(file_id));
                }
            }
            completed_files.push(SfvJobFile {
                file_id,
                filename,
                decoded_bytes,
            });
        }

        let completed_basenames: HashSet<String> = by_basename.keys().cloned().collect();
        let mut budget = SfvScanBudget::default();
        let mut listings: Vec<SfvJobFile> = Vec::new();
        // A genuine `.sfv` name is unambiguous and wins over heuristic
        // discovery. Only when the job has none do we inspect completed files
        // that are plausibly small enough to be listings; the probe itself
        // still supplies the hard I/O bound.
        if named_listings.is_empty() {
            let mut candidates = completed_files
                .iter()
                .filter(|file| file.decoded_bytes <= MAX_SFV_BYTES)
                .cloned()
                .collect::<Vec<_>>();
            sort_obfuscated_sfv_candidates(&mut candidates);
            let candidate_count = candidates.len();
            for (index, candidate) in candidates.into_iter().enumerate() {
                if !budget.take_probe() {
                    break;
                }
                let Some(path) = self.resolve_job_input_path(job_id, &candidate.filename) else {
                    continue;
                };
                match read_sfv_probe(&path).await {
                    Ok(contents) if looks_like_sfv_probe(&contents) => {
                        if !budget.take_listing() {
                            break;
                        }
                        debug!(
                            job_id = job_id.0,
                            file = %candidate.filename,
                            "detected obfuscated SFV listing"
                        );
                        listings.push(candidate);
                        if budget.listings == MAX_SFV_LISTINGS_PER_JOB
                            && index + 1 < candidate_count
                        {
                            budget.listings_exhausted = true;
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(error) => warn!(
                        job_id = job_id.0,
                        file = %candidate.filename,
                        error = %error,
                        "failed to probe completed file for an obfuscated SFV listing"
                    ),
                }
            }
        } else {
            named_listings.sort_by(|left, right| {
                left.filename
                    .cmp(&right.filename)
                    .then_with(|| left.decoded_bytes.cmp(&right.decoded_bytes))
            });
            for listing in named_listings {
                if !budget.take_listing() {
                    break;
                }
                listings.push(listing);
            }
        }

        let mut catalog = SfvCatalog::default();
        for listing in &listings {
            let Some(max_bytes) = budget.listing_read_limit() else {
                budget.listing_bytes_exhausted = true;
                break;
            };
            let Some(path) = self.resolve_job_input_path(job_id, &listing.filename) else {
                continue;
            };
            match read_sfv_listing(&path, max_bytes).await {
                Ok(SfvListingRead::Contents(bytes)) => {
                    if !budget.record_listing_bytes(bytes.len() as u64) {
                        break;
                    }
                    catalog.extend_for_completed_files(
                        &String::from_utf8_lossy(&bytes),
                        &completed_basenames,
                    );
                }
                Ok(SfvListingRead::Oversized) if max_bytes < MAX_SFV_BYTES => {
                    budget.listing_bytes_exhausted = true;
                    break;
                }
                Ok(SfvListingRead::Oversized) => warn!(
                    job_id = job_id.0,
                    file = %listing.filename,
                    limit = MAX_SFV_BYTES,
                    "skipping oversized SFV listing"
                ),
                Err(error) => warn!(
                    job_id = job_id.0,
                    file = %listing.filename,
                    error = %error,
                    "failed to read SFV listing"
                ),
            }
        }

        if budget.exhausted() {
            warn!(
                job_id = job_id.0,
                probes = budget.probes,
                probe_limit = MAX_OBFUSCATED_SFV_PROBES,
                listings = budget.listings,
                listing_limit = MAX_SFV_LISTINGS_PER_JOB,
                listing_bytes = budget.listing_bytes,
                listing_bytes_limit = MAX_SFV_LISTING_BYTES_PER_JOB,
                probes_exhausted = budget.probes_exhausted,
                listings_exhausted = budget.listings_exhausted,
                listing_bytes_exhausted = budget.listing_bytes_exhausted,
                "SFV discovery budget exhausted; verifying entries collected so far"
            );
        }

        if listings.is_empty() {
            return None;
        }

        for basename in &catalog.conflicting {
            warn!(
                job_id = job_id.0,
                entry = %basename,
                "conflicting .sfv entries for one name — leaving it unverified"
            );
        }
        if catalog.unparsable_lines > 0 {
            debug!(
                job_id = job_id.0,
                lines = catalog.unparsable_lines,
                "ignored unparsable .sfv lines"
            );
        }
        if catalog.unrelated_entries > 0 {
            debug!(
                job_id = job_id.0,
                entries = catalog.unrelated_entries,
                "ignored SFV entries unrelated to completed job files"
            );
        }
        if catalog.is_empty() {
            return None;
        }

        // RAR volumes an incremental extraction already consumed and removed.
        // Nothing on disk answers for them, so the disk arm cannot run and
        // their absence is weaver's own doing rather than a hole in the
        // download.
        let consumed_by_extraction = self
            .eagerly_deleted
            .get(&job_id)
            .cloned()
            .unwrap_or_default();

        let listed = catalog.len();
        let mut verified = 0usize;
        let mut combined_arm = 0usize;
        let mut read_arm = 0usize;
        let mut unmatched = 0usize;
        let mut unmeasurable = 0usize;
        let mut failure: Option<String> = None;
        // Sorted by basename, so which failure a multiply-damaged job reports
        // is the same on every run.
        for (basename, expected) in &catalog.entries {
            let Some(Some(file_id)) = by_basename.get(basename).copied() else {
                unmatched += 1;
                warn!(
                    job_id = job_id.0,
                    entry = %basename,
                    "sfv entry names no completed file in this job"
                );
                continue;
            };
            let Some(filename) = completed_files
                .iter()
                .find(|file| file.file_id == file_id)
                .map(|file| file.filename.clone())
            else {
                unmatched += 1;
                continue;
            };
            let consumed = consumed_by_extraction.contains(&filename);
            let Some(actual) = self
                .sfv_file_crc32(job_id, file_id, &filename, consumed)
                .await
            else {
                if consumed {
                    // Extraction took the volume before the gate could measure
                    // it, and the download retained no composable CRC for it
                    // either. That is an absence weaver created, not one the
                    // listing found, so it leaves the file unverified rather
                    // than failing a job whose payload actually arrived.
                    unmeasurable += 1;
                    warn!(
                        job_id = job_id.0,
                        file = %filename,
                        "sfv entry names a volume extraction already consumed — leaving it unverified"
                    );
                    continue;
                }
                failure = Some(format!("sfv missing: {filename}"));
                break;
            };
            match actual {
                SfvFileCrc::Combined(_) => combined_arm += 1,
                SfvFileCrc::Read(_) => read_arm += 1,
            }
            if actual.value() != *expected {
                failure = Some(format!("sfv mismatch: {filename}"));
                break;
            }
            verified += 1;
        }

        #[cfg(test)]
        self.sfv_verify_read_splits.push((combined_arm, read_arm));
        // The combined arm is the zero-read path; the read arm re-reads a file
        // whole because its streamed CRC could not compose (duplicates, gaps,
        // out-of-order assembly). Counted in production so a job that quietly
        // re-reads its payload for SFV is visible instead of folklore.
        crate::runtime::perf_probe::record_value(
            "completion.sfv.files_verified_from_streamed_crc",
            combined_arm as u64,
        );
        if read_arm > 0 {
            crate::runtime::perf_probe::record_value(
                "completion.sfv.files_reread_from_disk",
                read_arm as u64,
            );
            info!(
                job_id = job_id.0,
                combined_from_stream = combined_arm,
                reread_from_disk = read_arm,
                "SFV verification re-read files whose streamed CRC could not compose"
            );
        }

        if let Some(cause) = failure {
            warn!(job_id = job_id.0, error = %cause, "SFV verification failed");
            self.emit_job_verification_started(job_id);
            self.note_job_verification_result(job_id, false, 0);
            return Some(cause);
        }

        if verified == 0 {
            // The listing covered nothing this job holds. Recording a pass
            // would claim evidence that was never gathered, so the job keeps
            // the `unverifiable` attribution its terminal transition gives it.
            info!(
                job_id = job_id.0,
                listed, unmatched, unmeasurable, "no .sfv entry could be measured for this job"
            );
            return None;
        }

        let unlisted = completed_files
            .iter()
            .filter(|file| !catalog.entries.contains_key(&sfv_basename(&file.filename)))
            .count();
        info!(
            job_id = job_id.0,
            verified,
            from_wire_crcs = combined_arm,
            read_from_disk = read_arm,
            unmatched,
            unmeasurable,
            unlisted,
            "SFV verification passed"
        );
        self.emit_job_verification_started(job_id);
        self.note_job_verification_result(job_id, true, 0);
        None
    }

    /// One file's whole-file CRC32, cheap arm first.
    ///
    /// `None` means the file could not be measured at all — the listing names
    /// it but there are no bytes to read.
    ///
    /// `consumed_by_extraction` says the file is a RAR volume an incremental
    /// extraction already deleted. It changes only which evidence arm (a) will
    /// accept, never what a CRC32 means.
    async fn sfv_file_crc32(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
        filename: &str,
        consumed_by_extraction: bool,
    ) -> Option<SfvFileCrc> {
        let path = self.resolve_job_input_path(job_id, filename)?;

        // ARM (a), the zero-I/O one. It ran because every article of this file
        // arrived with a wire-verified CRC32 over a placement record that
        // proves a gap-free, overlap-free tiling of the decoded file — so the
        // whole-file CRC32 is the composition of checksums already checked
        // against the wire, and reading the file back would only re-derive a
        // number the download already holds.
        if let Some(crc32) =
            self.sfv_crc32_from_wire_evidence(job_id, file_id, &path, consumed_by_extraction)
        {
            debug!(
                job_id = job_id.0,
                file = %filename,
                "sfv arm: composed whole-file CRC32 from verified article CRCs"
            );
            return Some(SfvFileCrc::Combined(crc32));
        }

        if consumed_by_extraction {
            // No arm can run: the volume is gone and the download kept no
            // composable CRC for it. The caller reports it as unmeasurable
            // rather than absent.
            return None;
        }

        // ARM (b), the disk one. It ran because arm (a) refused: the file has
        // at least one article the wire could not vouch for. Every uuencode
        // file is here by construction — the encoding carries no checksum, so
        // its segments commit unverified — as is any file whose assembly
        // observed a duplicate or whose placements do not prove contiguity.
        debug!(
            job_id = job_id.0,
            file = %filename,
            "sfv arm: streaming whole-file CRC32 from disk"
        );
        let read_path = path.clone();
        match tokio::task::spawn_blocking(move || stream_crc32(&read_path)).await {
            Ok(Ok(crc32)) => Some(SfvFileCrc::Read(crc32)),
            Ok(Err(error)) => {
                warn!(
                    job_id = job_id.0,
                    file = %path.display(),
                    error = %error,
                    "failed to read a file named by an .sfv listing"
                );
                None
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    file = %path.display(),
                    error = %error,
                    "sfv checksum task panicked"
                );
                None
            }
        }
    }

    /// The composed whole-file CRC32, when the retained assembly evidence is
    /// strong enough to license it.
    ///
    /// The three conditions are exactly the ones the retained-PAR2-session
    /// evidence path already requires before it will stand a composed CRC32 in
    /// for a content read (`contiguous_assembly_proven`): every part CRC
    /// verified against the wire, no duplicate article, and placements that
    /// prove a gap-free tiling. Weakening any of them would let a file that was
    /// rewritten mid-assembly, or one whose articles carried no checksum at
    /// all, claim a verdict the download never earned — so a file that fails
    /// them falls to the disk arm rather than being admitted on a softer test.
    fn sfv_crc32_from_wire_evidence(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
        path: &Path,
        consumed_by_extraction: bool,
    ) -> Option<u32> {
        let checksum = self
            .par2_runtime(job_id)?
            .completed_checksums
            .get(&file_id)
            .copied()?;
        if !checksum.all_parts_crc_verified {
            return None;
        }
        let file = self.jobs.get(&job_id)?.assembly.file(file_id)?;
        if file.has_duplicate_segments() || !file.contiguous_placements_proven() {
            return None;
        }
        // A volume incremental extraction already consumed has no file left to
        // stat. The composition still describes exactly what the listing does
        // — the decoded bytes of the posted volume — so it is accepted, with
        // the caveat stated plainly: for this one shape the verdict vouches for
        // what the wire delivered, not for what survived on disk. Extraction
        // has already read those bytes and checked its own member CRCs against
        // them, which is why the volume was eligible for deletion at all.
        if consumed_by_extraction {
            return Some(checksum.crc32);
        }
        // Otherwise one metadata stat, never a content read: the composition
        // describes the decoded bytes the download wrote, so a file whose
        // on-disk length is not that length is not the file the composition
        // speaks for.
        match std::fs::metadata(path) {
            Ok(metadata) if metadata.len() == file.received_bytes() => Some(checksum.crc32),
            Ok(_) | Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(catalog: &SfvCatalog) -> Vec<(String, u32)> {
        catalog
            .entries
            .iter()
            .map(|(name, crc)| (name.clone(), *crc))
            .collect()
    }

    fn sfv_job_file(filename: &str, decoded_bytes: u64, file_index: u32) -> SfvJobFile {
        SfvJobFile {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index,
            },
            filename: filename.to_string(),
            decoded_bytes,
        }
    }

    #[test]
    fn sfv_scan_budget_bounds_obfuscated_probes_and_selected_listings() {
        let mut probe_budget = SfvScanBudget::default();
        for _ in 0..MAX_OBFUSCATED_SFV_PROBES {
            assert!(probe_budget.take_probe());
        }
        assert!(!probe_budget.take_probe());
        assert_eq!(probe_budget.probes, 1_024);
        assert!(probe_budget.probes_exhausted);

        let mut listing_budget = SfvScanBudget::default();
        for _ in 0..MAX_SFV_LISTINGS_PER_JOB {
            assert!(listing_budget.take_listing());
        }
        assert!(!listing_budget.take_listing());
        assert_eq!(listing_budget.listings, 16);
        assert!(listing_budget.listings_exhausted);
    }

    #[test]
    fn sfv_scan_budget_accepts_exact_aggregate_content_limit_and_rejects_overage() {
        let mut exact = SfvScanBudget::default();
        assert!(exact.record_listing_bytes(MAX_SFV_LISTING_BYTES_PER_JOB));
        assert_eq!(exact.listing_read_limit(), None);

        let mut over = SfvScanBudget::default();
        assert!(!over.record_listing_bytes(MAX_SFV_LISTING_BYTES_PER_JOB + 1));
        assert_eq!(over.listing_bytes, 0);
        assert!(over.listing_bytes_exhausted);
    }

    #[test]
    fn obfuscated_sfv_candidates_sort_by_decoded_size_then_filename() {
        let mut candidates = vec![
            sfv_job_file("zeta", 200, 0),
            sfv_job_file("beta", 100, 1),
            sfv_job_file("alpha", 100, 2),
        ];

        sort_obfuscated_sfv_candidates(&mut candidates);

        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.filename.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta", "zeta"]
        );
    }

    #[test]
    fn incremental_catalog_filters_unrelated_entries_and_unions_split_listings() {
        let completed_basenames = HashSet::from([
            "release.part01.rar".to_string(),
            "release.part02.rar".to_string(),
        ]);
        let mut catalog = SfvCatalog::default();

        catalog.extend_for_completed_files(
            "release.part01.rar 1a2b3c4d\nunrelated.bin deadbeef\nmalformed\n",
            &completed_basenames,
        );
        catalog.extend_for_completed_files("release.part02.rar 01020304\n", &completed_basenames);

        assert_eq!(
            entries(&catalog),
            vec![
                ("release.part01.rar".to_string(), 0x1a2b_3c4d),
                ("release.part02.rar".to_string(), 0x0102_0304),
            ]
        );
        assert_eq!(catalog.unrelated_entries, 1);
        assert_eq!(catalog.unparsable_lines, 1);
    }

    #[test]
    fn comments_and_blank_lines_are_ignored() {
        let catalog = SfvCatalog::parse(
            "; Generated by a checksum tool\n\
             ;\n\
             \n\
             silver.horizon.part01.rar 1a2b3c4d\n",
        );
        assert_eq!(
            entries(&catalog),
            vec![("silver.horizon.part01.rar".to_string(), 0x1a2b_3c4d)]
        );
        assert_eq!(catalog.unparsable_lines, 0);
    }

    #[test]
    fn crlf_and_dos_paths_and_case_are_normalized() {
        let catalog = SfvCatalog::parse(
            "Silver.Horizon\\Disc1\\SILVER.HORIZON.PART01.RAR 0A0B0C0D\r\n\
             Silver.Horizon/Disc1/Silver.Horizon.Part02.rar deadbeef\r\n",
        );
        assert_eq!(
            entries(&catalog),
            vec![
                ("silver.horizon.part01.rar".to_string(), 0x0a0b_0c0d),
                ("silver.horizon.part02.rar".to_string(), 0xdead_beef),
            ]
        );
    }

    #[test]
    fn names_with_spaces_keep_everything_before_the_checksum() {
        let catalog = SfvCatalog::parse("Silver Horizon 1994.avi 12345678\n");
        assert_eq!(
            entries(&catalog),
            vec![("silver horizon 1994.avi".to_string(), 0x1234_5678)]
        );
    }

    #[test]
    fn short_checksums_are_read_as_leading_zeros() {
        let catalog = SfvCatalog::parse("silver.horizon.nfo 1f\n");
        assert_eq!(
            entries(&catalog),
            vec![("silver.horizon.nfo".to_string(), 0x1f)]
        );
    }

    #[test]
    fn junk_lines_are_counted_and_skipped() {
        let catalog = SfvCatalog::parse(
            "silver.horizon.part01.rar 1a2b3c4d\n\
             this-line-has-no-checksum\n\
             silver.horizon.part02.rar zzzzzzzz\n\
             silver.horizon.part03.rar 1234567890\n\
             silver.horizon.part04.rar d41d8cd98f00b204e9800998ecf8427e\n\
             \t  \n",
        );
        assert_eq!(
            entries(&catalog),
            vec![("silver.horizon.part01.rar".to_string(), 0x1a2b_3c4d)]
        );
        assert_eq!(catalog.unparsable_lines, 4);
    }

    #[test]
    fn an_empty_listing_yields_nothing() {
        let catalog = SfvCatalog::parse("");
        assert!(catalog.is_empty());
        assert_eq!(catalog.unparsable_lines, 0);
    }

    #[test]
    fn a_repeated_identical_entry_is_not_a_conflict() {
        let catalog = SfvCatalog::parse(
            "silver.horizon.part01.rar 1a2b3c4d\n\
             SILVER.HORIZON.PART01.RAR 1a2b3c4d\n",
        );
        assert_eq!(
            entries(&catalog),
            vec![("silver.horizon.part01.rar".to_string(), 0x1a2b_3c4d)]
        );
        assert!(catalog.conflicting.is_empty());
    }

    #[test]
    fn conflicting_entries_withdraw_the_name_entirely() {
        let catalog = SfvCatalog::parse(
            "silver.horizon.part01.rar 1a2b3c4d\n\
             silver.horizon.part01.rar 99999999\n\
             silver.horizon.part01.rar 1a2b3c4d\n\
             silver.horizon.part02.rar 0000ffff\n",
        );
        assert_eq!(
            entries(&catalog),
            vec![("silver.horizon.part02.rar".to_string(), 0x0000_ffff)]
        );
        assert!(catalog.conflicting.contains("silver.horizon.part01.rar"));
    }

    #[test]
    fn merging_listings_unions_them_and_withdraws_disagreements() {
        let mut catalog = SfvCatalog::parse(
            "silver.horizon.part01.rar 1a2b3c4d\n\
             silver.horizon.part02.rar 0000ffff\n",
        );
        catalog.merge(SfvCatalog::parse(
            "silver.horizon.part02.rar 11112222\n\
             silver.horizon.part03.rar 33334444\n",
        ));
        assert_eq!(
            entries(&catalog),
            vec![
                ("silver.horizon.part01.rar".to_string(), 0x1a2b_3c4d),
                ("silver.horizon.part03.rar".to_string(), 0x3333_4444),
            ]
        );
        assert!(catalog.conflicting.contains("silver.horizon.part02.rar"));
    }

    #[test]
    fn a_withdrawn_name_stays_withdrawn_when_a_later_listing_repeats_it() {
        let mut catalog = SfvCatalog::parse(
            "silver.horizon.part01.rar 1a2b3c4d\n\
             silver.horizon.part01.rar 99999999\n",
        );
        catalog.merge(SfvCatalog::parse("silver.horizon.part01.rar 1a2b3c4d\n"));
        assert!(catalog.is_empty());
        assert!(catalog.conflicting.contains("silver.horizon.part01.rar"));
    }

    #[test]
    fn a_bom_does_not_become_part_of_the_first_name() {
        let catalog = SfvCatalog::parse("\u{feff}silver.horizon.part01.rar 1a2b3c4d\n");
        assert_eq!(
            entries(&catalog),
            vec![("silver.horizon.part01.rar".to_string(), 0x1a2b_3c4d)]
        );
    }

    #[test]
    fn an_obfuscated_probe_requires_sabnzbd_s_conservative_signature() {
        assert!(looks_like_sfv_probe(
            b"; Generated by a checksum tool\n\nSilver Horizon.bin 1a2b3c4d\n"
        ));

        for sample in [
            b"Silver Horizon.bin 1a2b3c4d\n\0".as_slice(),
            b"; comments only\n\n".as_slice(),
            b"Silver Horizon.bin 1a2b3c4\n".as_slice(),
            b"Silver Horizon.bin\n".as_slice(),
            b"Silver Horizon.bin\t1a2b3c4d\n".as_slice(),
            b"Silver Horizon.bin 1a2b3c4d\nnot an SFV line\n".as_slice(),
        ] {
            assert!(
                !looks_like_sfv_probe(sample),
                "sample must not be discovered as an SFV: {sample:?}"
            );
        }
    }

    #[test]
    fn ten_valid_probe_entries_are_confident_even_with_later_text() {
        let mut sample = String::new();
        for index in 0..SFV_PROBE_CONFIDENCE_ENTRIES {
            sample.push_str(&format!("release.part{index:02}.rar 1a2b3c4d\n"));
        }
        sample.push_str("a later malformed line is outside the confidence cutoff\n");

        assert!(looks_like_sfv_probe(sample.as_bytes()));
    }

    #[tokio::test]
    async fn bounded_sfv_readers_enforce_the_probe_and_listing_ceilings() {
        let temp_dir = tempfile::tempdir().unwrap();
        let probe_path = temp_dir.path().join("probe-after-limit");
        let probe_prefix = ";".repeat(SFV_PROBE_BYTES as usize);
        tokio::fs::write(
            &probe_path,
            format!("{probe_prefix}\nrelease.part01.rar 1a2b3c4d\n"),
        )
        .await
        .unwrap();
        let probe = read_sfv_probe(&probe_path).await.unwrap();
        assert_eq!(probe.len(), SFV_PROBE_BYTES as usize);
        assert!(
            !looks_like_sfv_probe(&probe),
            "the valid entry begins beyond the probe ceiling"
        );

        let listing_path = temp_dir.path().join("listing-boundary");
        tokio::fs::write(&listing_path, vec![b';'; MAX_SFV_BYTES as usize])
            .await
            .unwrap();
        match read_sfv_listing(&listing_path, MAX_SFV_BYTES)
            .await
            .unwrap()
        {
            SfvListingRead::Contents(contents) => {
                assert_eq!(contents.len(), MAX_SFV_BYTES as usize)
            }
            SfvListingRead::Oversized => panic!("an exact-limit listing must be accepted"),
        }

        tokio::fs::write(&listing_path, vec![b';'; MAX_SFV_BYTES as usize + 1])
            .await
            .unwrap();
        assert!(matches!(
            read_sfv_listing(&listing_path, MAX_SFV_BYTES)
                .await
                .unwrap(),
            SfvListingRead::Oversized
        ));
    }

    #[tokio::test]
    async fn a_sparse_obfuscated_candidate_is_probed_but_rejected_as_an_oversized_listing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let sparse_path = temp_dir.path().join("obfuscated-listing");
        let mut prefix = String::new();
        for index in 0..SFV_PROBE_CONFIDENCE_ENTRIES {
            prefix.push_str(&format!("release.part{index:02}.rar 1a2b3c4d\n"));
        }
        // Fill the sampled range with a semicolon comment. The logical extent
        // after it remains a sparse hole, so the probe never sees binary zeros.
        prefix.push(';');
        prefix.extend(std::iter::repeat_n(
            'x',
            SFV_PROBE_BYTES as usize - prefix.len(),
        ));
        assert_eq!(prefix.len(), SFV_PROBE_BYTES as usize);
        std::fs::write(&sparse_path, prefix).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&sparse_path)
            .unwrap()
            .set_len(MAX_SFV_BYTES + 1)
            .unwrap();

        let probe = read_sfv_probe(&sparse_path).await.unwrap();
        assert!(looks_like_sfv_probe(&probe));
        assert!(matches!(
            read_sfv_listing(&sparse_path, MAX_SFV_BYTES).await.unwrap(),
            SfvListingRead::Oversized
        ));
        assert_eq!(
            std::fs::metadata(&sparse_path).unwrap().len(),
            MAX_SFV_BYTES + 1
        );
    }
}
