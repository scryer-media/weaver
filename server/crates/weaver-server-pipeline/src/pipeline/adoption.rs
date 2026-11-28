// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Adopting already-downloaded files from an existing directory for an NZB job.
//
// When a job is submitted with an `existing_dir`, the activation worker scans
// that directory off the async runtime before the job's articles are queued to
// NNTP. Any `.par2` files in the directory are parsed first; files matching a
// PAR2 `FileDescription` are verified by exact length and full MD5. When no
// PAR2 description covers a file, it falls back to filename + expected size.
//
// Verified files are never modified or deleted in `existing_dir`. Their
// segments are withheld from NNTP download and streamed read-only from disk
// into `Pipeline::graph_article_decoded` through a bounded channel, so the
// streaming graph, extraction, PAR2 repair, checkpoints and final delivery
// behave identically to freshly downloaded articles.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use par2_rs::packet::{HEADER_SIZE, MAGIC, PacketHeader, PacketType};
use par2_rs::{FileHashState, Packet, Par2FileSet, parse_packet};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::{DecodedChunk, Pipeline};
use crate::jobs::model::{FileSpec, JobArticles};
use weaver_server_base::jobs::ids::{JobId, NzbFileId, SegmentId};

// Bound directory scanning and PAR2 index reads so a hostile or huge directory
// cannot exhaust memory or stall activation.
const MAX_SCAN_ENTRIES: usize = 50_000;
const MAX_PAR2_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_PAR2_TOTAL_BYTES: u64 = 128 * 1024 * 1024;
const MAX_PAR2_METADATA_BODY: u64 = 16 * 1024 * 1024;
const HASH_BUFFER_BYTES: usize = 64 * 1024;

// A verified file on disk in `existing_dir` that will be streamed into the
// job's graph instead of fetched over NNTP.
#[derive(Debug, Clone)]
pub(crate) struct AdoptedFile {
    pub(crate) file_index: u32,
    pub(crate) path: PathBuf,
    pub(crate) filename: String,
    pub(crate) length: u64,
    pub(crate) ordinals: Vec<u32>,
}

pub(crate) struct AdoptedChunk {
    pub(crate) job_id: JobId,
    pub(crate) activation: u64,
    pub(crate) segment_id: SegmentId,
    pub(crate) offset: u64,
    pub(crate) data: DecodedChunk,
    pub(crate) length: u64,
    pub(crate) declared_end: u64,
    pub(crate) segment: weaver_yenc::Segment,
    pub(crate) filename: String,
}

pub(crate) enum AdoptedMessage {
    Chunk(AdoptedChunk),
    ReadFailed {
        job_id: JobId,
        activation: u64,
        file_index: u32,
        remaining_ordinals: Vec<u32>,
        error: String,
    },
}

// Scan `dir` for complete files matching `files`, verifying MD5 against any
// `.par2` files present in `dir` and falling back to filename + expected size
// otherwise. Runs synchronously on a blocking thread.
pub(crate) fn scan_existing_dir(dir: &Path, files: &[FileSpec]) -> Vec<AdoptedFile> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            warn!(dir = %dir.display(), %error, "could not read existing directory for NZB adoption");
            return Vec::new();
        }
    };

    // Only regular files directly inside `dir` (no symlinks).
    let mut disk_files: HashMap<String, (PathBuf, u64)> = HashMap::new();
    let mut par2_paths: Vec<(PathBuf, u64)> = Vec::new();
    for (count, entry) in entries.enumerate() {
        if count >= MAX_SCAN_ENTRIES {
            warn!(dir = %dir.display(), limit = MAX_SCAN_ENTRIES, "existing directory exceeded scan entry limit");
            break;
        }
        let Ok(entry) = entry else {
            continue;
        };
        let path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.file_type().is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let len = meta.len();
        if name.to_ascii_lowercase().ends_with(".par2") {
            par2_paths.push((path.clone(), len));
        }
        disk_files.insert(name.to_ascii_lowercase(), (path, len));
    }

    if disk_files.is_empty() {
        return Vec::new();
    }

    let par2_expected = load_par2_expectations(&par2_paths);
    // Keep track of small `.par2` index files on disk (which other clients
    // sometimes rename with a CRC/set-id suffix like `Name.9C29AEFB.par2`).
    let mut par2_index_candidates: Vec<(PathBuf, u64)> = par2_paths
        .iter()
        .filter(|(path, _)| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| !n.to_ascii_lowercase().contains(".vol"))
        })
        .cloned()
        .collect();
    par2_index_candidates.sort_by_key(|(_, len)| *len);
    let mut adopted = Vec::new();

    for (index, file) in files.iter().enumerate() {
        if file.segments.is_empty() || !is_safe_filename(&file.filename) {
            continue;
        }
        let key = file.filename.to_ascii_lowercase();
        let matched = disk_files.get(&key).cloned().or_else(|| {
            // If this is a PAR2 index file and exact name wasn't found, match a
            // single non-vol `.par2` index file in `dir` whose size fits.
            if matches!(
                file.role,
                weaver_model::files::FileRole::Par2 { is_index: true, .. }
            ) {
                par2_index_candidates
                    .iter()
                    .find(|(_, len)| fallback_size_matches(file, *len))
                    .cloned()
            } else {
                None
            }
        });
        let Some((path, disk_len)) = matched else {
            continue;
        };

        // Re-check symlink_metadata right before reading.
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.file_type().is_file() || meta.len() != disk_len {
            continue;
        }

        let verified = if let Some(&(expected_len, expected_md5)) = par2_expected.get(&key) {
            if disk_len != expected_len {
                debug!(
                    filename = %file.filename,
                    disk_len,
                    expected_len,
                    "skipping existing file: length does not match PAR2 description"
                );
                false
            } else {
                match hash_file_md5(&path, expected_len) {
                    Ok(actual_md5) if actual_md5 == expected_md5 => true,
                    Ok(_) => {
                        debug!(
                            filename = %file.filename,
                            "skipping existing file: MD5 does not match PAR2 description"
                        );
                        false
                    }
                    Err(error) => {
                        warn!(path = %path.display(), %error, "failed to hash existing file for PAR2 verification");
                        false
                    }
                }
            }
        } else {
            fallback_size_matches(file, disk_len)
        };

        if verified {
            adopted.push(AdoptedFile {
                file_index: index as u32,
                path,
                filename: file.filename.clone(),
                length: disk_len,
                ordinals: file.segments.iter().map(|seg| seg.ordinal).collect(),
            });
        }
    }

    // Stream `.par2` index files first so the graph knows the recovery set
    // geometry before the larger archive volumes finish streaming.
    adopted.sort_by_key(|item| {
        let is_par2_index = files.get(item.file_index as usize).is_some_and(|file| {
            matches!(
                file.role,
                weaver_model::files::FileRole::Par2 { is_index: true, .. }
            )
        });
        (!is_par2_index, item.file_index)
    });

    if !adopted.is_empty() {
        let total_bytes: u64 = adopted.iter().map(|file| file.length).sum();
        info!(
            dir = %dir.display(),
            adopted_files = adopted.len(),
            total_files = files.len(),
            adopted_bytes = total_bytes,
            par2_descriptions = par2_expected.len(),
            "adopted existing files for NZB job"
        );
    }

    adopted
}

fn is_safe_filename(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains('\0')
        && name != "."
        && name != ".."
}

// Fallback size check when no PAR2 description in `existing_dir` covers `file`.
// NZB segment byte counts are wire/encoded sizes (typically 100-105% of decoded
// size for yEnc, plus ~150-300 bytes of header/footer per segment).
fn fallback_size_matches(file: &FileSpec, disk_len: u64) -> bool {
    if disk_len == 0 {
        return false;
    }
    let encoded_total: u64 = file.segments.iter().map(|seg| u64::from(seg.bytes)).sum();
    if encoded_total == 0 {
        return false;
    }
    if disk_len > encoded_total {
        return false;
    }
    // For tiny files (e.g. single-segment small .nfo/.par2), yEnc headers can be
    // a noticeable fraction of `encoded_total`, so any positive size up to
    // `encoded_total` is plausible. For larger files, require at least 90% of
    // encoded bytes so truncated half-downloaded volumes are rejected.
    if encoded_total < 8192 {
        true
    } else {
        disk_len >= encoded_total.saturating_mul(90) / 100
    }
}

fn load_par2_expectations(par2_paths: &[(PathBuf, u64)]) -> HashMap<String, (u64, [u8; 16])> {
    let mut packets: Vec<Packet> = Vec::new();
    let mut total_read: u64 = 0;

    // Sort smaller `.par2` files first so the index `.par2` is always read even
    // if large recovery volumes hit the total cap.
    let mut sorted = par2_paths.to_vec();
    sorted.sort_by_key(|(_, len)| *len);

    for (path, len) in sorted {
        if len == 0 || len > MAX_PAR2_FILE_BYTES {
            continue;
        }
        if total_read.saturating_add(len) > MAX_PAR2_TOTAL_BYTES {
            break;
        }
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        total_read = total_read.saturating_add(bytes.len() as u64);
        collect_par2_metadata_packets(&bytes, &mut packets);
    }

    if packets.is_empty() {
        return HashMap::new();
    }

    let Ok(set) = Par2FileSet::from_packets(packets) else {
        return HashMap::new();
    };

    let mut map = HashMap::with_capacity(set.files.len());
    for desc in set.files.values() {
        if is_safe_filename(&desc.filename) {
            map.insert(
                desc.filename.to_ascii_lowercase(),
                (desc.length, desc.hash_full),
            );
        }
    }
    map
}

// Scan a `.par2` buffer for valid metadata packets (skipping recovery slice
// payloads so we don't allocate megabytes of recovery data).
fn collect_par2_metadata_packets(bytes: &[u8], out: &mut Vec<Packet>) {
    let mut pos = 0usize;
    while pos + HEADER_SIZE <= bytes.len() {
        if &bytes[pos..pos + 8] != MAGIC {
            if let Some(next) = bytes[pos + 1..].windows(8).position(|w| w == MAGIC) {
                pos += 1 + next;
                continue;
            }
            break;
        }
        let Ok(header) = PacketHeader::parse(&bytes[pos..], pos as u64) else {
            pos += 1;
            continue;
        };
        let pkt_len = header.length as usize;
        if pkt_len < HEADER_SIZE || pos.saturating_add(pkt_len) > bytes.len() {
            pos += 1;
            continue;
        }
        if header.packet_type == PacketType::RecoverySlice
            || header.body_length() > MAX_PAR2_METADATA_BODY
        {
            pos += pkt_len;
            continue;
        }
        match parse_packet(&bytes[pos..pos + pkt_len], pos as u64) {
            Ok((packet, _)) => {
                out.push(packet);
                pos += pkt_len;
            }
            Err(_) => {
                pos += 1;
            }
        }
    }
}

fn hash_file_md5(path: &Path, expected_len: u64) -> std::io::Result<[u8; 16]> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = FileHashState::new();
    let mut buf = vec![0u8; HASH_BUFFER_BYTES];
    let mut remaining = expected_len;
    while remaining > 0 {
        let want = (remaining as usize).min(buf.len());
        let read = file.read(&mut buf[..want])?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "existing file was truncated while hashing",
            ));
        }
        hasher.update(&buf[..read]);
        remaining -= read as u64;
    }
    // Ensure file did not grow past expected_len.
    if file.read(&mut buf[..1])? != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "existing file grew past expected length while hashing",
        ));
    }
    Ok(hasher.finalize())
}

// Stream all adopted files from disk into `tx` on a blocking thread, sliced
// across each file's NZB segment ordinals.
pub(crate) fn spawn_adopted_reader(
    job_id: JobId,
    activation: u64,
    adopted: Vec<AdoptedFile>,
    tx: mpsc::Sender<AdoptedMessage>,
) {
    tokio::task::spawn_blocking(move || {
        for file in adopted {
            let n = file.ordinals.len() as u64;
            if n == 0 {
                continue;
            }
            let mut reader = match std::fs::File::open(&file.path) {
                Ok(reader) => reader,
                Err(error) => {
                    let _ = tx.blocking_send(AdoptedMessage::ReadFailed {
                        job_id,
                        activation,
                        file_index: file.file_index,
                        remaining_ordinals: file.ordinals,
                        error: error.to_string(),
                    });
                    continue;
                }
            };
            let base = file.length / n;
            let rem = file.length % n;
            let mut offset = 0u64;
            for (idx, &ordinal) in file.ordinals.iter().enumerate() {
                let chunk_len = base + u64::from((idx as u64) < rem);
                let mut buf = vec![0u8; chunk_len as usize];
                if chunk_len > 0
                    && let Err(error) = reader.read_exact(&mut buf)
                {
                    let _ = tx.blocking_send(AdoptedMessage::ReadFailed {
                        job_id,
                        activation,
                        file_index: file.file_index,
                        remaining_ordinals: file.ordinals[idx..].to_vec(),
                        error: error.to_string(),
                    });
                    break;
                }
                let crc32 = crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, &buf) as u32;
                let declared_end = offset + chunk_len;
                let segment = weaver_yenc::Segment {
                    file_offset: offset,
                    len: chunk_len,
                    crc32,
                };
                let msg = AdoptedMessage::Chunk(AdoptedChunk {
                    job_id,
                    activation,
                    segment_id: SegmentId {
                        file_id: NzbFileId {
                            job_id,
                            file_index: file.file_index,
                        },
                        segment_number: ordinal,
                    },
                    offset,
                    data: DecodedChunk::Contiguous(bytes::Bytes::from(buf)),
                    length: file.length,
                    declared_end,
                    segment,
                    filename: file.filename.clone(),
                });
                if tx.blocking_send(msg).is_err() {
                    return;
                }
                offset = declared_end;
            }
        }
    });
}

impl Pipeline {
    pub(crate) fn handle_adopted_message(&mut self, msg: AdoptedMessage) {
        match msg {
            AdoptedMessage::Chunk(chunk) => self.handle_adopted_chunk(chunk),
            AdoptedMessage::ReadFailed {
                job_id,
                activation,
                file_index,
                remaining_ordinals,
                error,
            } => {
                if self.adopted_activations.get(&job_id) != Some(&activation) {
                    return;
                }
                warn!(
                    job_id = job_id.0,
                    file_index,
                    remaining = remaining_ordinals.len(),
                    %error,
                    "failed to read adopted file from disk; re-queuing remaining segments for NNTP download"
                );
                let Some(state) = self.jobs.get_mut(&job_id) else {
                    return;
                };
                let Some(file_spec) = state.spec.files.get(file_index as usize) else {
                    return;
                };
                let groups: std::sync::Arc<[String]> =
                    std::sync::Arc::from(file_spec.groups.as_slice());
                let file_id = NzbFileId { job_id, file_index };
                if let Some(active) = state.articles.active_mut() {
                    for ordinal in remaining_ordinals {
                        let bytes = file_spec
                            .segment(ordinal)
                            .map_or(0, |segment| segment.bytes);
                        active.download_queue.push_article(
                            SegmentId {
                                file_id,
                                segment_number: ordinal,
                            },
                            &groups,
                            crate::pipeline::graph::NEUTRAL_PRIORITY,
                            bytes,
                        );
                    }
                }
                self.download_dispatch_wake = true;
            }
        }
    }

    fn handle_adopted_chunk(&mut self, chunk: AdoptedChunk) {
        let AdoptedChunk {
            job_id,
            activation,
            segment_id,
            offset,
            data,
            length,
            declared_end,
            segment,
            filename,
        } = chunk;

        if self.adopted_activations.get(&job_id) != Some(&activation) {
            return;
        }
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        if !matches!(state.articles, JobArticles::Active(_))
            || !self.graph_jobs.contains_key(&job_id)
        {
            return;
        }
        let decoded_size = data.len_bytes() as u32;
        if let Some(file) = state
            .articles
            .assembly_mut()
            .and_then(|assembly| assembly.file_mut(segment_id.file_id))
        {
            file.record_placement(segment_id.segment_number, offset, decoded_size);
        }

        self.graph_article_decoded(
            segment_id,
            offset,
            data,
            true,
            true,
            length,
            Some(declared_end),
            vec![segment],
            &filename,
        );
        if self.held_for_full_disk(job_id) {
            return;
        }
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.downloaded_bytes += u64::from(decoded_size);
            if let Some(category_bytes) = state.category_bytes.as_ref() {
                category_bytes.fetch_add(u64::from(decoded_size), Ordering::Relaxed);
            }
        }
        self.report_job_soon(job_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::model::{MessageIds, SegmentSpec};
    use weaver_model::files::FileRole;

    fn make_spec(filename: &str, segment_bytes: &[u32]) -> FileSpec {
        FileSpec {
            filename: filename.to_string(),
            role: FileRole::from_filename(filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: segment_bytes
                .iter()
                .enumerate()
                .map(|(i, &bytes)| SegmentSpec {
                    ordinal: i as u32,
                    article_number: (i + 1) as u32,
                    bytes,
                    message_ids: MessageIds::new(vec![format!("msg-{filename}-{i}@test")]),
                })
                .collect(),
        }
    }

    fn make_par2_index(files: &[(&str, &[u8])]) -> Vec<u8> {
        let slice_size = 4096u64;
        let mut file_ids = Vec::new();
        let mut file_descs = Vec::new();
        for &(name, data) in files {
            let hash_full = par2_rs::checksum::md5(data);
            let hash_16k = par2_rs::checksum::md5(&data[..data.len().min(16384)]);
            let mut id_input = Vec::new();
            id_input.extend_from_slice(&hash_16k);
            id_input.extend_from_slice(&(data.len() as u64).to_le_bytes());
            id_input.extend_from_slice(name.as_bytes());
            let file_id = par2_rs::checksum::md5(&id_input);
            file_ids.push(file_id);
            file_descs.push((file_id, hash_full, hash_16k, data.len() as u64, name));
        }
        file_ids.sort();

        // Build Main packet body: slice_size (8) + count (4) + sorted file_ids
        let mut main_body = Vec::new();
        main_body.extend_from_slice(&slice_size.to_le_bytes());
        main_body.extend_from_slice(&(file_ids.len() as u32).to_le_bytes());
        for id in &file_ids {
            main_body.extend_from_slice(id);
        }
        let set_id = par2_rs::checksum::md5(&main_body);

        let build_packet = |pkt_type: &[u8; 16], body: &[u8]| -> Vec<u8> {
            let length = (HEADER_SIZE + body.len()) as u64;
            let mut pkt = Vec::with_capacity(length as usize);
            pkt.extend_from_slice(MAGIC);
            pkt.extend_from_slice(&length.to_le_bytes());
            pkt.extend_from_slice(&[0u8; 16]);
            pkt.extend_from_slice(&set_id);
            pkt.extend_from_slice(pkt_type);
            pkt.extend_from_slice(body);
            let hash = par2_rs::checksum::md5(&pkt[32..]);
            pkt[16..32].copy_from_slice(&hash);
            pkt
        };

        let mut out = build_packet(b"PAR 2.0\0Main\0\0\0\0", &main_body);
        for (file_id, hash_full, hash_16k, length, name) in file_descs {
            let mut body = Vec::new();
            body.extend_from_slice(&file_id);
            body.extend_from_slice(&hash_full);
            body.extend_from_slice(&hash_16k);
            body.extend_from_slice(&length.to_le_bytes());
            body.extend_from_slice(name.as_bytes());
            while !body.len().is_multiple_of(4) {
                body.push(0);
            }
            out.extend_from_slice(&build_packet(b"PAR 2.0\0FileDesc", &body));
        }
        out
    }

    #[test]
    fn scan_verifies_md5_against_par2_and_rejects_corrupt_or_missing() {
        let dir = tempfile::tempdir().unwrap();
        let good_data = vec![0x42u8; 12_000];
        let bad_data_expected = vec![0x99u8; 12_000];
        let mut bad_data_on_disk = bad_data_expected.clone();
        bad_data_on_disk[500] ^= 0xFF;

        let par2 = make_par2_index(&[
            ("part01.rar", &good_data),
            ("part02.rar", &bad_data_expected),
            ("part03.rar", &good_data),
        ]);
        std::fs::write(dir.path().join("set.par2"), &par2).unwrap();
        std::fs::write(dir.path().join("part01.rar"), &good_data).unwrap();
        std::fs::write(dir.path().join("part02.rar"), &bad_data_on_disk).unwrap();
        // part03.rar intentionally missing

        let specs = vec![
            make_spec("part01.rar", &[6_200, 6_200]),
            make_spec("part02.rar", &[6_200, 6_200]),
            make_spec("part03.rar", &[6_200, 6_200]),
        ];
        let adopted = scan_existing_dir(dir.path(), &specs);
        assert_eq!(adopted.len(), 1);
        assert_eq!(adopted[0].file_index, 0);
        assert_eq!(adopted[0].filename, "part01.rar");
        assert_eq!(adopted[0].length, 12_000);
        assert_eq!(adopted[0].ordinals, vec![0, 1]);
    }

    #[test]
    fn scan_falls_back_to_size_without_par2_and_rejects_truncated() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("part01.rar"), vec![1u8; 20_000]).unwrap();
        // Truncated (only 10_000 of ~20_400)
        std::fs::write(dir.path().join("part02.rar"), vec![2u8; 10_000]).unwrap();

        let specs = vec![
            make_spec("part01.rar", &[10_200, 10_200]),
            make_spec("part02.rar", &[10_200, 10_200]),
        ];
        let adopted = scan_existing_dir(dir.path(), &specs);
        assert_eq!(adopted.len(), 1);
        assert_eq!(adopted[0].filename, "part01.rar");
    }

    #[test]
    fn scan_rejects_unsafe_filenames() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("ok.rar"), vec![1u8; 10_000]).unwrap();
        let specs = vec![make_spec("../ok.rar", &[10_000])];
        let adopted = scan_existing_dir(dir.path(), &specs);
        assert!(adopted.is_empty());
    }

    #[test]
    fn scan_adopts_renamed_par2_index_and_sorts_it_first() {
        let dir = tempfile::tempdir().unwrap();
        let rar_data = vec![0x55u8; 12_000];
        let par2 = make_par2_index(&[("archive.part01.rar", &rar_data)]);
        // Other clients often append a CRC/set-id suffix to the index `.par2`.
        std::fs::write(dir.path().join("archive.9C29AEFB.par2"), &par2).unwrap();
        std::fs::write(dir.path().join("archive.part01.rar"), &rar_data).unwrap();

        let specs = vec![
            make_spec("archive.part01.rar", &[6_200, 6_200]),
            make_spec("archive.par2", &[par2.len() as u32 + 256]),
        ];
        let adopted = scan_existing_dir(dir.path(), &specs);
        assert_eq!(adopted.len(), 2);
        // The PAR2 index must be streamed first so the graph learns the set
        // geometry before larger archive volumes finish streaming.
        assert_eq!(adopted[0].filename, "archive.par2");
        assert_eq!(adopted[0].file_index, 1);
        assert_eq!(adopted[1].filename, "archive.part01.rar");
        assert_eq!(adopted[1].file_index, 0);
    }
}
