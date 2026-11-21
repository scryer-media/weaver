// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Renaming the members a finished job delivers.
//
// The PAR2 rename pass fixes *posted* filenames, which is enough when the
// obfuscation stops at the archive volumes. It does nothing for a post whose
// in-archive member name is randomized too: the job folder ends up correctly
// named and the payload inside it does not, which is exactly the shape no
// downstream importer can match.
//
// This pass runs once, at the output seam, over the delivery directory after
// every entry has landed in it. That placement is deliberate. A job's payload
// arrives by two different routes — extraction writes members into the working
// root, direct-store commits them into the staging root — and the two are only
// ever one set after the final move unions them. Candidate selection asks
// "which file dominates this delivery", a question neither root can answer
// alone. Renaming here is still a same-directory rename, and it completes
// before the move reports done, so post-processing, history and the completion
// event all observe the final names.
//
// A delivered file whose name has no extension, renamed or not, takes the
// extension its leading bytes identify: importers walk a delivery by
// extension and pass over a payload without one.
//
// The pass never fails a job. Every step degrades to "leave the name alone".

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use weaver_nzb::delivery_rename::{DeliveredFile, PlannedRename};

use crate::ingest::FileRename;
use crate::jobs::working_dir::OUTPUT_DIR_MARKER;
use crate::post_processing::model::PostProcessingSettings;
use crate::proxies::{Consumer, FetchOptions, ProxyRuntime, RoutedBody, routed_fetch};
use weaver_server_base::jobs::ids::JobId;

// Public release index, queried by the CRC32 of a file inside the archives.
pub(super) const SRRDB_API_BASE: &str = "https://api.srrdb.com/v1";

// One attempt, bounded. A release name is a nicety; completion waiting on a
// third party is not acceptable at any duration a user would notice.
const SRRDB_TIMEOUT: Duration = Duration::from_secs(5);

// Upper bound on decoded SRRDB response bytes. Live archive-CRC responses are
// normally only a few KiB; one MiB leaves substantial headroom while refusing
// an unexpectedly large third-party payload.
pub const MAX_SRRDB_RESPONSE_BYTES: usize = 1024 * 1024;

// Bounds on the delivery walk. A delivery is a handful of files in practice;
// these exist so a pathological tree cannot turn a cosmetic pass into work.
const MAX_SCAN_DEPTH: u32 = 8;
const MAX_SCAN_FILES: usize = 10_000;

// Everything the pass needs, resolved on the pipeline task before the move
// worker is spawned. The worker owns no handles to pipeline state.
#[derive(Clone)]
pub struct DeliveryNamingPlan {
    // The job's display name — the fallback rename target, and the name the
    // refusal gate judges.
    pub job_display_name: String,
    // `Some` only when the operator opted into the outbound lookup.
    pub srrdb: Option<SrrdbInputs>,
}

// The CRC32s the archives' headers stated for their members, keyed by the
// normalized, lowercased delivery-relative path. Both delivery routes learn
// these from the same headers, so the map covers extraction and direct-store
// alike without collapsing members in different directories.
#[derive(Clone)]
pub struct SrrdbInputs {
    pub base_url: String,
    pub crc32_by_member_path: HashMap<String, u32>,
    // Carries the lookup over Default outbound.
    pub proxies: Option<Arc<ProxyRuntime>>,
}

// Renames the delivery's payload when it still wears an obfuscated name,
// and names every file without an extension by its content. Returns the
// renames applied, each with the full name it gave.
//
// `policy` is the unwanted extension policy the delivery was scanned under.
// A payload it refuses, or an executable, is never named after the job.
pub async fn rename_obfuscated_members(
    job_id: JobId,
    root: &Path,
    plan: &DeliveryNamingPlan,
    policy: &PostProcessingSettings,
) -> Vec<FileRename> {
    let files = {
        let root = root.to_path_buf();
        match weaver_server_base::runtime::panic_trap::spawn_blocking(move || {
            let files = scan_delivery(&root);
            let extensions: Vec<_> = files
                .iter()
                .map(|(relative_path, _)| content_extension(&root, relative_path))
                .collect();
            (files, extensions)
        })
        .await
        {
            Ok(files) => files,
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "delivery scan worker failed; leaving delivered names as they are"
                );
                return Vec::new();
            }
        }
    };
    let (files, extensions) = files;
    if files.is_empty() {
        return Vec::new();
    }

    let entries: Vec<DeliveredFile<'_>> = files
        .iter()
        .zip(&extensions)
        .map(|((relative_path, bytes), extension)| DeliveredFile {
            relative_path,
            bytes: *bytes,
            content_extension: *extension,
        })
        .collect();
    // A full name too long for the record of full file names is not copied
    // into each rename: the record would drop it, and with it every entry
    // after it, so those renames record no full name.
    let mut full_name_never_kept = false;
    let mut renames = match weaver_nzb::select_rename_candidate(&entries, |name| {
        policy.unacceptable_extension_match(name).is_some()
    }) {
        Some(candidate) => {
            let candidate_path = entries[candidate].relative_path;
            match resolve_target_name(job_id, candidate_path, plan).await {
                Some((target_stem, full_stem)) => {
                    full_name_never_kept = crate::ingest::full_file_name_never_kept(&full_stem);
                    let full_stem = if full_name_never_kept {
                        target_stem.as_str()
                    } else {
                        full_stem.as_str()
                    };
                    weaver_nzb::plan_renames(&entries, candidate, &target_stem, full_stem)
                }
                None => Vec::new(),
            }
        }
        None => Vec::new(),
    };
    // The policy judges the names the delivery ends up with. A name it would
    // refuse on disk is never written, so a content-derived extension cannot
    // deliver what the operator rejects. The payload's plan stands or falls
    // whole: its helpers are not renamed around a payload that keeps its name.
    let refused = |rename: &PlannedRename| {
        policy
            .unacceptable_extension_match(file_name_of(&rename.to))
            .is_some()
    };
    if let Some(rename) = renames.iter().find(|rename| refused(rename)) {
        debug!(
            job_id = job_id.0,
            member = %rename.from,
            name = %rename.to,
            "skipping member deobfuscation because the policy refuses the name it would write"
        );
        renames.clear();
    }
    let unkept_full_names = if full_name_never_kept {
        renames.len()
    } else {
        0
    };
    let named_by_content = weaver_nzb::plan_content_extensions(&entries, &renames)
        .into_iter()
        .filter(|rename| {
            let keep = !refused(rename);
            if !keep {
                debug!(
                    job_id = job_id.0,
                    member = %rename.from,
                    name = %rename.to,
                    "leaving a member without its content extension because the policy refuses it"
                );
            }
            keep
        });
    renames.extend(named_by_content);
    if renames.is_empty() {
        return Vec::new();
    }

    let root = root.to_path_buf();
    match weaver_server_base::runtime::panic_trap::spawn_blocking(move || {
        apply_renames(job_id, &root, renames, unkept_full_names)
    })
    .await
    {
        Ok(renamed) => renamed,
        Err(error) => {
            warn!(
                job_id = job_id.0,
                error = %error,
                "delivery rename worker failed; some members may keep their obfuscated names"
            );
            Vec::new()
        }
    }
}

// The bare file name of a delivery-relative path.
fn file_name_of(relative_path: &str) -> &str {
    relative_path.rsplit('/').next().unwrap_or(relative_path)
}

// Walks the ladder that decides what the payload should be called: the srrdb
// release name when the operator enabled the lookup and it answered
// unambiguously, the job's own display name otherwise.
//
// Returns the stem to write and the full name it was cleaned or shortened
// from. `None` means the pass refuses itself. That happens when the name it would
// write is no more readable than the one already on disk — renaming one
// unreadable name to another helps nobody and destroys the only handle an
// operator has for correlating the file with its post.
async fn resolve_target_name(
    job_id: JobId,
    candidate_path: &str,
    plan: &DeliveryNamingPlan,
) -> Option<(String, String)> {
    let from_srrdb = match plan.srrdb.as_ref() {
        Some(inputs) => srrdb_target(job_id, candidate_path, inputs).await,
        None => None,
    };

    let (target, full_name) = match from_srrdb {
        Some(release) => (release.clone(), release),
        None => (
            crate::jobs::working_dir::sanitize_dirname(&plan.job_display_name),
            plan.job_display_name.trim().to_string(),
        ),
    };

    if weaver_nzb::looks_obfuscated_for_delivery(&target) {
        debug!(
            job_id = job_id.0,
            member = %candidate_path,
            "skipping member deobfuscation because the rename target is obfuscated too"
        );
        return None;
    }
    Some((target, full_name))
}

async fn srrdb_target(job_id: JobId, candidate_path: &str, inputs: &SrrdbInputs) -> Option<String> {
    let crc32 = *inputs
        .crc32_by_member_path
        .get(&candidate_path.to_ascii_lowercase())?;
    // The lookup leaves only on a route.
    let proxies = inputs.proxies.as_deref()?;

    // The request body is the checksum and nothing else — no filename, no job
    // name, no post identity.
    info!(
        job_id = job_id.0,
        crc32 = format!("{crc32:08x}"),
        "querying the release index for an obfuscated member's checksum"
    );
    let release = fetch_srrdb_release(proxies, &inputs.base_url, crc32).await;
    match release.as_deref() {
        Some(release) => info!(
            job_id = job_id.0,
            crc32 = format!("{crc32:08x}"),
            release = %release,
            "release index named the member's release"
        ),
        None => debug!(
            job_id = job_id.0,
            crc32 = format!("{crc32:08x}"),
            "release index had no unambiguous answer; falling back to the job name"
        ),
    }

    // The answer is a filename supplied by a third party. Treat it exactly like
    // any other untrusted name: it becomes one sanitized path component or it
    // does not get used.
    let release = release?;
    let sanitized = weaver_model::files::sanitize_path_component(&release);
    if sanitized == "unknown" || sanitized != release {
        debug!(
            job_id = job_id.0,
            release = %release,
            "discarding a release name that is not a usable path component"
        );
        return None;
    }
    Some(sanitized)
}

// Builds the search URL. The only job-derived value that leaves the process is
// the checksum, rendered as the eight lowercase hex digits the index expects.
pub fn srrdb_search_url(base_url: &str, crc32: u32) -> String {
    format!(
        "{}/search/archive-crc:{crc32:08x}",
        base_url.trim_end_matches('/')
    )
}

// Reads a release name out of a search response, and only when the index
// pointed at exactly one release. Zero results is a miss; several are an
// ambiguity we have no way to break, and guessing would rename the payload
// after the wrong release.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub fn parse_srrdb_release(body: &str) -> Option<String> {
    parse_srrdb_release_reader(body.as_bytes())
}

#[derive(serde::Deserialize)]
struct SrrdbSearchResponse {
    #[serde(default)]
    results: SrrdbSearchResults,
}

#[derive(Default)]
struct SrrdbSearchResults {
    release: Option<String>,
    ambiguous: bool,
}

impl<'de> serde::Deserialize<'de> for SrrdbSearchResults {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ResultsVisitor;

        impl<'de> serde::de::Visitor<'de> for ResultsVisitor {
            type Value = SrrdbSearchResults;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an array of SRRDB search results")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                #[derive(serde::Deserialize)]
                struct SearchResult {
                    #[serde(default)]
                    release: String,
                }

                let Some(first) = sequence.next_element::<SearchResult>()? else {
                    return Ok(SrrdbSearchResults::default());
                };
                let mut results = SrrdbSearchResults {
                    release: Some(first.release),
                    ambiguous: false,
                };
                if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    results.release = None;
                    results.ambiguous = true;
                    while sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                }
                Ok(results)
            }
        }

        deserializer.deserialize_seq(ResultsVisitor)
    }
}

fn parse_srrdb_release_reader(reader: impl std::io::Read) -> Option<String> {
    let parsed: SrrdbSearchResponse = serde_json::from_reader(reader).ok()?;
    if parsed.results.ambiguous {
        return None;
    }
    let release = parsed.results.release?;
    let release = release.trim();
    if release.is_empty() {
        return None;
    }
    Some(release.to_string())
}

struct SrrdbBodyReader {
    receiver: tokio::sync::mpsc::Receiver<bytes::Bytes>,
    chunk: bytes::Bytes,
    offset: usize,
}

impl SrrdbBodyReader {
    fn new(receiver: tokio::sync::mpsc::Receiver<bytes::Bytes>) -> Self {
        Self {
            receiver,
            chunk: bytes::Bytes::new(),
            offset: 0,
        }
    }
}

impl std::io::Read for SrrdbBodyReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            if self.offset < self.chunk.len() {
                let available = &self.chunk[self.offset..];
                let copied = available.len().min(output.len());
                output[..copied].copy_from_slice(&available[..copied]);
                self.offset += copied;
                return Ok(copied);
            }
            let Some(chunk) = self.receiver.blocking_recv() else {
                return Ok(0);
            };
            self.chunk = chunk;
            self.offset = 0;
        }
    }
}

pub async fn fetch_srrdb_release(
    proxies: &ProxyRuntime,
    base_url: &str,
    crc32: u32,
) -> Option<String> {
    // One bound however many routes are tried.
    tokio::time::timeout(
        SRRDB_TIMEOUT,
        fetch_srrdb_release_on(proxies, base_url, crc32),
    )
    .await
    .ok()
    .flatten()
}

async fn fetch_srrdb_release_on(
    proxies: &ProxyRuntime,
    base_url: &str,
    crc32: u32,
) -> Option<String> {
    let url = reqwest::Url::parse(&srrdb_search_url(base_url, crc32)).ok()?;
    let options = FetchOptions {
        label: "srrdb",
        timeout: SRRDB_TIMEOUT,
        redirects: 5,
        user_agent: "weaver-srrdb/1",
        https_only: false,
        allow_private_network: proxies.allow_private_network(),
    };
    let mut response = routed_fetch(
        proxies,
        Consumer::Outbound,
        &url,
        &options,
        |request, _, _| request,
    )
    .await
    .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let route = response.extensions().get::<RoutedBody>().cloned();

    // Feed the response through a small channel into serde's reader-based
    // parser. The body is never buffered in full, and decoded bytes are counted
    // as they arrive so compression cannot bypass the response ceiling.
    let (body_tx, body_rx) = tokio::sync::mpsc::channel(2);
    let parse_task = weaver_server_base::runtime::panic_trap::spawn_blocking(move || {
        parse_srrdb_release_reader(SrrdbBodyReader::new(body_rx))
    });
    let mut received_bytes = 0usize;
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let Some(next_received_bytes) = received_bytes.checked_add(chunk.len()) else {
                    drop(body_tx);
                    let _ = parse_task.await;
                    return None;
                };
                if next_received_bytes > MAX_SRRDB_RESPONSE_BYTES {
                    drop(body_tx);
                    let _ = parse_task.await;
                    return None;
                }
                received_bytes = next_received_bytes;
                if body_tx.send(chunk).await.is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => {
                if let Some(route) = &route {
                    route.failed();
                }
                drop(body_tx);
                let _ = parse_task.await;
                return None;
            }
        }
    }
    drop(body_tx);
    parse_task.await.ok().flatten()
}

// Collects the delivery's regular files as root-relative, `/`-separated paths.
//
// Symlinks are recorded neither as files nor as directories to descend: this
// pass renames payload, and a link is not payload.
pub fn scan_delivery(root: &Path) -> Vec<(String, u64)> {
    let mut files = Vec::new();
    let mut stack = vec![(root.to_path_buf(), String::new(), 0u32)];

    while let Some((dir, prefix, depth)) = stack.pop() {
        if depth > MAX_SCAN_DEPTH || files.len() >= MAX_SCAN_FILES {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            // Weaver's own bookkeeping is not part of the delivery, and neither
            // is anything else hidden: renaming after a dotfile would be absurd
            // and counting one toward the dominance test would be wrong.
            if name.starts_with('.') || name == OUTPUT_DIR_MARKER {
                continue;
            }
            // `DirEntry::metadata` does not traverse the entry itself, so a
            // symlink reports as neither file nor directory and drops out here.
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            let relative_path = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            if metadata.is_dir() {
                stack.push((entry.path(), relative_path, depth + 1));
            } else if metadata.is_file() {
                if files.len() >= MAX_SCAN_FILES {
                    break;
                }
                files.push((relative_path, metadata.len()));
            }
        }
    }

    files.sort();
    files
}

// The extension a delivered file's content identifies, read only for a
// file whose name has none. Unreadable or unknown bytes name nothing.
pub fn content_extension(root: &Path, relative_path: &str) -> Option<&'static str> {
    use std::io::Read;

    let name = relative_path.rsplit('/').next().unwrap_or(relative_path);
    if !weaver_nzb::lacks_extension(name) {
        return None;
    }
    let file = std::fs::File::open(root.join(relative_path)).ok()?;
    let mut head = Vec::with_capacity(weaver_nzb::SNIFF_BYTES);
    file.take(weaver_nzb::SNIFF_BYTES as u64)
        .read_to_end(&mut head)
        .ok()?;
    weaver_nzb::sniff_extension(&head)
}

// The first `unkept_full_names` renames carry a full name the record of full
// file names cannot keep. Once one of them is applied, the record ends there,
// so it and every rename after it record no full name.
fn apply_renames(
    job_id: JobId,
    root: &Path,
    renames: Vec<PlannedRename>,
    unkept_full_names: usize,
) -> Vec<FileRename> {
    let mut applied = Vec::new();
    let mut record_ended = false;
    for (index, rename) in renames.into_iter().enumerate() {
        let from = root.join(&rename.from);
        let to = root.join(&rename.to);
        // The plan reserved this name, but the directory is on a filesystem
        // other processes can see. Refuse rather than clobber.
        if to.symlink_metadata().is_ok() {
            warn!(
                job_id = job_id.0,
                from = %rename.from,
                to = %rename.to,
                "skipping a member rename because the destination appeared underneath it"
            );
            continue;
        }
        match std::fs::rename(&from, &to) {
            Ok(()) => {
                info!(
                    job_id = job_id.0,
                    from = %rename.from,
                    to = %rename.to,
                    "renamed a delivered member"
                );
                record_ended |= index < unkept_full_names;
                applied.push(FileRename {
                    from: rename.from,
                    to: rename.to,
                    full_name: if record_ended {
                        String::new()
                    } else {
                        rename.full_to
                    },
                });
            }
            Err(error) => warn!(
                job_id = job_id.0,
                from = %rename.from,
                to = %rename.to,
                error = %error,
                "failed to rename an obfuscated delivered member"
            ),
        }
    }
    applied
}
