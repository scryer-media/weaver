//! Renaming the members a finished job delivers.
//!
//! The PAR2 rename pass fixes *posted* filenames, which is enough when the
//! obfuscation stops at the archive volumes. It does nothing for a post whose
//! in-archive member name is randomized too: the job folder ends up correctly
//! named and the payload inside it does not, which is exactly the shape no
//! downstream importer can match.
//!
//! This pass runs once, at the output seam, over the delivery directory after
//! every entry has landed in it. That placement is deliberate. A job's payload
//! arrives by two different routes — extraction writes members into the working
//! root, direct-store commits them into the staging root — and the two are only
//! ever one set after the final move unions them. Candidate selection asks
//! "which file dominates this delivery", a question neither root can answer
//! alone. Renaming here is still a same-directory rename, and it completes
//! before the move reports done, so post-processing, history and the completion
//! event all observe the final names.
//!
//! The pass never fails a job. Every step degrades to "leave the name alone".

use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;

use tracing::{debug, info, warn};

use weaver_nzb::delivery_rename::{DeliveredFile, PlannedRename};

use crate::jobs::ids::JobId;
use crate::jobs::working_dir::OUTPUT_DIR_MARKER;

/// Public release index, queried by the CRC32 of a file inside the archives.
pub(super) const SRRDB_API_BASE: &str = "https://api.srrdb.com/v1";

/// Environment switch for the release-index rung: `WEAVER_SRRDB_LOOKUP`.
///
/// **This is how the rung is turned on today**, and the reason it exists rather
/// than the config row alone is consent: the lookup is the one step of
/// completion that leaves the operator's network, and until the settings UI can
/// say so in words, a switch visible in the process environment is a better
/// place to make that choice than a row in a file. It overrides config in both
/// directions — an off word forces the rung off even where config enabled it —
/// and leaving it unset defers to config, which defaults off.
pub(crate) const SRRDB_LOOKUP_ENV: &str = "WEAVER_SRRDB_LOOKUP";

/// Whether the environment says anything about the release-index rung. Read
/// once, in the same style as the direct-store gate.
fn env_srrdb_lookup() -> Option<bool> {
    static OVERRIDE: OnceLock<Option<bool>> = OnceLock::new();
    *OVERRIDE.get_or_init(|| {
        crate::pipeline::direct_store::parse_enabled(
            std::env::var(SRRDB_LOOKUP_ENV).ok().as_deref(),
        )
    })
}

/// The rung's gate: the environment first, config behind it.
///
/// Split from the reader so the precedence is testable without mutating process
/// state, exactly as the direct-store gate splits `resolve_parts`.
pub(crate) fn srrdb_lookup_enabled(env: Option<bool>, from_config: bool) -> bool {
    env.unwrap_or(from_config)
}

/// The live gate, for callers that are not a test pinning the inputs.
pub(crate) fn srrdb_lookup_enabled_now(from_config: bool) -> bool {
    srrdb_lookup_enabled(env_srrdb_lookup(), from_config)
}

/// One attempt, bounded. A release name is a nicety; completion waiting on a
/// third party is not acceptable at any duration a user would notice.
const SRRDB_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounds on the delivery walk. A delivery is a handful of files in practice;
/// these exist so a pathological tree cannot turn a cosmetic pass into work.
const MAX_SCAN_DEPTH: u32 = 8;
const MAX_SCAN_FILES: usize = 10_000;

/// Everything the pass needs, resolved on the pipeline task before the move
/// worker is spawned. The worker owns no handles to pipeline state.
#[derive(Debug, Clone)]
pub(crate) struct DeliveryNamingPlan {
    /// The job's display name — the fallback rename target, and the name the
    /// refusal gate judges.
    pub(crate) job_display_name: String,
    /// `Some` only when the operator opted into the outbound lookup.
    pub(crate) srrdb: Option<SrrdbInputs>,
}

/// The CRC32s the archives' headers stated for their members, keyed by the
/// member's filename lowercased. Both delivery routes learn these from the same
/// headers, so the map covers extraction and direct-store alike.
#[derive(Debug, Clone)]
pub(crate) struct SrrdbInputs {
    pub(crate) base_url: String,
    pub(crate) crc32_by_member_name: HashMap<String, u32>,
}

/// Renames the delivery's payload when it still wears an obfuscated name.
/// Returns how many files were renamed.
pub(super) async fn rename_obfuscated_members(
    job_id: JobId,
    root: &Path,
    plan: &DeliveryNamingPlan,
) -> u32 {
    let files = {
        let root = root.to_path_buf();
        match tokio::task::spawn_blocking(move || scan_delivery(&root)).await {
            Ok(files) => files,
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "delivery scan worker failed; leaving delivered names as they are"
                );
                return 0;
            }
        }
    };
    if files.is_empty() {
        return 0;
    }

    let entries: Vec<DeliveredFile<'_>> = files
        .iter()
        .map(|(relative_path, bytes)| DeliveredFile {
            relative_path,
            bytes: *bytes,
        })
        .collect();
    let Some(candidate) = weaver_nzb::select_rename_candidate(&entries) else {
        return 0;
    };
    let candidate_name = file_name_of(entries[candidate].relative_path);

    let target = resolve_target_name(job_id, candidate_name, plan).await;
    let Some(target_stem) = target else {
        return 0;
    };

    let renames = weaver_nzb::plan_renames(&entries, candidate, &target_stem);
    if renames.is_empty() {
        return 0;
    }

    let root = root.to_path_buf();
    match tokio::task::spawn_blocking(move || apply_renames(job_id, &root, &renames)).await {
        Ok(renamed) => renamed,
        Err(error) => {
            warn!(
                job_id = job_id.0,
                error = %error,
                "delivery rename worker failed; some members may keep their obfuscated names"
            );
            0
        }
    }
}

/// Walks the ladder that decides what the payload should be called: the srrdb
/// release name when the operator enabled the lookup and it answered
/// unambiguously, the job's own display name otherwise.
///
/// `None` means the pass refuses itself. That happens when the name it would
/// write is no more readable than the one already on disk — renaming one
/// unreadable name to another helps nobody and destroys the only handle an
/// operator has for correlating the file with its post.
async fn resolve_target_name(
    job_id: JobId,
    candidate_name: &str,
    plan: &DeliveryNamingPlan,
) -> Option<String> {
    let from_srrdb = match plan.srrdb.as_ref() {
        Some(inputs) => srrdb_target(job_id, candidate_name, inputs).await,
        None => None,
    };

    let target = from_srrdb
        .unwrap_or_else(|| crate::jobs::working_dir::sanitize_dirname(&plan.job_display_name));

    if weaver_nzb::looks_obfuscated_for_delivery(&target) {
        debug!(
            job_id = job_id.0,
            member = %candidate_name,
            "skipping member deobfuscation because the rename target is obfuscated too"
        );
        return None;
    }
    Some(target)
}

async fn srrdb_target(job_id: JobId, candidate_name: &str, inputs: &SrrdbInputs) -> Option<String> {
    let crc32 = *inputs
        .crc32_by_member_name
        .get(&candidate_name.to_ascii_lowercase())?;

    // The request body is the checksum and nothing else — no filename, no job
    // name, no post identity.
    info!(
        job_id = job_id.0,
        crc32 = format!("{crc32:08x}"),
        "querying the release index for an obfuscated member's checksum"
    );
    let release = fetch_srrdb_release(&inputs.base_url, crc32).await;
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

/// Builds the search URL. The only job-derived value that leaves the process is
/// the checksum, rendered as the eight lowercase hex digits the index expects.
pub(super) fn srrdb_search_url(base_url: &str, crc32: u32) -> String {
    format!(
        "{}/search/archive-crc:{crc32:08x}",
        base_url.trim_end_matches('/')
    )
}

/// Reads a release name out of a search response, and only when the index
/// pointed at exactly one release. Zero results is a miss; several are an
/// ambiguity we have no way to break, and guessing would rename the payload
/// after the wrong release.
pub(super) fn parse_srrdb_release(body: &str) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct SearchResponse {
        #[serde(default)]
        results: Vec<SearchResult>,
    }
    #[derive(serde::Deserialize)]
    struct SearchResult {
        #[serde(default)]
        release: String,
    }

    let parsed: SearchResponse = serde_json::from_str(body).ok()?;
    let [result] = parsed.results.as_slice() else {
        return None;
    };
    let release = result.release.trim();
    if release.is_empty() {
        return None;
    }
    Some(release.to_string())
}

async fn fetch_srrdb_release(base_url: &str, crc32: u32) -> Option<String> {
    let url = srrdb_search_url(base_url, crc32);
    let client = reqwest::Client::builder()
        .timeout(SRRDB_TIMEOUT)
        .user_agent("weaver-srrdb/1")
        .build()
        .ok()?;
    let response = client.get(url).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body = response.text().await.ok()?;
    parse_srrdb_release(&body)
}

/// Collects the delivery's regular files as root-relative, `/`-separated paths.
///
/// Symlinks are recorded neither as files nor as directories to descend: this
/// pass renames payload, and a link is not payload.
fn scan_delivery(root: &Path) -> Vec<(String, u64)> {
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

fn apply_renames(job_id: JobId, root: &Path, renames: &[PlannedRename]) -> u32 {
    let mut applied = 0u32;
    for rename in renames {
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
                    "renamed an obfuscated delivered member"
                );
                applied += 1;
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

fn file_name_of(relative_path: &str) -> &str {
    relative_path
        .rfind('/')
        .map_or(relative_path, |slash| &relative_path[slash + 1..])
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn the_environment_has_the_last_word_on_the_outbound_rung() {
        // Unset defers to config, which is where the default-off lives.
        assert!(!srrdb_lookup_enabled(None, false));
        assert!(srrdb_lookup_enabled(None, true));
        // Set overrides config in BOTH directions: the switch exists so an
        // operator can grant consent without editing config, and withdraw it
        // without trusting that the config write took.
        assert!(srrdb_lookup_enabled(Some(true), false));
        assert!(!srrdb_lookup_enabled(Some(false), true));
    }

    #[test]
    fn the_switch_reads_the_same_on_off_words_as_the_direct_store_gate() {
        // One vocabulary for every operator switch: the parser is shared with
        // the direct-store gate rather than restated here.
        use crate::pipeline::direct_store::parse_enabled;
        assert_eq!(parse_enabled(Some("1")), Some(true));
        assert_eq!(parse_enabled(Some(" YES ")), Some(true));
        assert_eq!(parse_enabled(Some("off")), Some(false));
        // A typo must not read as "off" — it defers to config instead, so a
        // fat-fingered variable cannot silently withdraw a granted consent in
        // the direction the operator did not ask for.
        assert_eq!(parse_enabled(Some("ture")), None);
        assert_eq!(parse_enabled(None), None);
    }

    fn plan(job_display_name: &str) -> DeliveryNamingPlan {
        DeliveryNamingPlan {
            job_display_name: job_display_name.to_string(),
            srrdb: None,
        }
    }

    fn plan_with_srrdb(job_display_name: &str, member: &str, crc32: u32) -> DeliveryNamingPlan {
        DeliveryNamingPlan {
            job_display_name: job_display_name.to_string(),
            srrdb: Some(SrrdbInputs {
                // Loopback on a port nothing can bind: the request is refused
                // by the kernel, with no name resolution and nothing leaving
                // the machine.
                base_url: "http://127.0.0.1:1/v1".to_string(),
                crc32_by_member_name: HashMap::from([(member.to_ascii_lowercase(), crc32)]),
            }),
        }
    }

    fn write_file(root: &Path, relative_path: &str, bytes: u64) {
        let path = root.join(relative_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(bytes).unwrap();
    }

    fn delivered_names(root: &Path) -> Vec<String> {
        let mut names: Vec<String> = scan_delivery(root)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        names.sort();
        names
    }

    // ── srrdb URL and response parsing (never touches the network) ───────

    #[test]
    fn search_url_renders_the_checksum_as_eight_lowercase_hex_digits() {
        assert_eq!(
            srrdb_search_url("https://api.example.test/v1", 0x0a1b_2c3d),
            "https://api.example.test/v1/search/archive-crc:0a1b2c3d"
        );
        // Leading zeroes are significant to the index.
        assert_eq!(
            srrdb_search_url("https://api.example.test/v1/", 0x0000_00ff),
            "https://api.example.test/v1/search/archive-crc:000000ff"
        );
    }

    #[test]
    fn a_single_result_yields_its_release_name() {
        let body = r#"{"resultsCount":"1","results":[
            {"release":"Silver.Horizon.2024.1080p.WEB-DL.x264-CREW","date":"2024-04-01"}
        ]}"#;
        assert_eq!(
            parse_srrdb_release(body).as_deref(),
            Some("Silver.Horizon.2024.1080p.WEB-DL.x264-CREW")
        );
    }

    #[test]
    fn a_miss_or_an_ambiguity_yields_nothing() {
        assert_eq!(parse_srrdb_release(r#"{"results":[]}"#), None);
        assert_eq!(
            parse_srrdb_release(
                r#"{"results":[{"release":"Silver.Horizon.2024"},{"release":"Quiet.Harbour.2021"}]}"#
            ),
            None
        );
        assert_eq!(parse_srrdb_release(r#"{"results":[{"release":""}]}"#), None);
        assert_eq!(parse_srrdb_release("not json at all"), None);
        assert_eq!(parse_srrdb_release("{}"), None);
    }

    // ── the pass over a real directory ──────────────────────────────────

    #[tokio::test]
    async fn an_obfuscated_member_takes_the_job_name() {
        let root = tempfile::tempdir().unwrap();
        write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);
        write_file(root.path(), "Yb5drZSkNi20UCMkb-sample.mkv", 2 * MIB);
        write_file(root.path(), "Yb5drZSkNi20UCMkb.dut.srt", 4096);

        let renamed =
            rename_obfuscated_members(JobId(1), root.path(), &plan("Silver Horizon 2024")).await;

        assert_eq!(renamed, 3);
        assert_eq!(
            delivered_names(root.path()),
            vec![
                "Silver Horizon 2024-sample.mkv".to_string(),
                "Silver Horizon 2024.dut.srt".to_string(),
                "Silver Horizon 2024.mkv".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn a_readable_member_is_left_alone() {
        let root = tempfile::tempdir().unwrap();
        write_file(root.path(), "Silver.Horizon.2024.1080p.mkv", 64 * MIB);

        let renamed =
            rename_obfuscated_members(JobId(2), root.path(), &plan("Quiet Harbour")).await;

        assert_eq!(renamed, 0);
        assert_eq!(
            delivered_names(root.path()),
            vec!["Silver.Horizon.2024.1080p.mkv".to_string()]
        );
    }

    #[tokio::test]
    async fn an_obfuscated_job_name_refuses_the_pass() {
        let root = tempfile::tempdir().unwrap();
        write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);

        let renamed = rename_obfuscated_members(
            JobId(3),
            root.path(),
            &plan("2c0837e5fa42c8cfb5d5e583168a2af4"),
        )
        .await;

        assert_eq!(renamed, 0);
        assert_eq!(
            delivered_names(root.path()),
            vec!["Yb5drZSkNi20UCMkb.mkv".to_string()]
        );
    }

    #[tokio::test]
    async fn a_disc_structure_delivery_is_never_touched() {
        let root = tempfile::tempdir().unwrap();
        write_file(root.path(), "VIDEO_TS/VTS_01_1.VOB", 64 * MIB);
        write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 32 * MIB);

        let renamed =
            rename_obfuscated_members(JobId(4), root.path(), &plan("Silver Horizon")).await;

        assert_eq!(renamed, 0);
        assert_eq!(
            delivered_names(root.path()),
            vec![
                "VIDEO_TS/VTS_01_1.VOB".to_string(),
                "Yb5drZSkNi20UCMkb.mkv".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn an_unreachable_release_index_falls_through_to_the_job_name() {
        // The endpoint resolves nowhere, which is the same ladder rung as a
        // miss, a timeout or a malformed answer: the job name still wins and
        // the job still completes.
        let root = tempfile::tempdir().unwrap();
        write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);

        let renamed = rename_obfuscated_members(
            JobId(5),
            root.path(),
            &plan_with_srrdb("Silver Horizon 2024", "Yb5drZSkNi20UCMkb.mkv", 0x1234_5678),
        )
        .await;

        assert_eq!(renamed, 1);
        assert_eq!(
            delivered_names(root.path()),
            vec!["Silver Horizon 2024.mkv".to_string()]
        );
    }

    /// The whole rung, against a canned index bound to loopback: nothing leaves
    /// the machine, and the assertion covers what the request carried as well as
    /// what the answer did to the name.
    #[tokio::test]
    async fn a_release_index_hit_outranks_the_job_name() {
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use axum::Router;
        use axum::extract::{Path as AxumPath, State};
        use axum::routing::get;

        #[derive(Clone)]
        struct Index {
            requests: StdArc<std::sync::Mutex<Vec<String>>>,
            calls: StdArc<AtomicUsize>,
        }

        async fn search(State(index): State<Index>, AxumPath(query): AxumPath<String>) -> String {
            index.calls.fetch_add(1, Ordering::SeqCst);
            index.requests.lock().unwrap().push(query);
            r#"{"resultsCount":"1","results":[
                {"release":"Silver.Horizon.2024.1080p.WEB-DL.x264-CREW"}
            ]}"#
            .to_string()
        }

        let index = Index {
            requests: StdArc::new(std::sync::Mutex::new(Vec::new())),
            calls: StdArc::new(AtomicUsize::new(0)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/v1/search/{query}", get(search))
            .with_state(index.clone());
        let served = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let root = tempfile::tempdir().unwrap();
        write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);
        write_file(root.path(), "Yb5drZSkNi20UCMkb.eng.srt", 4096);

        let mut plan = plan_with_srrdb("Quiet Harbour 2021", "Yb5drZSkNi20UCMkb.mkv", 0x0a1b_2c3d);
        plan.srrdb.as_mut().unwrap().base_url = base_url;

        let renamed = rename_obfuscated_members(JobId(8), root.path(), &plan).await;
        served.abort();

        assert_eq!(
            index.requests.lock().unwrap().as_slice(),
            ["archive-crc:0a1b2c3d".to_string()],
            "the request carries the checksum and nothing else"
        );
        assert_eq!(index.calls.load(Ordering::SeqCst), 1, "exactly one attempt");
        assert_eq!(renamed, 2);
        assert_eq!(
            delivered_names(root.path()),
            vec![
                "Silver.Horizon.2024.1080p.WEB-DL.x264-CREW.eng.srt".to_string(),
                "Silver.Horizon.2024.1080p.WEB-DL.x264-CREW.mkv".to_string(),
            ],
            "the release name outranks the job name, and the helper follows the payload"
        );
    }

    #[tokio::test]
    async fn a_job_name_that_is_already_the_member_name_plans_nothing() {
        let root = tempfile::tempdir().unwrap();
        write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);

        let renamed =
            rename_obfuscated_members(JobId(6), root.path(), &plan("Yb5drZSkNi20UCMkb")).await;

        assert_eq!(renamed, 0);
    }

    #[tokio::test]
    async fn weaver_bookkeeping_is_not_part_of_the_delivery() {
        let root = tempfile::tempdir().unwrap();
        write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);
        write_file(root.path(), OUTPUT_DIR_MARKER, 32);
        write_file(root.path(), ".hidden-scratch", 512);

        let renamed =
            rename_obfuscated_members(JobId(7), root.path(), &plan("Silver Horizon")).await;

        assert_eq!(renamed, 1);
        assert!(root.path().join(OUTPUT_DIR_MARKER).is_file());
        assert!(root.path().join(".hidden-scratch").is_file());
    }
}
