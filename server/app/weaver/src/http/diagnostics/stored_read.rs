// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The read of the stand-in table's sources: one step per source, each step
// a run of short statements, and every page of names handed to the shared
// table the moment it is read. A step that fails costs only its own names; a
// read that is stopped leaves every page read so far behind. The read checks
// whether to stop before every statement, so a stopped read ends after the
// statement it is in and frees its connection.
//
// The stored NZBs are the large part. They are sized first, read in small
// batches whose bytes are reserved against a fixed budget before the batch
// is read, and opened by a few workers, each with one zstd context it keeps
// for every NZB it opens.

use std::collections::{BTreeSet, HashMap};
use std::io::{BufReader, Read};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use weaver_nzb::NzbNames;
use weaver_server_core::Database;
use weaver_server_core::jobs::diagnostic_names::{
    DIAGNOSTIC_NAME_PAGE, DiagnosticJobs, DiagnosticNzbSize,
};

use weaver_server_core::SchedulerHandle;
use weaver_server_core::categories::CategoryConfig;
use weaver_server_core::jobs::diagnostic_names::JobNameRecord;
use weaver_server_core::servers::ServerConfig;
use weaver_server_core::settings::model::SharedConfig;

use super::archive::{ManifestStep, ManifestTable};
use super::sanitize::{Guard, StoredNames};

pub(super) const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

// At most this many stored NZBs are read by one statement: a page of ids.
// A statement costs far more than a small NZB, so a batch is cut by bytes
// first.
pub(super) const NZB_BATCH: usize = DIAGNOSTIC_NAME_PAGE;
// At most this many bytes of one batch's stored NZBs.
const NZB_BATCH_BYTES: u64 = 16 * 1024 * 1024;
// The stored NZB bytes the read holds at once: every batch reserves its
// bytes before it is read and frees each NZB's as it is opened. An NZB
// stored larger than this is not read, and the step says so.
pub(super) const NZB_BYTES_IN_FLIGHT: u64 = 64 * 1024 * 1024;
// The buffer each worker reads a decompressed NZB through.
const PARSE_BUFFER: usize = 64 * 1024;

pub(super) const STEP_SERVERS: &str = "servers";
pub(super) const STEP_CATEGORIES: &str = "categories";
pub(super) const STEP_LOGIN: &str = "login";
pub(super) const STEP_SCHEDULES: &str = "schedules";
pub(super) const STEP_PROXIES: &str = "proxy profiles";
pub(super) const STEP_EGRESS: &str = "egress interfaces";
pub(super) const STEP_POOLS: &str = "proxy pools";
pub(super) const STEP_DNS_LISTS: &str = "DNS server lists";
pub(super) const STEP_DNS_PROFILES: &str = "DNS profiles";
pub(super) const STEP_FEEDS: &str = "RSS feeds";
pub(super) const STEP_SCRIPT_DIR: &str = "scripts directory";
pub(super) const STEP_SCRIPT_INSTANCES: &str = "script instances";
pub(super) const STEP_HISTORY_TOTAL: &str = "history total";
pub(super) const STEP_RECENT_HISTORY: &str = "recent history";
pub(super) const STEP_ACTIVE_NAMES: &str = "active job names";
pub(super) const STEP_HISTORY_NAMES: &str = "finished job names";
pub(super) const STEP_ACTIVE_NZBS: &str = "active job NZBs";
pub(super) const STEP_HISTORY_NZBS: &str = "finished job NZBs";

// Every step, in the order the read takes them.
pub(super) const STEPS: [&str; 18] = [
    STEP_SERVERS,
    STEP_CATEGORIES,
    STEP_LOGIN,
    STEP_SCHEDULES,
    STEP_PROXIES,
    STEP_EGRESS,
    STEP_POOLS,
    STEP_DNS_LISTS,
    STEP_DNS_PROFILES,
    STEP_FEEDS,
    STEP_SCRIPT_DIR,
    STEP_SCRIPT_INSTANCES,
    STEP_HISTORY_TOTAL,
    STEP_RECENT_HISTORY,
    STEP_ACTIVE_NAMES,
    STEP_HISTORY_NAMES,
    STEP_ACTIVE_NZBS,
    STEP_HISTORY_NZBS,
];

pub(super) const STATE_COMPLETE: &str = "complete";
pub(super) const STATE_PARTIAL: &str = "partial";
pub(super) const STATE_FAILED: &str = "failed";
pub(super) const STATE_NOT_REACHED: &str = "not_reached";

const REASON_STOPPED: &str = "stopped";
const REASON_READ_FAILED: &str = super::bounded::REASON_READ_FAILED;
const REASON_TOO_LARGE: &str = "too_large";

// Why a step ends before it has read everything, from outside the read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Cut {
    // The read's time is up: this step and every later one end here.
    Stop,
    // This step fails with this error; the read goes on with the next.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "only the test hook fails a step from outside")
    )]
    Fail(&'static str),
}

type CutHook = Box<dyn Fn(&str, u64) -> Option<Cut> + Send + Sync>;

// What the read has gathered so far, shared with the request that waits on
// it.
#[derive(Default)]
pub(super) struct ReadState {
    pub(super) stored: StoredNames,
    pub(super) steps: Vec<ManifestStep>,
    // Jobs whose names page was read.
    pub(super) named: BTreeSet<u64>,
    // Jobs whose stored NZB was sized, those that have one, and those whose
    // NZB was opened.
    pub(super) sized: BTreeSet<u64>,
    pub(super) with_nzb: BTreeSet<u64>,
    pub(super) opened: BTreeSet<u64>,
    // The recent finished jobs the read set out to name.
    pub(super) recent: Vec<u64>,
    pub(super) statements: u64,
    pub(super) rows: u64,
    pub(super) nzb_bytes: u64,
    pub(super) decoded_bytes: u64,
    pub(super) read_ms: u64,
}

impl ReadState {
    // Whether every name the read holds for a job was read: its names page,
    // and its stored NZB when it has one.
    pub(super) fn covers(&self, job_id: u64) -> bool {
        self.named.contains(&job_id)
            && self.sized.contains(&job_id)
            && (!self.with_nzb.contains(&job_id) || self.opened.contains(&job_id))
    }

    fn step(&self, step: &str) -> Option<&ManifestStep> {
        self.steps.iter().find(|entry| entry.step == step)
    }

    // Whether a step read everything it set out to.
    pub(super) fn complete(&self, step: &str) -> bool {
        self.step(step)
            .is_some_and(|entry| entry.state == STATE_COMPLETE)
    }
}

// One read of the table's sources, shared between the blocking task that
// runs it and the request that stops it.
pub(super) struct NameRead {
    stop: AtomicBool,
    state: Mutex<ReadState>,
    cut: Option<CutHook>,
    started: Instant,
}

impl NameRead {
    pub(super) fn new() -> Self {
        Self {
            stop: AtomicBool::new(false),
            state: Mutex::new(ReadState::default()),
            cut: None,
            started: Instant::now(),
        }
    }

    // A read that asks `cut` before each statement of each step, given the
    // step and how many of its statements have run, whether to end it there.
    #[cfg(test)]
    pub(super) fn with_cut(cut: impl Fn(&str, u64) -> Option<Cut> + Send + Sync + 'static) -> Self {
        Self {
            cut: Some(Box::new(cut)),
            ..Self::new()
        }
    }

    // Ends the read after the statement it is in, and takes what it has
    // gathered. A step that never began is reported as not reached.
    pub(super) fn finish(&self) -> ReadState {
        self.stop.store(true, Ordering::SeqCst);
        let mut state = std::mem::take(&mut *self.lock());
        for step in STEPS {
            if state.step(step).is_none() {
                state.steps.push(ManifestStep {
                    step: step.to_string(),
                    state: STATE_NOT_REACHED.to_string(),
                    ..ManifestStep::default()
                });
            }
        }
        state
            .steps
            .sort_by_key(|entry| STEPS.iter().position(|step| *step == entry.step));
        state.read_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        state
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ReadState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    fn check(&self, step: &str, statements: u64) -> Option<Cut> {
        if self.stopped() {
            return Some(Cut::Stop);
        }
        let cut = self.cut.as_ref().and_then(|cut| cut(step, statements));
        if cut == Some(Cut::Stop) {
            self.stop.store(true, Ordering::SeqCst);
        }
        cut
    }

    fn report(
        &self,
        step: &str,
        state: &str,
        read: Option<u64>,
        of: Option<u64>,
        error: Option<String>,
        reason: Option<&str>,
    ) {
        self.lock().steps.push(ManifestStep {
            step: step.to_string(),
            state: state.to_string(),
            read,
            of,
            error,
            reason: reason.map(str::to_string),
        });
    }

    // A step cut before or between its statements: stopped (partial when
    // some ran), or failed with the hook's error.
    fn cut_short(&self, step: &str, cut: Cut, ran: u64, read: Option<u64>, of: Option<u64>) {
        match cut {
            Cut::Stop if ran == 0 => {}
            Cut::Stop => self.report(
                step,
                STATE_PARTIAL,
                read,
                of,
                Some("the read's time ran out".to_string()),
                Some(REASON_STOPPED),
            ),
            Cut::Fail(error) => self.report(
                step,
                if ran == 0 {
                    STATE_FAILED
                } else {
                    STATE_PARTIAL
                },
                read,
                of,
                Some(error.to_string()),
                Some(REASON_READ_FAILED),
            ),
        }
    }

    // A step of one statement: its value, put into the table when it comes.
    // Returns whether the read goes on.
    fn one<T, E: std::fmt::Display>(
        &self,
        step: &'static str,
        fetch: impl FnOnce() -> Result<T, E>,
        put: impl FnOnce(&mut StoredNames, T),
    ) -> bool {
        if let Some(cut) = self.check(step, 0) {
            self.cut_short(step, cut, 0, None, None);
            return cut != Cut::Stop;
        }
        match fetch() {
            Ok(value) => {
                let mut state = self.lock();
                state.statements += 1;
                put(&mut state.stored, value);
                drop(state);
                self.report(step, STATE_COMPLETE, None, None, None, None);
            }
            Err(error) => self.report(
                step,
                STATE_FAILED,
                None,
                None,
                Some(error.to_string()),
                Some(REASON_READ_FAILED),
            ),
        }
        !self.stopped()
    }
}

// Reads every source of the table into `read`, step by step. Blocking: run
// it off the async runtime.
pub(super) fn read_stored_names(db: &Database, read: &NameRead) {
    let _ = read_steps(db, read);
}

fn read_steps(db: &Database, read: &NameRead) -> Option<()> {
    go(read.one(
        STEP_SERVERS,
        || db.list_servers(),
        |stored, servers| {
            stored.servers = servers;
        },
    ))?;
    go(read.one(
        STEP_CATEGORIES,
        || db.list_categories(),
        |stored, value| {
            stored.categories = value;
        },
    ))?;
    go(read.one(
        STEP_LOGIN,
        || db.get_auth_credentials(),
        |stored, value| {
            stored.logins = value
                .map(|credentials| credentials.username)
                .into_iter()
                .collect();
        },
    ))?;
    go(read.one(
        STEP_SCHEDULES,
        || db.list_schedules(),
        |stored, value| {
            stored.schedules = value;
        },
    ))?;
    go(read.one(
        STEP_PROXIES,
        || db.list_proxy_profiles(),
        |stored, value| {
            stored.proxies = value;
        },
    ))?;
    go(read.one(
        STEP_EGRESS,
        || db.list_egress_interfaces(),
        |stored, value| {
            stored.egress = value;
        },
    ))?;
    go(read.one(
        STEP_POOLS,
        || db.list_proxy_pools(),
        |stored, value| {
            stored.pools = value;
        },
    ))?;
    go(read.one(
        STEP_DNS_LISTS,
        || db.list_dns_server_lists(),
        |stored, value| {
            stored.dns_lists = value;
        },
    ))?;
    go(read.one(
        STEP_DNS_PROFILES,
        || db.list_dns_profiles(),
        |stored, value| {
            stored.dns_profiles = value;
        },
    ))?;
    go(read.one(
        STEP_FEEDS,
        || db.list_rss_feeds(),
        |stored, value| {
            stored.feeds = value;
        },
    ))?;
    go(read.one(
        STEP_SCRIPT_DIR,
        || script_directory(db),
        |stored, (dir, files)| {
            stored.script_dirs = dir.into_iter().collect();
            stored.scripts.extend(files);
        },
    ))?;
    go(read.one(
        STEP_SCRIPT_INSTANCES,
        || db.script_instances(),
        |stored, instances| {
            for instance in instances {
                stored.scripts.push(instance.script.as_str().to_string());
                stored.scripts.push(instance.name);
            }
        },
    ))?;
    go(read.one(
        STEP_HISTORY_TOTAL,
        || db.diagnostic_history_total(),
        |stored, total| {
            stored.history_total = total;
        },
    ))?;
    let mut recent = Vec::new();
    go(read.one(
        STEP_RECENT_HISTORY,
        || db.diagnostic_recent_history_ids(),
        |_, ids| {
            recent = ids;
        },
    ))?;
    recent.sort_unstable();
    read.lock().recent = recent.clone();

    let active = active_names(db, read)?;
    let mut ran = 0;
    let of = Some(recent.len() as u64);
    let mut named = 0u64;
    let mut ended = false;
    for chunk in recent.chunks(DIAGNOSTIC_NAME_PAGE) {
        if let Some(cut) = read.check(STEP_HISTORY_NAMES, ran) {
            read.cut_short(STEP_HISTORY_NAMES, cut, ran, Some(named), of);
            go(cut != Cut::Stop)?;
            ended = true;
            break;
        }
        match db.diagnostic_job_names_page(DiagnosticJobs::History(chunk)) {
            Ok(page) => {
                ran += page.statements.len() as u64;
                named += chunk.len() as u64;
                let mut state = read.lock();
                state.statements += page.statements.len() as u64;
                state.rows += page.rows;
                state.named.extend(chunk.iter().copied());
                state.stored.jobs.extend(page.jobs);
            }
            Err(error) => {
                read.report(
                    STEP_HISTORY_NAMES,
                    if named == 0 {
                        STATE_FAILED
                    } else {
                        STATE_PARTIAL
                    },
                    Some(named),
                    of,
                    Some(error.to_string()),
                    Some(REASON_READ_FAILED),
                );
                ended = true;
                break;
            }
        }
    }
    if !ended {
        read.report(
            STEP_HISTORY_NAMES,
            STATE_COMPLETE,
            Some(named),
            of,
            None,
            None,
        );
    }
    go(!read.stopped())?;

    go(posted(db, read, STEP_ACTIVE_NZBS, &active))?;
    go(posted(db, read, STEP_HISTORY_NZBS, &recent))?;
    Some(())
}

fn go(on: bool) -> Option<()> {
    on.then_some(())
}

// The scripts directory and every script file in it.
fn script_directory(db: &Database) -> Result<(Option<String>, Vec<String>), String> {
    let dir = db
        .diagnostic_script_directory()
        .map_err(|error| error.to_string())?;
    let mut scripts = Vec::new();
    if let Some(dir) = &dir {
        match std::fs::read_dir(dir) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(|error| error.to_string())?;
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if !name.starts_with('.') {
                        scripts.push(name);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok((dir, scripts))
}

// Every active job's names, a page at a time. Returns the active job ids the
// pages covered, for their NZBs, or `None` when the read was stopped.
fn active_names(db: &Database, read: &NameRead) -> Option<Vec<u64>> {
    let mut ids = Vec::new();
    let mut after = 0;
    let mut ran = 0;
    loop {
        if let Some(cut) = read.check(STEP_ACTIVE_NAMES, ran) {
            read.cut_short(STEP_ACTIVE_NAMES, cut, ran, Some(ids.len() as u64), None);
            go(cut != Cut::Stop)?;
            return Some(ids);
        }
        match db.diagnostic_job_names_page(DiagnosticJobs::ActiveAfter(after)) {
            Ok(page) => {
                ran += page.statements.len() as u64;
                ids.extend(page.active_ids.iter().copied());
                let mut state = read.lock();
                state.statements += page.statements.len() as u64;
                state.rows += page.rows;
                state.named.extend(page.active_ids.iter().copied());
                state.stored.jobs.extend(page.jobs);
                drop(state);
                match page.last_active {
                    Some(last) => after = last,
                    None => {
                        read.report(
                            STEP_ACTIVE_NAMES,
                            STATE_COMPLETE,
                            Some(ids.len() as u64),
                            Some(ids.len() as u64),
                            None,
                            None,
                        );
                        return Some(ids);
                    }
                }
            }
            Err(error) => {
                read.report(
                    STEP_ACTIVE_NAMES,
                    if ids.is_empty() {
                        STATE_FAILED
                    } else {
                        STATE_PARTIAL
                    },
                    Some(ids.len() as u64),
                    None,
                    Some(error.to_string()),
                    Some(REASON_READ_FAILED),
                );
                return Some(ids);
            }
        }
    }
}

// The bytes of stored NZBs read and not yet opened, under a fixed budget:
// a batch reserves its bytes before it is read, and each NZB frees its own
// once opened.
struct Budget {
    held: Mutex<u64>,
    freed: Condvar,
}

impl Budget {
    fn reserve(&self, bytes: u64) {
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *held + bytes > NZB_BYTES_IN_FLIGHT {
            held = self
                .freed
                .wait(held)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        *held += bytes;
    }

    fn free(&self, bytes: u64) {
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *held = held.saturating_sub(bytes);
        self.freed.notify_all();
    }
}

// How many workers open NZBs: a quarter of the cores, one to four, so the
// read never takes the cores downloads decode on.
pub(super) fn parse_workers() -> usize {
    std::thread::available_parallelism().map_or(1, |cores| (cores.get() / 4).clamp(1, 4))
}

// The NZBs of `ids` into the table: sized a chunk at a time, read in
// batches under the byte budget, opened by the workers. Returns whether the
// read goes on.
fn posted(db: &Database, read: &NameRead, step: &'static str, ids: &[u64]) -> bool {
    let budget = Budget {
        held: Mutex::new(0),
        freed: Condvar::new(),
    };
    let workers = parse_workers();
    let (send, receive) = mpsc::sync_channel::<(u64, Vec<u8>, u64)>(workers);
    let receive = Mutex::new(receive);
    let opened = AtomicU64::new(0);
    let failures: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let mut total = 0u64;
    let mut too_large = 0u64;
    let mut ran = 0u64;
    let mut ended: Option<End> = None;

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                let mut context = zstd::zstd_safe::DCtx::create();
                loop {
                    let next = receive
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .recv();
                    let Ok((job_id, nzb, reserved)) = next else {
                        break;
                    };
                    if !read.stopped() {
                        match posted_names_with(&nzb, &mut context) {
                            Ok((names, decoded)) => {
                                opened.fetch_add(1, Ordering::Relaxed);
                                let mut state = read.lock();
                                state.decoded_bytes += decoded;
                                state.opened.insert(job_id);
                                state.stored.posted.insert(job_id, names);
                            }
                            Err(error) => failures
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .push(format!("the NZB of job {job_id}: {error}")),
                        }
                    }
                    drop(nzb);
                    budget.free(reserved);
                }
            });
        }

        'chunks: for chunk in ids.chunks(DIAGNOSTIC_NAME_PAGE) {
            if let Some(cut) = read.check(step, ran) {
                ended = Some(End::Cut(cut));
                break;
            }
            let sizes = match db.diagnostic_nzb_sizes(chunk) {
                Ok(sizes) => sizes,
                Err(error) => {
                    ended = Some(End::Failed(error.to_string()));
                    break;
                }
            };
            ran += 1;
            total += sizes.len() as u64;
            {
                let mut state = read.lock();
                state.statements += 1;
                state.rows += sizes.len() as u64;
                state.sized.extend(chunk.iter().copied());
                state.with_nzb.extend(sizes.iter().map(|size| size.job_id));
            }
            for batch in batches(&sizes) {
                if let Some(cut) = read.check(step, ran) {
                    ended = Some(End::Cut(cut));
                    break 'chunks;
                }
                if batch.len() == 1 && batch[0].bytes > NZB_BYTES_IN_FLIGHT {
                    too_large += 1;
                    continue;
                }
                let bytes: u64 = batch.iter().map(|size| size.bytes).sum();
                budget.reserve(bytes);
                let batch_ids: Vec<u64> = batch.iter().map(|size| size.job_id).collect();
                let nzbs = match db.diagnostic_nzbs(&batch_ids) {
                    Ok(nzbs) => nzbs,
                    Err(error) => {
                        budget.free(bytes);
                        ended = Some(End::Failed(error.to_string()));
                        break 'chunks;
                    }
                };
                ran += 1;
                {
                    let mut state = read.lock();
                    state.statements += 1;
                    state.rows += nzbs.len() as u64;
                    state.nzb_bytes += nzbs.iter().map(|(_, nzb)| nzb.len() as u64).sum::<u64>();
                }
                let sized: HashMap<u64, u64> =
                    batch.iter().map(|size| (size.job_id, size.bytes)).collect();
                let mut reserved = bytes;
                for (job_id, nzb) in nzbs {
                    let own = sized.get(&job_id).copied().unwrap_or(0).min(reserved);
                    reserved -= own;
                    if send.send((job_id, nzb, own)).is_err() {
                        break;
                    }
                }
                budget.free(reserved);
            }
        }
        drop(send);
    });

    let opened = opened.load(Ordering::Relaxed);
    let failures = failures
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let of = Some(total);
    match ended {
        Some(End::Failed(error)) => {
            read.report(
                step,
                if opened == 0 {
                    STATE_FAILED
                } else {
                    STATE_PARTIAL
                },
                Some(opened),
                of,
                Some(error),
                Some(REASON_READ_FAILED),
            );
            !read.stopped()
        }
        Some(End::Cut(cut)) => {
            read.cut_short(step, cut, ran, Some(opened), of);
            cut != Cut::Stop
        }
        None if failures.is_empty() && too_large == 0 && opened == total => {
            read.report(step, STATE_COMPLETE, Some(opened), of, None, None);
            true
        }
        None => {
            let mut errors = Vec::new();
            if too_large > 0 {
                errors.push(format!(
                    "{too_large} stored NZBs are larger than the {} MiB the read holds at once",
                    NZB_BYTES_IN_FLIGHT / (1024 * 1024)
                ));
            }
            if let Some(first) = failures.first() {
                errors.push(format!(
                    "{} stored NZBs could not be opened, the first {first}",
                    failures.len()
                ));
            }
            read.report(
                step,
                STATE_PARTIAL,
                Some(opened),
                of,
                Some(errors.join("; ")),
                Some(if failures.is_empty() {
                    REASON_TOO_LARGE
                } else {
                    REASON_READ_FAILED
                }),
            );
            !read.stopped()
        }
    }
}

// Why the NZB step ended before its last batch.
enum End {
    Cut(Cut),
    Failed(String),
}

// Consecutive stored NZBs, at most [`NZB_BATCH`] of them and
// [`NZB_BATCH_BYTES`] together; one larger than that is a batch of its own.
fn batches(sizes: &[DiagnosticNzbSize]) -> Vec<&[DiagnosticNzbSize]> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut bytes = 0u64;
    for (index, size) in sizes.iter().enumerate() {
        if index > start && (index - start == NZB_BATCH || bytes + size.bytes > NZB_BATCH_BYTES) {
            out.push(&sizes[start..index]);
            start = index;
            bytes = 0;
        }
        bytes += size.bytes;
    }
    if start < sizes.len() {
        out.push(&sizes[start..]);
    }
    out
}

// Counts the bytes a reader hands out.
struct Counted<R> {
    inner: R,
    bytes: u64,
}

impl<R: Read> Read for Counted<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.bytes += read as u64;
        Ok(read)
    }
}

// The title and file names of a stored NZB, read without its segments, its
// zstd stream opened with `context`, which is reset for it; and how many
// bytes it decompressed to.
pub(super) fn posted_names_with(
    nzb: &[u8],
    context: &mut zstd::zstd_safe::DCtx<'static>,
) -> Result<(NzbNames, u64), String> {
    if nzb.starts_with(&ZSTD_MAGIC) {
        let mut reader = BufReader::with_capacity(
            PARSE_BUFFER,
            Counted {
                inner: zstd::stream::read::Decoder::with_context(nzb, context),
                bytes: 0,
            },
        );
        let names =
            weaver_nzb::parse_nzb_names_reader(&mut reader).map_err(|error| error.to_string())?;
        Ok((names, reader.get_ref().bytes))
    } else {
        let names = weaver_nzb::parse_nzb_names_reader(nzb).map_err(|error| error.to_string())?;
        Ok((names, nzb.len() as u64))
    }
}

// The names Weaver holds in memory, for what the read did not reach: every
// queued job and every finished job the queue publishes, from the queue's
// shared rows, and the servers and categories of the loaded configuration.
#[derive(Default)]
pub(super) struct MemoryNames {
    pub(super) queue: Vec<(u64, JobNameRecord)>,
    pub(super) history: Vec<(u64, JobNameRecord)>,
    pub(super) servers: Vec<ServerConfig>,
    pub(super) categories: Vec<CategoryConfig>,
}

impl MemoryNames {
    // Reads the queue's shared rows and the loaded configuration; nothing
    // waits on the pipeline.
    pub(super) async fn current(handle: &SchedulerHandle, config: &SharedConfig) -> Self {
        let (servers, categories) = {
            let config = config.read().await;
            (config.servers.clone(), config.categories.clone())
        };
        Self {
            queue: handle.diagnostic_queue_names(),
            history: handle
                .diagnostic_history_rows()
                .iter()
                .map(|job| (job.job_id.0, JobNameRecord::of_info(job)))
                .collect(),
            servers,
            categories,
        }
    }
}

// The names the table is built from, what the read did not cover, and what
// the manifest says about it.
pub(super) struct TableNames {
    pub(super) stored: StoredNames,
    pub(super) guard: Option<Guard>,
    pub(super) manifest: ManifestTable,
    pub(super) history_total: Option<u64>,
}

impl TableNames {
    // A table read whole.
    #[cfg(test)]
    pub(super) fn whole(stored: StoredNames) -> Self {
        let history_total = Some(stored.history_total);
        Self {
            stored,
            guard: None,
            manifest: ManifestTable {
                complete: true,
                ..ManifestTable::default()
            },
            history_total,
        }
    }

    // What the read gathered, with memory covering what it did not reach:
    // a job the read did not name gets the names memory holds for it, and a
    // server or category step that did not finish takes the loaded
    // configuration's. Every job whose names were not all read is under
    // the guard. A read that covered everything uses no memory at all, so
    // its table is the one the read alone builds.
    pub(super) fn from_read(mut state: ReadState, memory: MemoryNames) -> Self {
        let mut stored = std::mem::take(&mut state.stored);
        let mut unread = BTreeSet::new();
        let mut from_memory = 0u64;
        let history_read =
            state.complete(STEP_RECENT_HISTORY) && state.complete(STEP_HISTORY_NAMES);
        let history = if history_read {
            Vec::new()
        } else {
            memory.history
        };
        for (id, record) in memory.queue.into_iter().chain(history) {
            if state.covers(id) {
                continue;
            }
            unread.insert(id);
            if let std::collections::btree_map::Entry::Vacant(entry) = stored.jobs.entry(id) {
                entry.insert(record);
                from_memory += 1;
            }
        }
        for id in state.recent.iter().chain(stored.jobs.keys()) {
            if !state.covers(*id) {
                unread.insert(*id);
            }
        }
        let mut fallback = Vec::new();
        if from_memory > 0 {
            fallback.push(format!(
                "{from_memory} jobs were named from the queue in memory"
            ));
        }
        if !state.complete(STEP_SERVERS) {
            stored.servers = memory.servers;
            fallback.push("the servers came from the loaded configuration".to_string());
        }
        if !state.complete(STEP_CATEGORIES) {
            stored.categories = memory.categories;
            fallback.push("the categories came from the loaded configuration".to_string());
        }
        let steps_complete = state.steps.iter().all(|step| step.state == STATE_COMPLETE);
        let complete = steps_complete && unread.is_empty();
        if !complete {
            fallback.push(format!(
                "{} jobs were not read whole; a line or value that names one of them, or \
                 holds a dotted name the table does not hold, is left out in place",
                unread.len()
            ));
        }
        let history_total = state
            .complete(STEP_HISTORY_TOTAL)
            .then_some(stored.history_total);
        Self {
            stored,
            guard: (!complete).then(|| Guard {
                jobs: Some(unread.clone()),
            }),
            manifest: ManifestTable {
                complete,
                steps: std::mem::take(&mut state.steps),
                names_from_table: 0,
                names_from_fallback: 0,
                jobs_from_memory: from_memory,
                jobs_not_fully_read: unread.len() as u64,
                read_ms: state.read_ms,
                fallback: (!fallback.is_empty()).then(|| fallback.join("; ")),
            },
            history_total,
        }
    }
}
