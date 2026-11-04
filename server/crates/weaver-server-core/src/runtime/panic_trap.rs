// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a panic leaves behind for the job that caused it.
//
// Weaver runs with `panic = "abort"`, so a panic is never caught: the process
// dies where it stands. When the panic came out of one job's work, a post
// whose bytes break an assumption, the restart restores that same job and
// walks it straight back into the same panic, on every start, with nothing
// in the log saying which job it is.
//
// SABnzbd catches an exception in a job's post-processing and fails that job
// with the message (sabnzbd/postproc.py:668-676); its assembler logs the
// exception and stops (sabnzbd/assembler.py:365-367). NZBGet has no per-job
// isolation and only enables a core dump (daemon/main/StackTrace.cpp:252-258).
// Weaver cannot catch under abort, so it remembers across the restart
// instead:
//
//   - while a thread works for a job, the job is named in a thread-local,
//     set by `JobScope` and cleared when it drops;
//   - the panic hook writes `panics/<job id>` in the data folder, or
//     `panics/daemon` when no job was named, holding the message, where it
//     was raised, the build and the time;
//   - the next start reads the markers before any job is restored. A marked
//     job is installed paused with the reason, its checkpoint untouched, and
//     nothing of it restored. Its marker stays until the job is resumed or
//     deleted, so every start before then parks it again without walking
//     into its restore. A resume removes the marker first and replays the
//     job as usual; should it panic again, a fresh marker parks it again.
//     The worst is one crash per resume, never a loop.
//
// Paused, not failed: a panic is a fault in Weaver, and says nothing about
// the post.
//
// The hook may run with the process in a poor state, so writing a marker is
// plain synchronous file work: no runtime, no database, no lock beyond the
// file system's.

use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::io::Write;
use std::marker::PhantomData;
use std::panic::Location;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::jobs::ids::JobId;

// The directory under the data folder the markers live in.
pub const MARKER_DIR: &str = "panics";
// The marker of a panic raised outside any job's work.
const DAEMON_MARKER: &str = "daemon";
// The message a marker states when its write was cut short before it.
const NO_MESSAGE: &str = "<no panic message recorded>";

thread_local! {
    static CURRENT_JOB: Cell<Option<JobId>> = const { Cell::new(None) };
}

// The job this thread works for now, if any.
pub fn current_job() -> Option<JobId> {
    // A thread being torn down has no locals left: it works for no job.
    CURRENT_JOB.try_with(Cell::get).ok().flatten()
}

// Names `job` as the one this thread works for until the scope drops, which
// puts back whatever was named before, so scopes nest. Bound to the thread
// that made it: it never crosses an `.await`, where the task may resume on
// another thread and another task may run on this one. Async work is named
// with `in_job`, which sets the scope for each poll instead.
#[must_use = "the job is named only while the scope lives"]
pub struct JobScope {
    previous: Option<JobId>,
    _thread: PhantomData<*const ()>,
}

impl JobScope {
    pub fn enter(job: JobId) -> Self {
        Self::carry(Some(job))
    }

    // Names whatever job another thread worked for when it handed this one
    // its work, `None` included: work handed over outside any job stays
    // outside one.
    pub fn carry(job: Option<JobId>) -> Self {
        let previous = CURRENT_JOB.replace(job);
        Self {
            previous,
            _thread: PhantomData,
        }
    }
}

impl Drop for JobScope {
    fn drop(&mut self) {
        CURRENT_JOB.set(self.previous);
    }
}

// Runs `future` with `job` named for every poll of it, and only then.
pub async fn in_job<F: Future>(job: JobId, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        let _job = JobScope::enter(job);
        future.as_mut().poll(cx)
    })
    .await
}

// `work`, to run on another thread working for the job this one works for
// now: work handed to a blocking thread or a pool keeps its job's name.
pub fn carried<R>(work: impl FnOnce() -> R) -> impl FnOnce() -> R {
    let job = current_job();
    move || {
        let _job = JobScope::carry(job);
        work()
    }
}

// `tokio::task::spawn_blocking`, the blocking thread working for the job
// this one works for now.
pub fn spawn_blocking<R: Send + 'static>(
    work: impl FnOnce() -> R + Send + 'static,
) -> tokio::task::JoinHandle<R> {
    tokio::task::spawn_blocking(carried(work))
}

// A panic's payload as text, as the standard hook renders it.
pub fn payload_text(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("<non-string panic payload>")
}

// Where this process writes its markers, and the build it states in them.
#[derive(Debug)]
pub struct Trap {
    dir: PathBuf,
    version: &'static str,
}

static ARMED: OnceLock<Trap> = OnceLock::new();

// Sets where the panic hook writes its markers, once the data folder is
// known. Before then a panic leaves no marker; nothing has been restored
// that could loop. The first call wins.
pub fn arm(data_dir: &Path, version: &'static str) {
    let _ = ARMED.set(Trap::new(data_dir, version));
}

pub fn armed() -> Option<&'static Trap> {
    ARMED.get()
}

impl Trap {
    pub fn new(data_dir: &Path, version: &'static str) -> Self {
        Self {
            dir: data_dir.join(MARKER_DIR),
            version,
        }
    }

    // Writes the marker of the panic `info` describes, for the job this
    // thread works for. Called from the panic hook.
    pub fn record(&self, info: &std::panic::PanicHookInfo<'_>) -> std::io::Result<PathBuf> {
        self.write(current_job(), payload_text(info.payload()), info.location())
    }

    // Writes `panics/<job>`, or `panics/daemon` with no job: the build, the
    // time, where the panic was raised, then the message, last because it
    // may run over lines. Synced before it returns, as the process is about
    // to end.
    pub fn write(
        &self,
        job: Option<JobId>,
        message: &str,
        location: Option<&Location<'_>>,
    ) -> std::io::Result<PathBuf> {
        std::fs::create_dir_all(&self.dir)?;
        let path = self.dir.join(marker_name(job));
        let mut file = std::fs::File::create(&path)?;
        let mut text = format!(
            "version: {}\ntime: {}\n",
            self.version,
            chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, false)
        );
        if let Some(location) = location {
            text.push_str(&format!(
                "location: {}:{}:{}\n",
                location.file(),
                location.line(),
                location.column()
            ));
        }
        text.push_str("message: ");
        text.push_str(message);
        text.push('\n');
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        Ok(path)
    }
}

fn marker_name(job: Option<JobId>) -> String {
    job.map_or_else(|| DAEMON_MARKER.to_owned(), |job| job.0.to_string())
}

// One marker read back at start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mark {
    // The job whose work panicked; `None` for a panic outside any job.
    pub job: Option<JobId>,
    pub message: String,
    pub location: Option<String>,
    pub version: Option<String>,
    pub time: Option<String>,
    path: PathBuf,
}

impl Mark {
    // What the job's queue entry says while it stands parked.
    pub fn reason(&self) -> String {
        format!("crashed the daemon: {}", self.message)
    }

    // Removes the marker: what it asked for is done.
    pub fn consume(&self) {
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %self.path.display(),
                %error,
                "a crash marker could not be removed; the next start reads it again"
            );
        }
    }

    fn parse(job: Option<JobId>, text: &str, path: PathBuf) -> Self {
        let mut mark = Self {
            job,
            message: NO_MESSAGE.to_owned(),
            location: None,
            version: None,
            time: None,
            path,
        };
        let mut rest = text;
        while !rest.is_empty() {
            if let Some(message) = rest.strip_prefix("message: ") {
                mark.message = message.trim_end().to_owned();
                break;
            }
            let (line, after) = rest.split_once('\n').unwrap_or((rest, ""));
            rest = after;
            let Some((key, value)) = line.split_once(": ") else {
                continue;
            };
            let value = Some(value.to_owned());
            match key {
                "location" => mark.location = value,
                "version" => mark.version = value,
                "time" => mark.time = value,
                _ => {}
            }
        }
        mark
    }
}

// Every marker in the data folder. A file not named for a job or the daemon
// is not a marker and is left alone.
pub fn read_marks(data_dir: &Path) -> Vec<Mark> {
    let dir = data_dir.join(MARKER_DIR);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut marks = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let job = match name {
            DAEMON_MARKER => None,
            id => match id.parse::<u64>() {
                Ok(id) => Some(JobId(id)),
                Err(_) => continue,
            },
        };
        let path = entry.path();
        match std::fs::read(&path) {
            Ok(bytes) => marks.push(Mark::parse(job, &String::from_utf8_lossy(&bytes), path)),
            Err(error) => tracing::warn!(
                path = %path.display(),
                %error,
                "a crash marker could not be read"
            ),
        }
    }
    marks.sort_by_key(|mark| mark.job);
    marks
}

// The markers of jobs that crashed the daemon and are queued: each parks its
// job at every install until a resume takes it.
#[derive(Debug, Default)]
pub struct CrashMarks(HashMap<JobId, Mark>);

impl CrashMarks {
    // The marker of `job`, if it crashed the daemon: its install parks it.
    pub fn get(&self, job: JobId) -> Option<&Mark> {
        self.0.get(&job)
    }

    // The marker of `job`, taken: the resume that takes it removes it.
    pub fn take(&mut self, job: JobId) -> Option<Mark> {
        self.0.remove(&job)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

// Reads the markers at start, before any job is restored. A daemon marker is
// logged and removed. A job's marker waits for the job's install when
// `restores` says the job is restored, and is otherwise logged and removed:
// the job is gone, or finished.
pub fn boot_marks(data_dir: &Path, restores: impl Fn(JobId) -> bool) -> CrashMarks {
    let mut waiting = HashMap::new();
    for mark in read_marks(data_dir) {
        match mark.job {
            None => {
                tracing::error!(
                    panic.message = %mark.message,
                    panic.location = mark.location.as_deref(),
                    panic.version = mark.version.as_deref(),
                    panic.time = mark.time.as_deref(),
                    "the daemon crashed on its last run outside any job's work"
                );
                mark.consume();
            }
            Some(job) if !restores(job) => {
                tracing::info!(
                    job_id = job.0,
                    panic.message = %mark.message,
                    "a crash marker names a job that is no longer queued; removed"
                );
                mark.consume();
            }
            Some(job) => {
                tracing::error!(
                    job_id = job.0,
                    panic.message = %mark.message,
                    panic.location = mark.location.as_deref(),
                    panic.version = mark.version.as_deref(),
                    panic.time = mark.time.as_deref(),
                    "a job crashed the daemon; it stays paused until resumed"
                );
                waiting.insert(job, mark);
            }
        }
    }
    CrashMarks(waiting)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scoped_early(job: JobId, leave: bool) -> Option<JobId> {
        let _job = JobScope::enter(job);
        if leave {
            return current_job();
        }
        current_job()
    }

    #[test]
    fn a_scope_names_the_job_for_its_work_and_clears_on_every_way_out() {
        assert_eq!(current_job(), None);
        assert_eq!(scoped_early(JobId(7), true), Some(JobId(7)));
        assert_eq!(current_job(), None);
        assert_eq!(scoped_early(JobId(8), false), Some(JobId(8)));
        assert_eq!(current_job(), None);
        {
            let _outer = JobScope::enter(JobId(1));
            {
                let _inner = JobScope::enter(JobId(2));
                assert_eq!(current_job(), Some(JobId(2)));
                let _none = JobScope::carry(None);
                assert_eq!(current_job(), None);
            }
            assert_eq!(current_job(), Some(JobId(1)));
        }
        assert_eq!(current_job(), None);
    }

    #[test]
    fn a_scope_unwinding_out_of_a_panic_clears_the_job() {
        let caught = std::panic::catch_unwind(|| {
            let _job = JobScope::enter(JobId(3));
            std::panic::resume_unwind(Box::new("out"));
        });
        assert!(caught.is_err());
        assert_eq!(current_job(), None);
    }

    // The job is named while the future is polled, and not while it waits:
    // the thread is free for other work between polls.
    #[tokio::test]
    async fn async_work_names_its_job_only_while_polled() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let work = tokio::spawn(in_job(JobId(5), async move {
            let before = current_job();
            rx.await.unwrap();
            (before, current_job())
        }));
        tokio::task::yield_now().await;
        assert_eq!(current_job(), None);
        tx.send(()).unwrap();
        assert_eq!(work.await.unwrap(), (Some(JobId(5)), Some(JobId(5))));
        assert_eq!(current_job(), None);
    }

    // A blocking thread handed work inside a job's work names that job while
    // it runs it; one handed work outside any job names none.
    #[tokio::test]
    async fn a_blocking_thread_works_for_the_job_that_handed_it_work() {
        let inside = in_job(JobId(8), async {
            spawn_blocking(current_job).await.unwrap()
        })
        .await;
        assert_eq!(inside, Some(JobId(8)));
        assert_eq!(spawn_blocking(current_job).await.unwrap(), None);
    }

    #[test]
    fn a_marker_holds_the_message_the_build_and_the_time() {
        let temp = tempfile::tempdir().unwrap();
        let trap = Trap::new(temp.path(), "9.9.9");
        let location = Location::caller();
        let path = trap
            .write(
                Some(JobId(42)),
                "inverted byte range 20..10",
                Some(location),
            )
            .unwrap();
        assert_eq!(path, temp.path().join("panics").join("42"));
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("version: 9.9.9\n"), "{text}");
        assert!(text.contains("\ntime: 20"), "{text}");
        assert!(
            text.contains(&format!(
                "location: {}:{}:{}\n",
                location.file(),
                location.line(),
                location.column()
            )),
            "{text}"
        );
        assert!(
            text.ends_with("message: inverted byte range 20..10\n"),
            "{text}"
        );

        let daemon = trap.write(None, "no job", None).unwrap();
        assert_eq!(daemon, temp.path().join("panics").join("daemon"));

        let marks = read_marks(temp.path());
        assert_eq!(marks.len(), 2);
        assert_eq!(marks[0].job, None);
        assert_eq!(marks[0].message, "no job");
        assert_eq!(marks[1].job, Some(JobId(42)));
        assert_eq!(marks[1].message, "inverted byte range 20..10");
        assert_eq!(marks[1].version.as_deref(), Some("9.9.9"));
        assert!(marks[1].time.is_some());
        assert_eq!(
            marks[1].reason(),
            "crashed the daemon: inverted byte range 20..10"
        );
    }

    #[test]
    fn a_multi_line_message_and_a_cut_short_marker_read_back() {
        let temp = tempfile::tempdir().unwrap();
        let trap = Trap::new(temp.path(), "1.0.0");
        trap.write(Some(JobId(1)), "first\nsecond", None).unwrap();
        std::fs::write(temp.path().join("panics").join("2"), "version: 1.0.0\nti").unwrap();
        std::fs::write(temp.path().join("panics").join("notes.txt"), "x").unwrap();
        let marks = read_marks(temp.path());
        assert_eq!(marks.len(), 2);
        assert_eq!(marks[0].message, "first\nsecond");
        assert_eq!(marks[1].job, Some(JobId(2)));
        assert_eq!(marks[1].message, NO_MESSAGE);
    }

    #[test]
    fn at_start_only_the_markers_of_restored_jobs_wait() {
        let temp = tempfile::tempdir().unwrap();
        let trap = Trap::new(temp.path(), "1.0.0");
        trap.write(Some(JobId(10)), "restored", None).unwrap();
        trap.write(Some(JobId(11)), "gone", None).unwrap();
        trap.write(None, "outside", None).unwrap();
        let mut marks = boot_marks(temp.path(), |job| job == JobId(10));
        let dir = temp.path().join("panics");
        assert!(dir.join("10").exists());
        assert!(!dir.join("11").exists());
        assert!(!dir.join("daemon").exists());
        assert!(marks.take(JobId(11)).is_none());
        let waiting = marks.take(JobId(10)).unwrap();
        assert_eq!(waiting.message, "restored");
        assert!(marks.is_empty());
        waiting.consume();
        assert!(!dir.join("10").exists());
    }

    // The hook is process-wide; this test owns it while it runs, and hands
    // every panic but its own to the hook it replaced.
    static HOOK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn a_real_panic_inside_a_jobs_work_leaves_its_marker() {
        let _hook = HOOK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = tempfile::tempdir().unwrap();
        let trap = std::sync::Arc::new(Trap::new(temp.path(), "2.0.0"));
        let here = std::thread::current().id();
        let previous = std::sync::Arc::new(std::panic::take_hook());
        {
            let trap = std::sync::Arc::clone(&trap);
            let previous = std::sync::Arc::clone(&previous);
            std::panic::set_hook(Box::new(move |info| {
                if std::thread::current().id() == here {
                    trap.record(info).unwrap();
                } else {
                    previous(info);
                }
            }));
        }
        let caught = std::panic::catch_unwind(|| {
            let _job = JobScope::enter(JobId(77));
            panic!("assert in a job's work: {}", 3);
        });
        let _ = std::panic::take_hook();
        let previous = std::sync::Arc::try_unwrap(previous).unwrap_or_else(|_| unreachable!());
        std::panic::set_hook(previous);

        assert!(caught.is_err());
        assert_eq!(current_job(), None);
        let marks = read_marks(temp.path());
        assert_eq!(marks.len(), 1);
        assert_eq!(marks[0].job, Some(JobId(77)));
        assert_eq!(marks[0].message, "assert in a job's work: 3");
        assert_eq!(marks[0].version.as_deref(), Some("2.0.0"));
        assert!(
            marks[0]
                .location
                .as_deref()
                .is_some_and(|location| location.contains("panic_trap.rs")),
            "{:?}",
            marks[0].location
        );
    }
}
