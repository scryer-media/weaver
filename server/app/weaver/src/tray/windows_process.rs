// Process plumbing for the Windows tray.
//
// Three things live here, all of them about processes rather than windows:
//
// - the kill-on-close job the tray starts the server in, so a tray that dies
//   or is killed cannot leave its server running and holding the installed
//   binaries open;
// - finding the Weaver processes the tray did not start: the one listening on
//   the tray's port, and every one running from the install directory;
// - stopping a process graceful-first: a console interrupt, a bounded wait,
//   then `TerminateProcess`.
//
// An upgrade is the reason all three exist. msiexec cannot replace a file a
// running process holds open; when one does, it raises a files-in-use prompt
// that a passive install hides, and the upgrade looks like it did nothing.

use std::os::windows::io::AsRawHandle;
use std::path::Path;
use std::process::Child;
use std::ptr;
use std::thread;
use std::time::{Duration, Instant};

use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_INSUFFICIENT_BUFFER, GetLastError, HANDLE, INVALID_HANDLE_VALUE,
    WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::NetworkManagement::IpHelper::{
    GetExtendedTcpTable, MIB_TCP6ROW_OWNER_PID, MIB_TCPROW_OWNER_PID, TCP_TABLE_OWNER_PID_LISTENER,
};
use windows_sys::Win32::System::Console::{
    AttachConsole, CTRL_C_EVENT, FreeConsole, GenerateConsoleCtrlEvent, SetConsoleCtrlHandler,
};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW, TH32CS_SNAPPROCESS,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_BREAKAWAY_OK,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcessId, OpenProcess, PROCESS_NAME_WIN32, PROCESS_QUERY_LIMITED_INFORMATION,
    PROCESS_SYNCHRONIZE, PROCESS_TERMINATE, QueryFullProcessImageNameW, TerminateProcess,
    WaitForSingleObject,
};

use super::shared;

// `AF_INET` and `AF_INET6`, which `GetExtendedTcpTable` takes as a plain
// integer. Named here rather than pulled from the WinSock bindings, which
// would add a feature for two numbers.
const AF_INET: u32 = 2;
const AF_INET6: u32 = 23;

// The exit code a forced stop leaves behind, so the server's stderr capture
// and any supervisor can tell it from a crash.
const FORCED_EXIT_CODE: u32 = 1;

// How often a stop looks again at processes it is waiting on.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(100);

// -- the server's job -------------------------------------------------------

// A job object that kills every process in it when its last handle closes.
//
// The tray holds the only handle for its whole life, so the server goes when
// the tray goes — whether it exited, crashed, or was ended from Task Manager.
// The handle is not inheritable, so no child can keep the job alive.
//
// Silent breakaway is deliberate: only the server itself is in the job, never
// what it starts. The upgrade helper is a child of the server and has to
// outlive both the server and the tray it asks to shut down; a job that
// claimed it would kill the helper in the middle of the upgrade it runs. The
// helper is spawned asking to break away, which a job allows only with
// explicit breakaway set as well, so both are.
pub(crate) struct KillOnCloseJob(HANDLE);

impl KillOnCloseJob {
    pub(crate) fn create() -> Result<Self, String> {
        // SAFETY: Null attributes give a non-inheritable handle; null name an
        // anonymous job.
        let handle = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
        if handle.is_null() {
            return Err(format!(
                "failed to create the server's job object: {}",
                std::io::Error::last_os_error()
            ));
        }
        let job = Self(handle);
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            | JOB_OBJECT_LIMIT_BREAKAWAY_OK
            | JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK;
        // SAFETY: `limits` is a fully initialized structure of the class named,
        // and the length passed is its size.
        let set = unsafe {
            SetInformationJobObject(
                job.0,
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if set == 0 {
            return Err(format!(
                "failed to make the server's job kill on close: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(job)
    }

    // Put a freshly spawned child in the job. Called straight after the spawn:
    // the server starts nothing of its own before it is serving, and whatever
    // it does start breaks away from the job anyway.
    pub(crate) fn assign(&self, child: &Child) -> Result<(), String> {
        // SAFETY: The job handle is owned by `self`, and the child's process
        // handle stays open for as long as `child` is borrowed.
        if unsafe { AssignProcessToJobObject(self.0, child.as_raw_handle() as HANDLE) } == 0 {
            return Err(format!(
                "failed to put the server (pid {}) in the tray's job: {}",
                child.id(),
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }
}

impl Drop for KillOnCloseJob {
    fn drop(&mut self) {
        // SAFETY: This value owns the handle CreateJobObjectW returned.
        unsafe { CloseHandle(self.0) };
    }
}

// -- graceful stop ----------------------------------------------------------

// Deliver Ctrl+C to the console `process_id` runs on, and report whether it
// was sent.
//
// The server is a console program the tray starts without a window, so its
// console is its own; Ctrl+C is the one shutdown request every console
// program on Windows understands, and the server's runtime answers it with
// the same graceful teardown a terminal's Ctrl+C would. A process with no
// console — a service, say — cannot be reached this way, and the caller
// falls back to terminating it.
//
// The tray is a GUI program with no console of its own, so attaching to the
// target's is safe; this process ignores the event while attached and stops
// ignoring it once detached, so nothing it starts afterwards inherits the
// ignore flag.
pub(crate) fn request_console_interrupt(process_id: u32) -> bool {
    // SAFETY: Each call takes plain integers or a null handler and touches
    // only this process's console attachment, which is restored before return.
    unsafe {
        FreeConsole();
        if AttachConsole(process_id) == 0 {
            return false;
        }
        SetConsoleCtrlHandler(None, 1);
        let sent = GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0) != 0;
        FreeConsole();
        SetConsoleCtrlHandler(None, 0);
        sent
    }
}

// An open handle on another process, with the rights a stop needs.
struct ProcessHandle {
    handle: HANDLE,
    process_id: u32,
}

impl ProcessHandle {
    // `None` when the process is gone or this process may not touch it.
    fn open(process_id: u32) -> Result<Self, u32> {
        // SAFETY: OpenProcess takes plain integers and returns a handle this
        // value owns, or null.
        let handle = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE | PROCESS_TERMINATE,
                0,
                process_id,
            )
        };
        if handle.is_null() {
            // SAFETY: GetLastError reads the error OpenProcess just set.
            return Err(unsafe { GetLastError() });
        }
        Ok(Self { handle, process_id })
    }

    fn image_path(&self) -> Option<String> {
        let mut buffer = vec![0u16; 32_768];
        let mut length = buffer.len() as u32;
        // SAFETY: `buffer` holds `length` UTF-16 units and outlives the call.
        let ok = unsafe {
            QueryFullProcessImageNameW(
                self.handle,
                PROCESS_NAME_WIN32,
                buffer.as_mut_ptr(),
                &mut length,
            )
        };
        (ok != 0).then(|| String::from_utf16_lossy(&buffer[..length as usize]))
    }

    fn exited_within(&self, timeout: Duration) -> bool {
        let milliseconds = u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX);
        // SAFETY: The handle is owned by `self` and carries SYNCHRONIZE.
        match unsafe { WaitForSingleObject(self.handle, milliseconds) } {
            WAIT_OBJECT_0 => true,
            WAIT_TIMEOUT => false,
            // A wait that failed outright says nothing about the process; it
            // is treated as still running so the caller escalates.
            _ => false,
        }
    }

    fn terminate(&self) -> Result<(), String> {
        // SAFETY: The handle is owned by `self` and carries PROCESS_TERMINATE.
        if unsafe { TerminateProcess(self.handle, FORCED_EXIT_CODE) } == 0 {
            return Err(format!(
                "failed to end process {}: {}",
                self.process_id,
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }
}

impl Drop for ProcessHandle {
    fn drop(&mut self) {
        // SAFETY: This value owns the handle OpenProcess returned.
        unsafe { CloseHandle(self.handle) };
    }
}

// What a stop left behind.
#[derive(Default)]
pub(crate) struct StopOutcome {
    // Processes still running after the stop gave up, described for a log
    // line or an error.
    pub(crate) survivors: Vec<String>,
    // Processes this one was not allowed to open, which it therefore could
    // neither stop nor wait on. Another user's process, seen from an
    // unelevated tray, lands here.
    pub(crate) inaccessible: Vec<String>,
}

// Stop each matching process: a console interrupt to every one, a wait until
// `graceful_until`, then `TerminateProcess` for whatever is left and a wait
// until `deadline`.
//
// Only processes running `names` directly from `install_dir` are touched,
// whatever the caller passed: the image path is read from the open handle, so
// a process id that was recycled between being found and being opened cannot
// redirect the stop at something else.
pub(crate) fn stop_installed_processes(
    process_ids: &[u32],
    install_dir: &Path,
    names: &[&str],
    graceful_until: Instant,
    deadline: Instant,
) -> StopOutcome {
    let own_id = current_process_id();
    let install_dir = install_dir.to_string_lossy();
    let mut outcome = StopOutcome::default();
    let mut targets = Vec::new();
    for &process_id in process_ids {
        if process_id == own_id {
            continue;
        }
        let process = match ProcessHandle::open(process_id) {
            Ok(process) => process,
            // Gone already, or never ours to stop.
            Err(error) if error == windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER => {
                continue;
            }
            Err(error) => {
                outcome
                    .inaccessible
                    .push(format!("process {process_id} (win32 error {error})"));
                continue;
            }
        };
        let Some(image) = process.image_path() else {
            continue;
        };
        if !shared::is_installed_executable(&image, &install_dir, names) {
            continue;
        }
        targets.push((process, image));
    }

    for (process, _) in &targets {
        request_console_interrupt(process.process_id);
    }
    let mut running = wait_for_exits(targets, graceful_until);
    for (process, image) in &running {
        if let Err(error) = process.terminate() {
            outcome.survivors.push(format!("{image}: {error}"));
        }
    }
    running = wait_for_exits(running, deadline);
    for (process, image) in running {
        let line = format!("{image} (pid {}) did not exit", process.process_id);
        if !outcome
            .survivors
            .iter()
            .any(|survivor| survivor.starts_with(&image))
        {
            outcome.survivors.push(line);
        }
    }
    outcome
}

// Wait until every process has exited or `until` passes; returns the ones
// still running.
fn wait_for_exits(
    mut processes: Vec<(ProcessHandle, String)>,
    until: Instant,
) -> Vec<(ProcessHandle, String)> {
    loop {
        processes.retain(|(process, _)| !process.exited_within(Duration::ZERO));
        if processes.is_empty() || Instant::now() >= until {
            return processes;
        }
        thread::sleep(STOP_POLL_INTERVAL.min(until.saturating_duration_since(Instant::now())));
    }
}

// -- finding Weaver processes -----------------------------------------------

pub(crate) fn current_process_id() -> u32 {
    // SAFETY: Takes no arguments and cannot fail.
    unsafe { GetCurrentProcessId() }
}

// Every running process whose executable file name is one of `names`. The
// snapshot only knows file names; callers confirm the full path through
// [`stop_installed_processes`], which reads it from an open handle.
pub(crate) fn processes_named(names: &[&str]) -> Vec<u32> {
    // SAFETY: A process snapshot takes no process id and returns a handle this
    // function closes, or INVALID_HANDLE_VALUE.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE || snapshot.is_null() {
        return Vec::new();
    }
    let mut found = Vec::new();
    let mut entry = PROCESSENTRY32W {
        dwSize: std::mem::size_of::<PROCESSENTRY32W>() as u32,
        ..Default::default()
    };
    // SAFETY: `entry` is writable, sized as the API requires, and the snapshot
    // handle is live until it is closed below.
    let mut more = unsafe { Process32FirstW(snapshot, &mut entry) } != 0;
    while more {
        let length = entry
            .szExeFile
            .iter()
            .position(|&unit| unit == 0)
            .unwrap_or(entry.szExeFile.len());
        let name = String::from_utf16_lossy(&entry.szExeFile[..length]);
        if names
            .iter()
            .any(|wanted| wanted.eq_ignore_ascii_case(&name))
        {
            found.push(entry.th32ProcessID);
        }
        // SAFETY: As above.
        more = unsafe { Process32NextW(snapshot, &mut entry) } != 0;
    }
    // SAFETY: This function owns the snapshot handle.
    unsafe { CloseHandle(snapshot) };
    found
}

// The process listening on `port`, on either address family.
pub(crate) fn listening_port_owner(port: u16) -> Option<u32> {
    listener_owner::<MIB_TCPROW_OWNER_PID>(AF_INET, port, |row| (row.dwLocalPort, row.dwOwningPid))
        .or_else(|| {
            listener_owner::<MIB_TCP6ROW_OWNER_PID>(AF_INET6, port, |row| {
                (row.dwLocalPort, row.dwOwningPid)
            })
        })
}

// Read one family's listener table and find the row on `port`. Both tables
// are a `u32` count followed by that many rows, so one reader serves both.
fn listener_owner<Row: Copy>(
    family: u32,
    port: u16,
    port_and_owner: impl Fn(&Row) -> (u32, u32),
) -> Option<u32> {
    let mut size = 0u32;
    // The table can grow between the size query and the read, so the read is
    // retried with the size it asks for.
    for _ in 0..4 {
        // A u32-aligned buffer, since the table starts with one.
        let mut buffer = vec![0u32; (size as usize).div_ceil(4).max(1)];
        let mut length = (buffer.len() * 4) as u32;
        // SAFETY: `buffer` is writable for `length` bytes and outlives the call.
        let status = unsafe {
            GetExtendedTcpTable(
                buffer.as_mut_ptr().cast(),
                &mut length,
                0,
                family,
                TCP_TABLE_OWNER_PID_LISTENER,
                0,
            )
        };
        if status == ERROR_INSUFFICIENT_BUFFER {
            size = length;
            continue;
        }
        if status != 0 {
            return None;
        }
        let count = buffer[0] as usize;
        let rows_offset = std::mem::align_of::<Row>().max(4);
        let available = (length as usize).saturating_sub(rows_offset) / std::mem::size_of::<Row>();
        let base = buffer.as_ptr().cast::<u8>();
        for index in 0..count.min(available) {
            // SAFETY: The row lies inside the bytes the API reported writing,
            // and `read_unaligned` makes no assumption about its alignment.
            let row = unsafe {
                ptr::read_unaligned(
                    base.add(rows_offset + index * std::mem::size_of::<Row>())
                        .cast::<Row>(),
                )
            };
            let (raw_port, owner) = port_and_owner(&row);
            if shared::tcp_table_port(raw_port) == port {
                return Some(owner);
            }
        }
        return None;
    }
    None
}
