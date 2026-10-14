// Process plumbing for the Windows tray that the shared upgrade core does not
// provide; finding, asking and stopping Weaver processes is
// `application_updater::windows_process`.
//
// - the kill-on-close job the tray starts the server in, so a tray that dies
//   or is killed cannot leave its server running and holding the installed
//   binaries open;
// - the owner of the tray's port, which is how the tray tells the server it
//   adopted from any other Weaver process.

use std::os::windows::io::AsRawHandle;
use std::process::Child;
use std::ptr;

use windows_sys::Win32::Foundation::{CloseHandle, ERROR_INSUFFICIENT_BUFFER, HANDLE};
use windows_sys::Win32::NetworkManagement::IpHelper::{
    GetExtendedTcpTable, MIB_TCP6ROW_OWNER_PID, MIB_TCPROW_OWNER_PID, TCP_TABLE_OWNER_PID_LISTENER,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_BREAKAWAY_OK,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject,
};

use super::shared;

// `AF_INET` and `AF_INET6`, which `GetExtendedTcpTable` takes as a plain
// integer. Named here rather than pulled from the WinSock bindings, which
// would add a feature for two numbers.
const AF_INET: u32 = 2;
const AF_INET6: u32 = 23;

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

// -- the tray's port -------------------------------------------------------

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
