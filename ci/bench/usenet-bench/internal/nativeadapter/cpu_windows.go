//go:build windows

package nativeadapter

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"syscall"
	"unsafe"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// Windows charges user and kernel time to whichever thread is running when
// the clock interrupt fires, so a client whose work is paced by that same
// clock -- a shaped localhost link releases bytes on timer ticks -- runs
// between the interrupts and is charged a fraction of what it used. On the
// smoke fixture GetProcessTimes reported 31 ms against 1.4 billion cycles for
// Weaver and 78 ms against 2.0 billion for NZBGet. Cycle counts are exact, so
// the Windows lane accounts in cycles and converts at the processor's nominal
// clock: the ratio between two clients on one host is exact whatever the
// clock did, and the absolute value is the time at the nominal frequency.
//
// A process's own counter also misses what its children used, which on this
// lane is the unpacker SABnzbd and NZBGet shell out to. The client is
// therefore placed in a job object; every process that joins the job is
// announced on a completion port, and a handle to each is held past its exit
// so its final count can still be read. The sum over the job is the
// client_process_tree figure. A process the announcement reached too late to
// hold makes the whole counter unavailable rather than a smaller number.
type windowsCPUAccount struct {
	job        syscall.Handle
	port       syscall.Handle
	nominalMHz uint64

	mu      sync.Mutex
	handles map[uint32]syscall.Handle
	missed  []uint32

	stopOnce sync.Once
	stop     chan struct{}
	drained  chan struct{}
}

const (
	windowsCPUCollector = "windows-job-cycle-time"
	windowsCPUScope     = "client_process_tree"

	jobObjectAssociateCompletionPortInformation = 7
	jobObjectMsgNewProcess                      = 6

	processTerminate               = 0x0001
	processSetQuota                = 0x0100
	processQueryLimitedInformation = 0x1000
	synchronize                    = 0x00100000

	// Members taskkill has just reached are given this long to finish leaving
	// so their final counts, not a snapshot from mid-exit, are read.
	memberExitWaitMillis = 5000
	drainPollMillis      = 100
)

var (
	kernel32                      = syscall.NewLazyDLL("kernel32.dll")
	procCreateJobObjectW          = kernel32.NewProc("CreateJobObjectW")
	procSetInformationJobObject   = kernel32.NewProc("SetInformationJobObject")
	procAssignProcessToJobObject  = kernel32.NewProc("AssignProcessToJobObject")
	procGetQueuedCompletionStatus = kernel32.NewProc("GetQueuedCompletionStatus")
	procQueryProcessCycleTime     = kernel32.NewProc("QueryProcessCycleTime")
)

type jobObjectAssociateCompletionPort struct {
	CompletionKey  uintptr
	CompletionPort syscall.Handle
}

func newCPUAccountant() cpuAccountant {
	account, err := newWindowsCPUAccount()
	if err != nil {
		return unavailableCPUAccount{reason: "windows job cycle accounting unavailable: " + err.Error()}
	}
	return account
}

func newWindowsCPUAccount() (*windowsCPUAccount, error) {
	nominalMHz, err := nominalProcessorMHz()
	if err != nil {
		return nil, err
	}
	job, _, callErr := procCreateJobObjectW.Call(0, 0)
	if job == 0 {
		return nil, fmt.Errorf("CreateJobObject: %w", callErr)
	}
	port, err := syscall.CreateIoCompletionPort(syscall.InvalidHandle, 0, 0, 1)
	if err != nil {
		_ = syscall.CloseHandle(syscall.Handle(job))
		return nil, fmt.Errorf("CreateIoCompletionPort: %w", err)
	}
	association := jobObjectAssociateCompletionPort{CompletionKey: 1, CompletionPort: port}
	ok, _, callErr := procSetInformationJobObject.Call(job, jobObjectAssociateCompletionPortInformation, uintptr(unsafe.Pointer(&association)), unsafe.Sizeof(association))
	if ok == 0 {
		_ = syscall.CloseHandle(port)
		_ = syscall.CloseHandle(syscall.Handle(job))
		return nil, fmt.Errorf("SetInformationJobObject(completion port): %w", callErr)
	}
	account := &windowsCPUAccount{
		job:        syscall.Handle(job),
		port:       port,
		nominalMHz: nominalMHz,
		handles:    make(map[uint32]syscall.Handle),
		stop:       make(chan struct{}),
		drained:    make(chan struct{}),
	}
	go account.drain()
	return account, nil
}

// attach places the freshly started client in the job. Children it starts
// from here on are members by inheritance; the moment between CreateProcess
// and this call is far shorter than any product's startup, and the job
// announces the client itself so its handle is held like any member's.
func (account *windowsCPUAccount) attach(process *os.Process) error {
	if process == nil || process.Pid < 1 {
		return os.ErrProcessDone
	}
	pid := uint32(process.Pid)
	handle, err := syscall.OpenProcess(processSetQuota|processTerminate|processQueryLimitedInformation|synchronize, false, pid)
	if err != nil {
		return fmt.Errorf("open client process %d: %w", pid, err)
	}
	ok, _, callErr := procAssignProcessToJobObject.Call(uintptr(account.job), uintptr(handle))
	if ok == 0 {
		_ = syscall.CloseHandle(handle)
		return fmt.Errorf("AssignProcessToJobObject: %w", callErr)
	}
	account.mu.Lock()
	defer account.mu.Unlock()
	if _, held := account.handles[pid]; held {
		_ = syscall.CloseHandle(handle)
		return nil
	}
	account.handles[pid] = handle
	return nil
}

func (account *windowsCPUAccount) drain() {
	defer close(account.drained)
	for {
		account.collect(drainPollMillis)
		select {
		case <-account.stop:
			account.collect(0)
			return
		default:
		}
	}
}

// collect takes every queued job message, waiting at most timeoutMillis for
// the first. A timeout comes back as a failure with no overlapped pointer,
// which is the only failure a job port produces.
func (account *windowsCPUAccount) collect(timeoutMillis uint32) {
	for {
		var message uint32
		var key uintptr
		var overlapped uintptr
		ok, _, _ := procGetQueuedCompletionStatus.Call(
			uintptr(account.port),
			uintptr(unsafe.Pointer(&message)),
			uintptr(unsafe.Pointer(&key)),
			uintptr(unsafe.Pointer(&overlapped)),
			uintptr(timeoutMillis),
		)
		if ok == 0 && overlapped == 0 {
			return
		}
		if message == jobObjectMsgNewProcess {
			account.hold(uint32(overlapped))
		}
		timeoutMillis = 0
	}
}

func (account *windowsCPUAccount) hold(pid uint32) {
	account.mu.Lock()
	defer account.mu.Unlock()
	if _, held := account.handles[pid]; held {
		return
	}
	handle, err := syscall.OpenProcess(processQueryLimitedInformation|synchronize, false, pid)
	if err != nil {
		account.missed = append(account.missed, pid)
		return
	}
	account.handles[pid] = handle
}

func (account *windowsCPUAccount) finishDraining() {
	account.stopOnce.Do(func() { close(account.stop) })
	<-account.drained
}

func (account *windowsCPUAccount) measurement(*os.ProcessState) benchmark.CounterMeasurement {
	account.finishDraining()
	account.mu.Lock()
	defer account.mu.Unlock()
	version := fmt.Sprintf("nominal-%dMHz", account.nominalMHz)
	if len(account.missed) > 0 {
		return benchmark.UnavailableMeasurement(windowsCPUScope, windowsCPUCollector, version,
			fmt.Sprintf("%d process(es) joined the client's job but exited before a handle could be held: pids %v", len(account.missed), account.missed))
	}
	pids := make([]int, 0, len(account.handles))
	for pid := range account.handles {
		pids = append(pids, int(pid))
	}
	sort.Ints(pids)
	var cycles uint64
	for _, pid := range pids {
		handle := account.handles[uint32(pid)]
		_, _ = syscall.WaitForSingleObject(handle, memberExitWaitMillis)
		var count uint64
		ok, _, callErr := procQueryProcessCycleTime.Call(uintptr(handle), uintptr(unsafe.Pointer(&count)))
		if ok == 0 {
			return benchmark.UnavailableMeasurement(windowsCPUScope, windowsCPUCollector, version,
				fmt.Sprintf("QueryProcessCycleTime(pid %d): %v", pid, callErr))
		}
		cycles += count
	}
	return benchmark.MeasuredMeasurement(windowsCPUScope, windowsCPUCollector, version, cycles*1000/account.nominalMHz)
}

func (account *windowsCPUAccount) close() {
	account.finishDraining()
	account.mu.Lock()
	defer account.mu.Unlock()
	for pid, handle := range account.handles {
		_ = syscall.CloseHandle(handle)
		delete(account.handles, pid)
	}
	if account.port != 0 {
		_ = syscall.CloseHandle(account.port)
		account.port = 0
	}
	if account.job != 0 {
		_ = syscall.CloseHandle(account.job)
		account.job = 0
	}
}

// nominalProcessorMHz is the clock the firmware reported for processor 0,
// the same figure Task Manager labels "Base speed".
func nominalProcessorMHz() (uint64, error) {
	subkey, err := syscall.UTF16PtrFromString(`HARDWARE\DESCRIPTION\System\CentralProcessor\0`)
	if err != nil {
		return 0, err
	}
	var key syscall.Handle
	if err := syscall.RegOpenKeyEx(syscall.HKEY_LOCAL_MACHINE, subkey, 0, syscall.KEY_READ, &key); err != nil {
		return 0, fmt.Errorf("open CentralProcessor\\0: %w", err)
	}
	defer syscall.RegCloseKey(key)
	name, err := syscall.UTF16PtrFromString("~MHz")
	if err != nil {
		return 0, err
	}
	var valueType uint32
	var value uint32
	length := uint32(unsafe.Sizeof(value))
	if err := syscall.RegQueryValueEx(key, name, nil, &valueType, (*byte)(unsafe.Pointer(&value)), &length); err != nil {
		return 0, fmt.Errorf("read ~MHz: %w", err)
	}
	if valueType != syscall.REG_DWORD || value == 0 {
		return 0, fmt.Errorf("~MHz is not a nonzero DWORD (type %d, value %d)", valueType, value)
	}
	return uint64(value), nil
}
