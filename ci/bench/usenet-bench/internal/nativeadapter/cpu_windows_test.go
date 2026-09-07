//go:build windows

package nativeadapter

import (
	"os/exec"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// The unpacker a client shells out to is a child of the client, and Windows
// charges nothing of a child to its parent. The job sums both: a cmd that
// does no work of its own but starts a PowerShell that does must report
// cycles the cmd alone could never have spent.
func TestTheJobChargesAChildsCyclesToTheClient(t *testing.T) {
	command := exec.Command("cmd.exe", "/c", `powershell -NoProfile -Command "$s=0; for($i=0;$i -lt 2000000;$i++){$s+=$i}; $s"`)
	if err := command.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	account := newCPUAccountant()
	if _, ok := account.(*windowsCPUAccount); !ok {
		t.Fatalf("accountant is %T, want the job account", account)
	}
	defer account.close()
	if err := account.attach(command.Process); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("wait: %v", err)
	}
	measured := account.measurement(command.ProcessState)
	if measured.Status != benchmark.CounterMeasured || measured.Value == nil {
		t.Fatalf("counter not measured: %+v", measured)
	}
	if measured.Scope != windowsCPUScope || measured.Collector != windowsCPUCollector {
		t.Fatalf("provenance %q/%q, want %q/%q", measured.Scope, measured.Collector, windowsCPUScope, windowsCPUCollector)
	}
	// Two million PowerShell loop iterations are hundreds of milliseconds at
	// any clock; cmd's own share is a few milliseconds at most.
	const floorNanos = 50_000_000
	if *measured.Value < floorNanos {
		t.Fatalf("job charged %d ns, want at least %d: the child's cycles were not counted", *measured.Value, floorNanos)
	}
	if held := len(account.(*windowsCPUAccount).handles); held < 2 {
		t.Fatalf("held %d process handle(s), want the cmd and its PowerShell", held)
	}
}
