// nativeadapter launches one native macOS or Windows client process for a
// single benchmark run. It deliberately shares the runner's BENCH_* contract
// with the Docker adapter so results differ only by persisted execution target.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/nativeadapter"
)

func main() {
	cfg, err := nativeadapter.LoadConfigFromEnvironment()
	if err == nil {
		err = nativeadapter.Run(context.Background(), cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "nativeadapter:", err)
		os.Exit(1)
	}
}
