// clientadapter is the shared, digest-pinned Docker adapter for Weaver,
// SABnzbd, and NZBGet. The product selection comes solely from BENCH_CLIENT.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/clientadapter"
)

func main() {
	cfg, err := clientadapter.LoadConfigFromEnvironment()
	if err == nil {
		err = clientadapter.Run(context.Background(), cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "clientadapter:", err)
		os.Exit(1)
	}
}
