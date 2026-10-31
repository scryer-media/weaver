// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/nntphealth"
)

func main() {
	address := flag.String("addr", "127.0.0.1:119", "fixture listener")
	flag.Parse()
	if err := nntphealth.CheckAddress(*address, time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
