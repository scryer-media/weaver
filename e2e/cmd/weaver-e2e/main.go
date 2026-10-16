// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package main

import (
	"github.com/scryer-media/weaver/e2e/internal/weaver"
	"os"
)

func main() {
	weaver.Run(os.Args[1:], "weaver-e2e")
}
