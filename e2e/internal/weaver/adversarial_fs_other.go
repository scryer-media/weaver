// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//go:build !unix

package weaver

import (
	"errors"
	"io/fs"
)

// adversarialLinkCount is one where the platform exposes no link count.
func adversarialLinkCount(fs.FileInfo) uint64 {
	return 1
}

// adversarialFreeBytes is unavailable where the lane does not run.
func adversarialFreeBytes(string) (uint64, error) {
	return 0, errors.New("free space is read on unix hosts only")
}
