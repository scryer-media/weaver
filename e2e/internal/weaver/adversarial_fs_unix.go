// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//go:build unix

package weaver

import (
	"io/fs"
	"syscall"
)

// adversarialLinkCount is how many hard links name the file; one when the
// platform does not say.
func adversarialLinkCount(info fs.FileInfo) uint64 {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(stat.Nlink)
	}
	return 1
}

// adversarialFreeBytes is the space an unprivileged writer may still use on
// the filesystem holding dir.
func adversarialFreeBytes(dir string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize), nil
}
