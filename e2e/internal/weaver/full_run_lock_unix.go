// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//go:build darwin || linux

package weaver

import (
	"errors"
	"os"
	"syscall"
)

var errFullRunLockHeld = errors.New("full-run lock is held")

// tryLockFile takes an exclusive flock on file without waiting. The lock
// belongs to the open file, so a child handed the same open file shares it,
// and it ends when the last process holding that open file exits.
func tryLockFile(file *os.File) error {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return errFullRunLockHeld
	}
	return err
}

func closeOnExec(fd int) {
	syscall.CloseOnExec(fd)
}

// fdNamesFile reports whether descriptor fd is open on the file at path.
func fdNamesFile(fd int, path string) bool {
	var held, want syscall.Stat_t
	if syscall.Fstat(fd, &held) != nil || syscall.Stat(path, &want) != nil {
		return false
	}
	return held.Dev == want.Dev && held.Ino == want.Ino
}
