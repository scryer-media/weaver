//go:build darwin || linux

package fixturegen

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockEnsure serialises fetch-and-generate across processes sharing one
// checkout with an advisory lock under target/. The lock is released when the
// returned function runs or the process exits.
func lockEnsure(root string) (func(), error) {
	dir := filepath.Join(root, "target", "fixturegen")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(dir, "ensure.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		file.Close()
		return nil, fmt.Errorf("lock %s: %w", file.Name(), err)
	}
	return func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		file.Close()
	}, nil
}
