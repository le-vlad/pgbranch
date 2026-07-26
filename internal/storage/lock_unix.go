//go:build unix

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/le-vlad/pgbranch/pkg/config"
)

// LockFileName guards read-modify-write cycles on the shared metadata file.
const LockFileName = ".lock"

// WithLock runs fn while holding an exclusive lock on the pgbranch directory.
//
// Every worktree of a repository shares one metadata file, so two worktrees
// being created at the same time would otherwise read the same metadata, each
// add their own branch, and the second write would drop the first. An atomic
// rename keeps the file well-formed but does not prevent that lost update.
//
// The lock is advisory and process-scoped; it protects concurrent pgbranch
// invocations, which is the case that actually occurs.
func WithLock(fn func() error) error {
	rootDir, err := config.GetRootDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return fmt.Errorf("failed to create %s: %w", rootDir, err)
	}

	file, err := os.OpenFile(filepath.Join(rootDir, LockFileName), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("failed to open lock file: %w", err)
	}
	defer file.Close()

	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

	return fn()
}
