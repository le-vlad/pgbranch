//go:build !unix

package storage

// WithLock runs fn directly. File locking is not implemented on this platform,
// so concurrent worktree creation can lose a metadata entry.
func WithLock(fn func() error) error {
	return fn()
}
