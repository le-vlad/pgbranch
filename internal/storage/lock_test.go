//go:build unix

package storage

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func initPgbranchDir(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	orig, err := os.Getwd()
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Chdir(orig) })

	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".pgbranch"), 0755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, ".pgbranch", "config.json"), []byte(`{"database":"x"}`), 0644))
	require.NoError(t, os.Chdir(dir))
}

// Every worktree shares one metadata file. Without serialization, two worktrees
// created at once each add a branch and the second write drops the first.
func TestWithLockSerializesHolders(t *testing.T) {
	initPgbranchDir(t)

	var inside, maxInside int32
	var wg sync.WaitGroup

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, WithLock(func() error {
				n := atomic.AddInt32(&inside, 1)
				for {
					m := atomic.LoadInt32(&maxInside)
					if n <= m || atomic.CompareAndSwapInt32(&maxInside, m, n) {
						break
					}
				}
				atomic.AddInt32(&inside, -1)
				return nil
			}))
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(1), maxInside, "more than one holder was inside the lock")
}

// A concurrent read-modify-write of metadata must not lose entries.
func TestWithLockPreventsLostUpdates(t *testing.T) {
	initPgbranchDir(t)
	require.NoError(t, NewMetadata().Save())

	var wg sync.WaitGroup
	names := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	for _, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, WithLock(func() error {
				meta, err := LoadMetadata()
				if err != nil {
					return err
				}
				meta.AddBranch(name, "", SnapshotDBName("appdb", name))
				return meta.Save()
			}))
		}()
	}
	wg.Wait()

	meta, err := LoadMetadata()
	require.NoError(t, err)
	assert.Len(t, meta.Branches, len(names), "a concurrent write was lost")
}

func TestWithLockReleasesOnError(t *testing.T) {
	initPgbranchDir(t)

	assert.Error(t, WithLock(func() error { return assert.AnError }))
	// A leaked lock would deadlock this second acquisition.
	assert.NoError(t, WithLock(func() error { return nil }))
}
