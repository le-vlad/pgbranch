package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/le-vlad/pgbranch/pkg/config"
)

func setupMetadataTestDir(t *testing.T) (string, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "pgbranch-metadata-test-*")
	require.NoError(t, err)

	originalDir, err := os.Getwd()
	require.NoError(t, err)

	err = os.Chdir(tmpDir)
	require.NoError(t, err)

	pgbranchDir := filepath.Join(tmpDir, config.DirName)
	err = os.MkdirAll(pgbranchDir, 0755)
	require.NoError(t, err)

	cleanup := func() {
		os.Chdir(originalDir)
		os.RemoveAll(tmpDir)
	}

	return tmpDir, cleanup
}

func TestNewMetadata(t *testing.T) {
	meta := NewMetadata()

	assert.Empty(t, meta.CurrentBranch)
	assert.NotNil(t, meta.Branches)
	assert.Len(t, meta.Branches, 0)
}

func TestAddBranch(t *testing.T) {
	meta := NewMetadata()

	branch := meta.AddBranch("feature-1", "", "feature-1.dump")

	assert.Equal(t, "feature-1", branch.Name)
	assert.Equal(t, "", branch.Parent)
	assert.Equal(t, "feature-1.dump", branch.Snapshot)
	assert.NotZero(t, branch.CreatedAt)

	assert.True(t, meta.BranchExists("feature-1"))
	assert.Len(t, meta.Branches, 1)
}

func TestAddBranchWithParent(t *testing.T) {
	meta := NewMetadata()

	meta.AddBranch("main", "", "main.dump")

	branch := meta.AddBranch("feature-1", "main", "feature-1.dump")

	assert.Equal(t, "main", branch.Parent)
}

func TestGetBranch(t *testing.T) {
	meta := NewMetadata()
	meta.AddBranch("feature-1", "", "feature-1.dump")

	branch, ok := meta.GetBranch("feature-1")
	assert.True(t, ok)
	assert.Equal(t, "feature-1", branch.Name)

	_, ok = meta.GetBranch("non-existent")
	assert.False(t, ok)
}

func TestDeleteBranch(t *testing.T) {
	meta := NewMetadata()
	meta.AddBranch("feature-1", "", "feature-1.dump")
	meta.AddBranch("feature-2", "", "feature-2.dump")

	err := meta.DeleteBranch("feature-1")
	require.NoError(t, err)
	assert.False(t, meta.BranchExists("feature-1"))
	assert.True(t, meta.BranchExists("feature-2"))

	err = meta.DeleteBranch("non-existent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestBranchExists(t *testing.T) {
	meta := NewMetadata()
	meta.AddBranch("feature-1", "", "feature-1.dump")

	assert.True(t, meta.BranchExists("feature-1"))
	assert.False(t, meta.BranchExists("feature-2"))
}

func TestListBranches(t *testing.T) {
	meta := NewMetadata()

	branches := meta.ListBranches()
	assert.Len(t, branches, 0)

	meta.AddBranch("feature-1", "", "feature-1.dump")
	meta.AddBranch("feature-2", "", "feature-2.dump")
	meta.AddBranch("main", "", "main.dump")

	branches = meta.ListBranches()
	assert.Len(t, branches, 3)
	assert.Contains(t, branches, "feature-1")
	assert.Contains(t, branches, "feature-2")
	assert.Contains(t, branches, "main")
}

func TestSetCurrentBranch(t *testing.T) {
	meta := NewMetadata()
	meta.AddBranch("feature-1", "", "feature-1.dump")

	err := meta.SetCurrentBranch("feature-1")
	require.NoError(t, err)
	assert.Equal(t, "feature-1", meta.CurrentBranch)

	err = meta.SetCurrentBranch("non-existent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	err = meta.SetCurrentBranch("")
	require.NoError(t, err)
	assert.Empty(t, meta.CurrentBranch)
}

func TestMetadataSaveAndLoad(t *testing.T) {
	_, cleanup := setupMetadataTestDir(t)
	defer cleanup()

	meta := NewMetadata()
	meta.AddBranch("main", "", "main.dump")
	meta.AddBranch("feature-1", "main", "feature-1.dump")
	meta.CurrentBranch = "feature-1"

	err := meta.Save()
	require.NoError(t, err)

	loadedMeta, err := LoadMetadata()
	require.NoError(t, err)

	assert.Equal(t, "feature-1", loadedMeta.CurrentBranch)
	assert.Len(t, loadedMeta.Branches, 2)
	assert.True(t, loadedMeta.BranchExists("main"))
	assert.True(t, loadedMeta.BranchExists("feature-1"))

	branch, ok := loadedMeta.GetBranch("feature-1")
	assert.True(t, ok)
	assert.Equal(t, "main", branch.Parent)
	assert.Equal(t, "feature-1.dump", branch.Snapshot)
}

func TestLoadMetadataCreatesNewIfNotExists(t *testing.T) {
	_, cleanup := setupMetadataTestDir(t)
	defer cleanup()

	meta, err := LoadMetadata()
	require.NoError(t, err)

	assert.Empty(t, meta.CurrentBranch)
	assert.NotNil(t, meta.Branches)
	assert.Len(t, meta.Branches, 0)
}

func TestGetMetadataPath(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)

	metaPath, err := GetMetadataPath()
	require.NoError(t, err)

	expected := filepath.Join(cwd, config.DirName, MetadataFileName)
	assert.Equal(t, expected, metaPath)
}

func TestGetStaleBranches(t *testing.T) {
	meta := NewMetadata()

	mainBranch := meta.AddBranch("main", "", "main.dump")
	mainBranch.CreatedAt = mainBranch.CreatedAt.AddDate(0, 0, -30)

	feature1 := meta.AddBranch("feature-1", "main", "feature-1.dump")
	feature1.CreatedAt = feature1.CreatedAt.AddDate(0, 0, -10) // 10 days old

	feature2 := meta.AddBranch("feature-2", "main", "feature-2.dump")
	feature2.CreatedAt = feature2.CreatedAt.AddDate(0, 0, -3) // 3 days old (not stale for 7 days)

	feature3 := meta.AddBranch("feature-3", "feature-1", "feature-3.dump")
	feature3.CreatedAt = feature3.CreatedAt.AddDate(0, 0, -15) // 15 days old

	staleBranches := meta.GetStaleBranches(7)

	assert.Len(t, staleBranches, 2)

	staleNames := make([]string, len(staleBranches))
	for i, b := range staleBranches {
		staleNames[i] = b.Name
	}

	assert.Contains(t, staleNames, "feature-1")
	assert.Contains(t, staleNames, "feature-3")
	assert.NotContains(t, staleNames, "main")
	assert.NotContains(t, staleNames, "feature-2")
}

func TestGetStaleBranchesExcludesRootBranch(t *testing.T) {
	meta := NewMetadata()

	mainBranch := meta.AddBranch("main", "", "main.dump")
	mainBranch.CreatedAt = mainBranch.CreatedAt.AddDate(0, 0, -100)
	staleBranches := meta.GetStaleBranches(7)

	assert.Len(t, staleBranches, 0)
}

func TestDaysSinceLastAccess(t *testing.T) {
	t.Run("recent checkout returns 0", func(t *testing.T) {
		b := &Branch{
			Name:           "feature-1",
			CreatedAt:      time.Now().Add(-48 * time.Hour),
			LastCheckoutAt: time.Now(),
		}
		assert.Equal(t, 0, b.DaysSinceLastAccess())
	})

	t.Run("checkout 10 days ago returns 10", func(t *testing.T) {
		b := &Branch{
			Name:           "feature-2",
			CreatedAt:      time.Now().Add(-30 * 24 * time.Hour),
			LastCheckoutAt: time.Now().Add(-10 * 24 * time.Hour),
		}
		assert.Equal(t, 10, b.DaysSinceLastAccess())
	})

	t.Run("zero LastCheckoutAt falls back to CreatedAt", func(t *testing.T) {
		b := &Branch{
			Name:      "feature-3",
			CreatedAt: time.Now().Add(-5 * 24 * time.Hour),
		}
		assert.True(t, b.LastCheckoutAt.IsZero())
		assert.Equal(t, 5, b.DaysSinceLastAccess())
	})
}

func TestUpdateLastCheckout(t *testing.T) {
	t.Run("updates existing branch", func(t *testing.T) {
		meta := NewMetadata()
		meta.AddBranch("feature-1", "", "feature-1.dump")

		before := time.Now()
		err := meta.UpdateLastCheckout("feature-1")
		require.NoError(t, err)

		branch, ok := meta.GetBranch("feature-1")
		require.True(t, ok)
		assert.False(t, branch.LastCheckoutAt.IsZero())
		assert.True(t, !branch.LastCheckoutAt.Before(before))
	})

	t.Run("returns error for non-existent branch", func(t *testing.T) {
		meta := NewMetadata()

		err := meta.UpdateLastCheckout("non-existent")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})
}
