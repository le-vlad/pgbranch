package storage

import (
	"os"
	"path/filepath"
	"testing"

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
