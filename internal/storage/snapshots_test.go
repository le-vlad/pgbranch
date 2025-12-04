package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/le-vlad/pgbranch/pkg/config"
)

func setupSnapshotsTestDir(t *testing.T) (string, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "pgbranch-snapshots-test-*")
	require.NoError(t, err)

	originalDir, err := os.Getwd()
	require.NoError(t, err)

	err = os.Chdir(tmpDir)
	require.NoError(t, err)

	snapshotsDir := filepath.Join(tmpDir, config.DirName, config.SnapshotsDir)
	err = os.MkdirAll(snapshotsDir, 0755)
	require.NoError(t, err)

	cleanup := func() {
		os.Chdir(originalDir)
		os.RemoveAll(tmpDir)
	}

	return tmpDir, cleanup
}

func TestSnapshotFilename(t *testing.T) {
	tests := []struct {
		branchName string
		expected   string
	}{
		{"main", "main.dump"},
		{"feature-1", "feature-1.dump"},
		{"feature/login", "feature/login.dump"},
		{"my-branch", "my-branch.dump"},
	}

	for _, tt := range tests {
		t.Run(tt.branchName, func(t *testing.T) {
			result := SnapshotFilename(tt.branchName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetSnapshotPath(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)

	path, err := GetSnapshotPath("test.dump")
	require.NoError(t, err)

	expected := filepath.Join(cwd, config.DirName, config.SnapshotsDir, "test.dump")
	assert.Equal(t, expected, path)
}

func TestSnapshotExists(t *testing.T) {
	_, cleanup := setupSnapshotsTestDir(t)
	defer cleanup()

	exists, err := SnapshotExists("test.dump")
	require.NoError(t, err)
	assert.False(t, exists)

	snapshotPath, err := GetSnapshotPath("test.dump")
	require.NoError(t, err)
	err = os.WriteFile(snapshotPath, []byte("test data"), 0644)
	require.NoError(t, err)

	exists, err = SnapshotExists("test.dump")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestDeleteSnapshot(t *testing.T) {
	_, cleanup := setupSnapshotsTestDir(t)
	defer cleanup()

	snapshotPath, err := GetSnapshotPath("test.dump")
	require.NoError(t, err)
	err = os.WriteFile(snapshotPath, []byte("test data"), 0644)
	require.NoError(t, err)

	exists, err := SnapshotExists("test.dump")
	require.NoError(t, err)
	assert.True(t, exists)

	err = DeleteSnapshot("test.dump")
	require.NoError(t, err)

	exists, err = SnapshotExists("test.dump")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestDeleteSnapshotNonExistent(t *testing.T) {
	_, cleanup := setupSnapshotsTestDir(t)
	defer cleanup()

	err := DeleteSnapshot("non-existent.dump")
	require.NoError(t, err)
}

func TestGetSnapshotSize(t *testing.T) {
	_, cleanup := setupSnapshotsTestDir(t)
	defer cleanup()

	testData := []byte("test data with known size")
	snapshotPath, err := GetSnapshotPath("test.dump")
	require.NoError(t, err)
	err = os.WriteFile(snapshotPath, testData, 0644)
	require.NoError(t, err)

	size, err := GetSnapshotSize("test.dump")
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), size)
}

func TestGetSnapshotSizeNonExistent(t *testing.T) {
	_, cleanup := setupSnapshotsTestDir(t)
	defer cleanup()

	_, err := GetSnapshotSize("non-existent.dump")
	require.Error(t, err)
}

func TestEnsureSnapshotsDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pgbranch-ensure-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	originalDir, err := os.Getwd()
	require.NoError(t, err)
	defer os.Chdir(originalDir)

	err = os.Chdir(tmpDir)
	require.NoError(t, err)

	snapshotsDir := filepath.Join(tmpDir, config.DirName, config.SnapshotsDir)
	_, err = os.Stat(snapshotsDir)
	assert.True(t, os.IsNotExist(err))

	err = EnsureSnapshotsDir()
	require.NoError(t, err)

	info, err := os.Stat(snapshotsDir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	err = EnsureSnapshotsDir()
	require.NoError(t, err)
}
