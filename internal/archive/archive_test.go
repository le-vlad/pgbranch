package archive

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewManifest(t *testing.T) {
	m := NewManifest("feature-1", "mydb")

	assert.Equal(t, CurrentVersion, m.Version)
	assert.Equal(t, "feature-1", m.Branch)
	assert.Equal(t, "mydb", m.Database)
	assert.False(t, m.CreatedAt.IsZero())
}

func TestManifestValidate(t *testing.T) {
	validManifest := func() *Manifest {
		m := NewManifest("feature-1", "mydb")
		m.DumpChecksum = "abc123"
		m.DumpSize = 100
		return m
	}

	t.Run("valid", func(t *testing.T) {
		m := validManifest()
		assert.NoError(t, m.Validate())
	})

	t.Run("missing version", func(t *testing.T) {
		m := validManifest()
		m.Version = 0
		err := m.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "version")
	})

	t.Run("unsupported version", func(t *testing.T) {
		m := validManifest()
		m.Version = CurrentVersion + 1
		err := m.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})

	t.Run("missing branch", func(t *testing.T) {
		m := validManifest()
		m.Branch = ""
		err := m.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "branch")
	})

	t.Run("missing database", func(t *testing.T) {
		m := validManifest()
		m.Database = ""
		err := m.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "database")
	})

	t.Run("missing checksum", func(t *testing.T) {
		m := validManifest()
		m.DumpChecksum = ""
		err := m.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "checksum")
	})

	t.Run("missing size", func(t *testing.T) {
		m := validManifest()
		m.DumpSize = 0
		err := m.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "size")
	})
}

func TestManifestJSONRoundTrip(t *testing.T) {
	m := NewManifest("feature-1", "mydb")
	m.DumpChecksum = "deadbeef"
	m.DumpSize = 42
	m.Description = "test snapshot"
	m.CreatedBy = "tester"
	m.PgDumpVersion = "16.1"

	data, err := m.ToJSON()
	require.NoError(t, err)

	parsed, err := ParseManifest(data)
	require.NoError(t, err)

	assert.Equal(t, m.Version, parsed.Version)
	assert.Equal(t, m.Branch, parsed.Branch)
	assert.Equal(t, m.Database, parsed.Database)
	assert.Equal(t, m.CreatedAt, parsed.CreatedAt)
	assert.Equal(t, m.DumpChecksum, parsed.DumpChecksum)
	assert.Equal(t, m.DumpSize, parsed.DumpSize)
	assert.Equal(t, m.Description, parsed.Description)
	assert.Equal(t, m.CreatedBy, parsed.CreatedBy)
	assert.Equal(t, m.PgDumpVersion, parsed.PgDumpVersion)
}

func TestComputeChecksum(t *testing.T) {
	data := []byte("hello world")

	checksum1, size1, err := ComputeChecksum(bytes.NewReader(data))
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), size1)

	checksum2, size2, err := ComputeChecksum(bytes.NewReader(data))
	require.NoError(t, err)
	assert.Equal(t, checksum1, checksum2)
	assert.Equal(t, size1, size2)
}

func TestVerifyChecksum(t *testing.T) {
	data := []byte("hello world")
	checksum, _, err := ComputeChecksum(bytes.NewReader(data))
	require.NoError(t, err)

	t.Run("correct checksum", func(t *testing.T) {
		ok, err := VerifyChecksum(bytes.NewReader(data), checksum)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("wrong checksum", func(t *testing.T) {
		ok, err := VerifyChecksum(strings.NewReader("different data"), checksum)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestArchiveWriteToReadFromRoundTrip(t *testing.T) {
	dumpData := []byte("fake pg_dump output")
	checksum, size, err := ComputeChecksum(bytes.NewReader(dumpData))
	require.NoError(t, err)

	m := NewManifest("feature-1", "mydb")
	m.DumpChecksum = checksum
	m.DumpSize = size

	original := &Archive{
		Manifest: m,
		DumpData: dumpData,
	}

	var buf bytes.Buffer
	_, err = original.WriteTo(&buf)
	require.NoError(t, err)

	restored, err := ReadFrom(&buf)
	require.NoError(t, err)

	assert.Equal(t, original.Manifest.Version, restored.Manifest.Version)
	assert.Equal(t, original.Manifest.Branch, restored.Manifest.Branch)
	assert.Equal(t, original.Manifest.Database, restored.Manifest.Database)
	assert.Equal(t, original.Manifest.DumpChecksum, restored.Manifest.DumpChecksum)
	assert.Equal(t, original.Manifest.DumpSize, restored.Manifest.DumpSize)
	assert.Equal(t, original.DumpData, restored.DumpData)
}

func TestArchiveSize(t *testing.T) {
	m := NewManifest("feature-1", "mydb")
	m.DumpSize = 12345

	a := &Archive{Manifest: m}

	assert.Equal(t, int64(12345), a.Size())
}

func TestSaveToFileLoadFromFileRoundTrip(t *testing.T) {
	dumpData := []byte("fake pg_dump output for file test")
	checksum, size, err := ComputeChecksum(bytes.NewReader(dumpData))
	require.NoError(t, err)

	m := NewManifest("feature-1", "mydb")
	m.DumpChecksum = checksum
	m.DumpSize = size

	original := &Archive{
		Manifest: m,
		DumpData: dumpData,
	}

	tmpFile, err := os.CreateTemp("", "pgbranch-archive-test-*.tar.gz")
	require.NoError(t, err)
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	err = original.SaveToFile(tmpPath)
	require.NoError(t, err)

	restored, err := LoadFromFile(tmpPath)
	require.NoError(t, err)

	assert.Equal(t, original.Manifest.Branch, restored.Manifest.Branch)
	assert.Equal(t, original.Manifest.Database, restored.Manifest.Database)
	assert.Equal(t, original.Manifest.DumpChecksum, restored.Manifest.DumpChecksum)
	assert.Equal(t, original.Manifest.DumpSize, restored.Manifest.DumpSize)
	assert.Equal(t, original.DumpData, restored.DumpData)
}
