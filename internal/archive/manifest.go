package archive

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const (
	ManifestFileName = "manifest.json"
	DumpFileName     = "dump.pgc"
	CurrentVersion   = 1
)

// Manifest contains metadata about a snapshot archive
type Manifest struct {
	Version int `json:"version"`

	Branch string `json:"branch"`

	Database string `json:"database"`

	CreatedAt time.Time `json:"created_at"`

	CreatedBy string `json:"created_by,omitempty"`

	PgVersion string `json:"pg_version,omitempty"`

	PgDumpVersion string `json:"pg_dump_version,omitempty"`

	DumpChecksum string `json:"dump_checksum"`

	DumpSize int64 `json:"dump_size"`

	Parent string `json:"parent,omitempty"`

	Description string `json:"description,omitempty"`
}

func NewManifest(branch, database string) *Manifest {
	return &Manifest{
		Version:   CurrentVersion,
		Branch:    branch,
		Database:  database,
		CreatedAt: time.Now().UTC(),
	}
}

func (m *Manifest) Validate() error {
	if m.Version == 0 {
		return fmt.Errorf("manifest version is required")
	}
	if m.Version > CurrentVersion {
		return fmt.Errorf("manifest version %d is not supported (max: %d)", m.Version, CurrentVersion)
	}
	if m.Branch == "" {
		return fmt.Errorf("branch name is required")
	}
	if m.Database == "" {
		return fmt.Errorf("database name is required")
	}
	if m.DumpChecksum == "" {
		return fmt.Errorf("dump checksum is required")
	}
	if m.DumpSize == 0 {
		return fmt.Errorf("dump size is required")
	}
	return nil
}

func (m *Manifest) ToJSON() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

func ParseManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}
	return &m, nil
}

func ComputeChecksum(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", 0, fmt.Errorf("failed to compute checksum: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func VerifyChecksum(r io.Reader, expectedChecksum string) (bool, error) {
	checksum, _, err := ComputeChecksum(r)
	if err != nil {
		return false, err
	}
	return checksum == expectedChecksum, nil
}
