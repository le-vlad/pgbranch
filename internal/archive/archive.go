// Package archive provides functionality for creating and reading
// pgbranch snapshot archives for remote storage and transfer.
package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/le-vlad/pgbranch/internal/postgres"
	"github.com/le-vlad/pgbranch/pkg/config"
)

// Archive represents a pgbranch snapshot archive.
// Archive format is a gzipped tar containing:
//   - manifest.json: metadata about the snapshot
//   - dump.pgc: pg_dump custom format file
type Archive struct {
	Manifest *Manifest
	DumpData []byte
}

// CreateOptions contains optional parameters for creating an archive.
type CreateOptions struct {
	Description string
	CreatedBy   string
}

// Create creates a new archive from the specified snapshot database.
func Create(ctx context.Context, cfg *config.Config, branchName, snapshotDBName string, opts *CreateOptions) (*Archive, error) {
	client := postgres.NewClient(cfg)

	pgDumpVersion, _ := postgres.GetPgDumpVersion()

	var dumpBuf bytes.Buffer
	if err := client.DumpSnapshotToWriter(ctx, snapshotDBName, &dumpBuf); err != nil {
		return nil, fmt.Errorf("failed to dump database: %w", err)
	}

	dumpData := dumpBuf.Bytes()

	checksum, size, err := ComputeChecksum(bytes.NewReader(dumpData))
	if err != nil {
		return nil, fmt.Errorf("failed to compute checksum: %w", err)
	}

	manifest := NewManifest(branchName, cfg.Database)
	manifest.PgDumpVersion = pgDumpVersion
	manifest.DumpChecksum = checksum
	manifest.DumpSize = size

	if opts != nil {
		manifest.Description = opts.Description
		manifest.CreatedBy = opts.CreatedBy
	}

	return &Archive{
		Manifest: manifest,
		DumpData: dumpData,
	}, nil
}

// WriteTo writes the archive to the given writer in gzipped tar format.
func (a *Archive) WriteTo(w io.Writer) (int64, error) {
	gzw := gzip.NewWriter(w)
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	manifestData, err := a.Manifest.ToJSON()
	if err != nil {
		return 0, fmt.Errorf("failed to serialize manifest: %w", err)
	}

	if err := writeToTar(tw, ManifestFileName, manifestData); err != nil {
		return 0, fmt.Errorf("failed to write manifest to archive: %w", err)
	}

	if err := writeToTar(tw, DumpFileName, a.DumpData); err != nil {
		return 0, fmt.Errorf("failed to write dump to archive: %w", err)
	}

	return int64(len(manifestData) + len(a.DumpData)), nil
}

// writeToTar writes a single file entry to the tar archive.
func writeToTar(tw *tar.Writer, name string, data []byte) error {
	header := &tar.Header{
		Name: name,
		Mode: 0644,
		Size: int64(len(data)),
	}

	if err := tw.WriteHeader(header); err != nil {
		return err
	}

	_, err := tw.Write(data)
	return err
}

// ReadFrom reads an archive from the given reader.
func ReadFrom(r io.Reader) (*Archive, error) {
	gzr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	var manifest *Manifest
	var dumpData []byte

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read archive: %w", err)
		}

		switch header.Name {
		case ManifestFileName:
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, fmt.Errorf("failed to read manifest: %w", err)
			}
			manifest, err = ParseManifest(data)
			if err != nil {
				return nil, err
			}

		case DumpFileName:
			dumpData, err = io.ReadAll(tr)
			if err != nil {
				return nil, fmt.Errorf("failed to read dump: %w", err)
			}
		}
	}

	if manifest == nil {
		return nil, fmt.Errorf("archive missing manifest")
	}
	if dumpData == nil {
		return nil, fmt.Errorf("archive missing dump data")
	}

	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}

	checksum, size, err := ComputeChecksum(bytes.NewReader(dumpData))
	if err != nil {
		return nil, fmt.Errorf("failed to verify checksum: %w", err)
	}

	if checksum != manifest.DumpChecksum {
		return nil, fmt.Errorf("checksum mismatch: expected %s, got %s", manifest.DumpChecksum, checksum)
	}

	if size != manifest.DumpSize {
		return nil, fmt.Errorf("size mismatch: expected %d, got %d", manifest.DumpSize, size)
	}

	return &Archive{
		Manifest: manifest,
		DumpData: dumpData,
	}, nil
}

// Restore restores the archive to the specified snapshot database.
func (a *Archive) Restore(ctx context.Context, cfg *config.Config, snapshotDBName string) error {
	client := postgres.NewClient(cfg)

	if err := client.RestoreSnapshotFromReader(ctx, snapshotDBName, bytes.NewReader(a.DumpData)); err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}

	return nil
}

// SaveToFile saves the archive to the specified file path.
func (a *Archive) SaveToFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	_, err = a.WriteTo(f)
	return err
}

// LoadFromFile loads an archive from the specified file path.
func LoadFromFile(path string) (*Archive, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	return ReadFrom(f)
}

// Size returns the size of the dump data in bytes.
func (a *Archive) Size() int64 {
	return a.Manifest.DumpSize
}
