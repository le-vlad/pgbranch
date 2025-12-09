package remote

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	Register("fs", NewFilesystemRemote)
}

type FilesystemRemote struct {
	name string
	path string
}

func NewFilesystemRemote(cfg *Config) (Remote, error) {
	path := cfg.URL
	if path == "" {
		return nil, fmt.Errorf("filesystem path is required")
	}

	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to expand home directory: %w", err)
		}
		path = filepath.Join(home, path[2:])
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve path: %w", err)
	}

	return &FilesystemRemote{
		name: cfg.Name,
		path: absPath,
	}, nil
}

func (r *FilesystemRemote) Name() string {
	return r.name
}

func (r *FilesystemRemote) Type() string {
	return "fs"
}

func (r *FilesystemRemote) ensureDir() error {
	return os.MkdirAll(r.path, 0755)
}

func (r *FilesystemRemote) archivePath(branchName string) string {
	return filepath.Join(r.path, ArchiveFileName(branchName))
}

func (r *FilesystemRemote) Push(ctx context.Context, branchName string, reader io.Reader, size int64) error {
	if err := r.ensureDir(); err != nil {
		return fmt.Errorf("failed to create remote directory: %w", err)
	}

	archivePath := r.archivePath(branchName)

	tmpPath := archivePath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	_, err = io.Copy(f, reader)
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write archive: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to close file: %w", err)
	}

	if err := os.Rename(tmpPath, archivePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to finalize archive: %w", err)
	}

	return nil
}

func (r *FilesystemRemote) Pull(ctx context.Context, branchName string) (io.ReadCloser, int64, error) {
	archivePath := r.archivePath(branchName)

	info, err := os.Stat(archivePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, fmt.Errorf("branch '%s' not found on remote", branchName)
		}
		return nil, 0, fmt.Errorf("failed to stat archive: %w", err)
	}

	f, err := os.Open(archivePath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open archive: %w", err)
	}

	return f, info.Size(), nil
}

func (r *FilesystemRemote) List(ctx context.Context) ([]RemoteBranch, error) {
	entries, err := os.ReadDir(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return []RemoteBranch{}, nil
		}
		return nil, fmt.Errorf("failed to list remote: %w", err)
	}

	var branches []RemoteBranch
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".pgbranch") {
			continue
		}

		branchName := strings.TrimSuffix(name, ".pgbranch")

		info, err := entry.Info()
		if err != nil {
			continue
		}

		branches = append(branches, RemoteBranch{
			Name:    branchName,
			Size:    info.Size(),
			ModTime: info.ModTime(),
		})
	}

	return branches, nil
}

func (r *FilesystemRemote) Delete(ctx context.Context, branchName string) error {
	archivePath := r.archivePath(branchName)

	err := os.Remove(archivePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("branch '%s' not found on remote", branchName)
		}
		return fmt.Errorf("failed to delete archive: %w", err)
	}

	return nil
}

func (r *FilesystemRemote) Exists(ctx context.Context, branchName string) (bool, error) {
	archivePath := r.archivePath(branchName)

	_, err := os.Stat(archivePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check archive: %w", err)
	}

	return true, nil
}
