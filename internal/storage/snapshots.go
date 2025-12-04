package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/le-vlad/pgbranch/pkg/config"
)

func GetSnapshotPath(filename string) (string, error) {
	snapshotsDir, err := config.GetSnapshotsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(snapshotsDir, filename), nil
}

func SnapshotFilename(branchName string) string {
	return fmt.Sprintf("%s.dump", branchName)
}

func SnapshotExists(filename string) (bool, error) {
	path, err := GetSnapshotPath(filename)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func DeleteSnapshot(filename string) error {
	path, err := GetSnapshotPath(filename)
	if err != nil {
		return err
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}
	return nil
}

func GetSnapshotSize(filename string) (int64, error) {
	path, err := GetSnapshotPath(filename)
	if err != nil {
		return 0, err
	}

	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func EnsureSnapshotsDir() error {
	snapshotsDir, err := config.GetSnapshotsDir()
	if err != nil {
		return err
	}
	return os.MkdirAll(snapshotsDir, 0755)
}
