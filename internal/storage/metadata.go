// Package storage provides persistent storage for branch metadata and snapshots.
package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/le-vlad/pgbranch/pkg/config"
)

// MetadataFileName is the name of the metadata file in the pgbranch directory.
const MetadataFileName = "metadata.json"

// Branch represents a database branch with its metadata.
type Branch struct {
	Name           string    `json:"name"`
	CreatedAt      time.Time `json:"created_at"`
	LastCheckoutAt time.Time `json:"last_checkout_at,omitempty"`
	Parent         string    `json:"parent,omitempty"`
	Snapshot       string    `json:"snapshot"`
}

// IsStale returns true if the branch hasn't been accessed in the specified
// number of days.
func (b *Branch) IsStale(staleDays int) bool {
	threshold := time.Now().AddDate(0, 0, -staleDays)

	// If never checked out, use CreatedAt
	if b.LastCheckoutAt.IsZero() {
		return b.CreatedAt.Before(threshold)
	}

	return b.LastCheckoutAt.Before(threshold)
}

// DaysSinceLastAccess returns the number of days since the branch was last
// accessed (checked out or created).
func (b *Branch) DaysSinceLastAccess() int {
	var lastAccess time.Time
	if b.LastCheckoutAt.IsZero() {
		lastAccess = b.CreatedAt
	} else {
		lastAccess = b.LastCheckoutAt
	}

	return int(time.Since(lastAccess).Hours() / 24)
}

// Metadata stores information about all branches and the current branch state.
type Metadata struct {
	CurrentBranch string             `json:"current_branch"`
	Branches      map[string]*Branch `json:"branches"`
}

// NewMetadata creates a new empty Metadata instance.
func NewMetadata() *Metadata {
	return &Metadata{
		CurrentBranch: "",
		Branches:      make(map[string]*Branch),
	}
}

// GetMetadataPath returns the absolute path to the metadata file.
func GetMetadataPath() (string, error) {
	rootDir, err := config.GetRootDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(rootDir, MetadataFileName), nil
}

// LoadMetadata reads and parses the metadata file. If the file doesn't exist,
// returns a new empty Metadata instance.
func LoadMetadata() (*Metadata, error) {
	metadataPath, err := GetMetadataPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return NewMetadata(), nil
		}
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	var meta Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse metadata file: %w", err)
	}

	if meta.Branches == nil {
		meta.Branches = make(map[string]*Branch)
	}

	return &meta, nil
}

// Save writes the metadata to the metadata file.
func (m *Metadata) Save() error {
	metadataPath, err := GetMetadataPath()
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize metadata: %w", err)
	}

	if err := os.WriteFile(metadataPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	return nil
}

// AddBranch creates and adds a new branch to the metadata.
func (m *Metadata) AddBranch(name, parent, snapshotFile string) *Branch {
	branch := &Branch{
		Name:      name,
		CreatedAt: time.Now(),
		Parent:    parent,
		Snapshot:  snapshotFile,
	}
	m.Branches[name] = branch
	return branch
}

// GetBranch returns the branch with the given name, or false if not found.
func (m *Metadata) GetBranch(name string) (*Branch, bool) {
	branch, ok := m.Branches[name]
	return branch, ok
}

// DeleteBranch removes a branch from the metadata.
func (m *Metadata) DeleteBranch(name string) error {
	if _, ok := m.Branches[name]; !ok {
		return fmt.Errorf("branch '%s' does not exist", name)
	}
	delete(m.Branches, name)
	return nil
}

// BranchExists returns true if a branch with the given name exists.
func (m *Metadata) BranchExists(name string) bool {
	_, ok := m.Branches[name]
	return ok
}

// ListBranches returns a list of all branch names.
func (m *Metadata) ListBranches() []string {
	names := make([]string, 0, len(m.Branches))
	for name := range m.Branches {
		names = append(names, name)
	}
	return names
}

// SetCurrentBranch sets the current branch to the given name.
func (m *Metadata) SetCurrentBranch(name string) error {
	if name != "" && !m.BranchExists(name) {
		return fmt.Errorf("branch '%s' does not exist", name)
	}
	m.CurrentBranch = name
	return nil
}

// GetStaleBranches returns all branches that haven't been accessed
// in the specified number of days.
func (m *Metadata) GetStaleBranches(staleDays int) []*Branch {
	var stale []*Branch
	for _, branch := range m.Branches {
		if branch.IsStale(staleDays) {
			stale = append(stale, branch)
		}
	}
	return stale
}

// UpdateLastCheckout updates the last checkout time for the given branch.
func (m *Metadata) UpdateLastCheckout(name string) error {
	branch, ok := m.Branches[name]
	if !ok {
		return fmt.Errorf("branch '%s' does not exist", name)
	}
	branch.LastCheckoutAt = time.Now()
	return nil
}
