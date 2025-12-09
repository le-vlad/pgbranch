package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/le-vlad/pgbranch/pkg/config"
)

const MetadataFileName = "metadata.json"

type Branch struct {
	Name           string    `json:"name"`
	CreatedAt      time.Time `json:"created_at"`
	LastCheckoutAt time.Time `json:"last_checkout_at,omitempty"`
	Parent         string    `json:"parent,omitempty"`
	Snapshot       string    `json:"snapshot"`
}

func (b *Branch) IsStale(staleDays int) bool {
	threshold := time.Now().AddDate(0, 0, -staleDays)

	// If never checked out, use CreatedAt
	if b.LastCheckoutAt.IsZero() {
		return b.CreatedAt.Before(threshold)
	}

	return b.LastCheckoutAt.Before(threshold)
}

func (b *Branch) DaysSinceLastAccess() int {
	var lastAccess time.Time
	if b.LastCheckoutAt.IsZero() {
		lastAccess = b.CreatedAt
	} else {
		lastAccess = b.LastCheckoutAt
	}

	return int(time.Since(lastAccess).Hours() / 24)
}

type Metadata struct {
	CurrentBranch string             `json:"current_branch"`
	Branches      map[string]*Branch `json:"branches"`
}

func NewMetadata() *Metadata {
	return &Metadata{
		CurrentBranch: "",
		Branches:      make(map[string]*Branch),
	}
}

func GetMetadataPath() (string, error) {
	rootDir, err := config.GetRootDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(rootDir, MetadataFileName), nil
}

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

func (m *Metadata) GetBranch(name string) (*Branch, bool) {
	branch, ok := m.Branches[name]
	return branch, ok
}

func (m *Metadata) DeleteBranch(name string) error {
	if _, ok := m.Branches[name]; !ok {
		return fmt.Errorf("branch '%s' does not exist", name)
	}
	delete(m.Branches, name)
	return nil
}

func (m *Metadata) BranchExists(name string) bool {
	_, ok := m.Branches[name]
	return ok
}

func (m *Metadata) ListBranches() []string {
	names := make([]string, 0, len(m.Branches))
	for name := range m.Branches {
		names = append(names, name)
	}
	return names
}

func (m *Metadata) SetCurrentBranch(name string) error {
	if name != "" && !m.BranchExists(name) {
		return fmt.Errorf("branch '%s' does not exist", name)
	}
	m.CurrentBranch = name
	return nil
}

func (m *Metadata) GetStaleBranches(staleDays int) []*Branch {
	var stale []*Branch
	for _, branch := range m.Branches {
		if branch.IsStale(staleDays) {
			stale = append(stale, branch)
		}
	}
	return stale
}

func (m *Metadata) UpdateLastCheckout(name string) error {
	branch, ok := m.Branches[name]
	if !ok {
		return fmt.Errorf("branch '%s' does not exist", name)
	}
	branch.LastCheckoutAt = time.Now()
	return nil
}
