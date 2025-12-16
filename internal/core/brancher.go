// Package core provides the main business logic for pgbranch,
// implementing database branching operations using PostgreSQL template databases.
package core

import (
	"fmt"
	"sort"

	"github.com/le-vlad/pgbranch/internal/postgres"
	"github.com/le-vlad/pgbranch/internal/storage"
	"github.com/le-vlad/pgbranch/pkg/config"
)

// Brancher manages database branches, coordinating between the PostgreSQL
// client, configuration, and metadata storage.
type Brancher struct {
	Config   *config.Config
	Metadata *storage.Metadata
	Client   *postgres.Client
}

// NewBrancher creates a new Brancher instance by loading the configuration
// and metadata from the current directory. Returns an error if pgbranch
// has not been initialized.
func NewBrancher() (*Brancher, error) {
	if !config.IsInitialized() {
		return nil, fmt.Errorf("pgbranch not initialized. Run 'pgbranch init' first")
	}

	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	meta, err := storage.LoadMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to load metadata: %w", err)
	}

	return &Brancher{
		Config:   cfg,
		Metadata: meta,
		Client:   postgres.NewClient(cfg),
	}, nil
}

// Initialize sets up pgbranch in the current directory with the given
// database connection parameters.
func Initialize(database, host string, port int, user, password string) error {
	rootDir, err := config.GetRootDir()
	if err != nil {
		return err
	}

	if err := config.EnsureDir(rootDir); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	cfg := config.DefaultConfig()
	cfg.Database = database
	if host != "" {
		cfg.Host = host
	}
	if port != 0 {
		cfg.Port = port
	}
	if user != "" {
		cfg.User = user
	}
	cfg.Password = password

	if err := cfg.Validate(); err != nil {
		return err
	}

	if err := cfg.Save(); err != nil {
		return fmt.Errorf("failed to save config: %w", err)
	}

	meta := storage.NewMetadata()
	if err := meta.Save(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// CreateBranch creates a new branch from the current database state.
// The branch is stored as a PostgreSQL template database.
func (b *Brancher) CreateBranch(name string) error {
	if b.Metadata.BranchExists(name) {
		return fmt.Errorf("branch '%s' already exists", name)
	}

	snapshotDBName := storage.SnapshotDBName(b.Config.Database, name)

	if err := b.Client.CreateSnapshot(snapshotDBName); err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	parent := b.Metadata.CurrentBranch
	b.Metadata.AddBranch(name, parent, snapshotDBName)

	if err := b.Metadata.Save(); err != nil {
		b.Client.DeleteSnapshot(snapshotDBName)
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// Checkout switches to the specified branch by replacing the working database
// with a copy of the branch's snapshot. The current branch state is saved
// before switching.
func (b *Brancher) Checkout(name string) error {
	branch, ok := b.Metadata.GetBranch(name)
	if !ok {
		return fmt.Errorf("branch '%s' does not exist", name)
	}

	if b.Metadata.CurrentBranch != "" && b.Metadata.CurrentBranch != name {
		if err := b.UpdateBranch(b.Metadata.CurrentBranch); err != nil {
			return fmt.Errorf("failed to save current branch '%s': %w", b.Metadata.CurrentBranch, err)
		}
	}

	snapshotDBName := branch.Snapshot

	if err := b.Client.RestoreFromSnapshot(snapshotDBName); err != nil {
		return fmt.Errorf("failed to restore branch: %w", err)
	}

	b.Metadata.CurrentBranch = name

	if err := b.Metadata.UpdateLastCheckout(name); err != nil {
		return fmt.Errorf("failed to update last checkout time: %w", err)
	}

	if err := b.Metadata.Save(); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	return nil
}

// DeleteBranch removes a branch and its associated snapshot database.
// Returns an error if trying to delete the current branch without force.
func (b *Brancher) DeleteBranch(name string, force bool) error {
	if name == b.Metadata.CurrentBranch && !force {
		return fmt.Errorf("cannot delete current branch '%s'. Use --force to override", name)
	}

	branch, ok := b.Metadata.GetBranch(name)
	if !ok {
		return fmt.Errorf("branch '%s' does not exist", name)
	}

	if err := b.Client.DeleteSnapshot(branch.Snapshot); err != nil {
		return fmt.Errorf("failed to delete snapshot database: %w", err)
	}

	if err := b.Metadata.DeleteBranch(name); err != nil {
		return err
	}

	if b.Metadata.CurrentBranch == name {
		b.Metadata.CurrentBranch = ""
	}

	if err := b.Metadata.Save(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// BranchInfo contains information about a branch for display purposes.
type BranchInfo struct {
	Name      string
	IsCurrent bool
	Branch    *storage.Branch
}

// ListBranches returns all branches sorted alphabetically by name.
func (b *Brancher) ListBranches() []BranchInfo {
	branches := make([]BranchInfo, 0, len(b.Metadata.Branches))

	for name, branch := range b.Metadata.Branches {
		branches = append(branches, BranchInfo{
			Name:      name,
			IsCurrent: name == b.Metadata.CurrentBranch,
			Branch:    branch,
		})
	}

	sort.Slice(branches, func(i, j int) bool {
		return branches[i].Name < branches[j].Name
	})

	return branches
}

// CurrentBranch returns the name of the currently checked out branch.
func (b *Brancher) CurrentBranch() string {
	return b.Metadata.CurrentBranch
}

// Status returns the current branch name and total number of branches.
func (b *Brancher) Status() (currentBranch string, branchCount int) {
	return b.Metadata.CurrentBranch, len(b.Metadata.Branches)
}

// UpdateBranch updates an existing branch's snapshot to match the current
// database state.
func (b *Brancher) UpdateBranch(name string) error {
	branch, ok := b.Metadata.GetBranch(name)
	if !ok {
		return fmt.Errorf("branch '%s' does not exist", name)
	}

	snapshotDBName := branch.Snapshot

	if err := b.Client.DeleteSnapshot(snapshotDBName); err != nil {
		return fmt.Errorf("failed to delete old snapshot: %w", err)
	}

	if err := b.Client.CreateSnapshot(snapshotDBName); err != nil {
		return fmt.Errorf("failed to create updated snapshot: %w", err)
	}

	return nil
}

// DefaultStaleDays is the default number of days after which a branch
// is considered stale.
const DefaultStaleDays = 7

// GetStaleBranches returns branches that haven't been accessed in the
// specified number of days, sorted by staleness (oldest first).
func (b *Brancher) GetStaleBranches(staleDays int) []BranchInfo {
	staleBranches := b.Metadata.GetStaleBranches(staleDays)
	result := make([]BranchInfo, 0, len(staleBranches))

	for _, branch := range staleBranches {
		result = append(result, BranchInfo{
			Name:      branch.Name,
			IsCurrent: branch.Name == b.Metadata.CurrentBranch,
			Branch:    branch,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Branch.DaysSinceLastAccess() > result[j].Branch.DaysSinceLastAccess()
	})

	return result
}

// PruneBranches deletes multiple branches by name, returning the list of
// successfully deleted branches and any errors encountered.
func (b *Brancher) PruneBranches(names []string) (deleted []string, errors []error) {
	for _, name := range names {
		if err := b.DeleteBranch(name, true); err != nil {
			errors = append(errors, fmt.Errorf("failed to delete '%s': %w", name, err))
		} else {
			deleted = append(deleted, name)
		}
	}
	return deleted, errors
}
