package core

import (
	"fmt"
	"sort"

	"github.com/le-vlad/pgbranch/internal/postgres"
	"github.com/le-vlad/pgbranch/internal/storage"
	"github.com/le-vlad/pgbranch/pkg/config"
)

type Brancher struct {
	Config   *config.Config
	Metadata *storage.Metadata
	Client   *postgres.Client
}

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

func (b *Brancher) Checkout(name string) error {
	branch, ok := b.Metadata.GetBranch(name)
	if !ok {
		return fmt.Errorf("branch '%s' does not exist", name)
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

type BranchInfo struct {
	Name      string
	IsCurrent bool
	Branch    *storage.Branch
}

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

func (b *Brancher) CurrentBranch() string {
	return b.Metadata.CurrentBranch
}

func (b *Brancher) Status() (currentBranch string, branchCount int) {
	return b.Metadata.CurrentBranch, len(b.Metadata.Branches)
}

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

const DefaultStaleDays = 7

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
