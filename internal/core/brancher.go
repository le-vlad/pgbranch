package core

import (
	"fmt"
	"os"
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

	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if err := storage.EnsureSnapshotsDir(); err != nil {
		return fmt.Errorf("failed to create snapshots directory: %w", err)
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

	snapshotFile := storage.SnapshotFilename(name)
	snapshotPath, err := storage.GetSnapshotPath(snapshotFile)
	if err != nil {
		return err
	}

	if err := b.Client.Dump(snapshotPath); err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	parent := b.Metadata.CurrentBranch
	b.Metadata.AddBranch(name, parent, snapshotFile)

	if err := b.Metadata.Save(); err != nil {
		os.Remove(snapshotPath)
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

func (b *Brancher) Checkout(name string) error {
	branch, ok := b.Metadata.GetBranch(name)
	if !ok {
		return fmt.Errorf("branch '%s' does not exist", name)
	}

	snapshotPath, err := storage.GetSnapshotPath(branch.Snapshot)
	if err != nil {
		return err
	}

	exists, err := storage.SnapshotExists(branch.Snapshot)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("snapshot file not found for branch '%s'", name)
	}

	if err := b.Client.RestoreClean(snapshotPath); err != nil {
		return fmt.Errorf("failed to restore branch: %w", err)
	}

	b.Metadata.CurrentBranch = name
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

	if err := storage.DeleteSnapshot(branch.Snapshot); err != nil {
		return fmt.Errorf("failed to delete snapshot: %w", err)
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

	snapshotPath, err := storage.GetSnapshotPath(branch.Snapshot)
	if err != nil {
		return err
	}

	if err := b.Client.Dump(snapshotPath); err != nil {
		return fmt.Errorf("failed to update snapshot: %w", err)
	}

	return nil
}
