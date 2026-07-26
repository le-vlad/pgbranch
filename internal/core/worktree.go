package core

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/le-vlad/pgbranch/internal/gitrepo"
	"github.com/le-vlad/pgbranch/internal/storage"
	"github.com/le-vlad/pgbranch/pkg/config"
)

// EnvFileName is written into each worktree so applications and agents can
// discover the database that worktree owns.
const EnvFileName = "env"

// AgentBranchPrefix marks branches created by an agent harness. Claude Code
// names worktree branches `worktree-<name>`; databases behind such branches are
// disposable, and prune treats them accordingly.
const AgentBranchPrefix = "worktree-"

// ErrBranchClaimed reports that another worktree has the branch checked out.
type ErrBranchClaimed struct {
	Branch   string
	Worktree string
}

func (e *ErrBranchClaimed) Error() string {
	return fmt.Sprintf("branch '%s' is checked out in the worktree at %s", e.Branch, e.Worktree)
}

// gitContext returns the git context, or nil when pgbranch is running outside a
// repository. Every worktree feature degrades to a no-op in that case.
func gitContext() *gitrepo.Context {
	ctx, err := gitrepo.Discover()
	if err != nil {
		return nil
	}
	return ctx
}

// ensureNotClaimed refuses operations on a branch that another worktree holds.
//
// Git already prevents two worktrees from checking out one branch, which is
// what makes "one live database per branch" safe. pgbranch commands bypass git,
// so they need the same rule: without it, `pgbranch checkout feature` run in the
// main worktree would template from — and terminate connections to — the
// database a linked worktree is actively using.
func (b *Brancher) ensureNotClaimed(branch string) error {
	ctx := gitContext()
	if ctx == nil {
		return nil
	}
	if wt, claimed := ctx.ClaimedBy(branch); claimed {
		return &ErrBranchClaimed{Branch: branch, Worktree: wt.Path}
	}
	return nil
}

// ensureMainWorktree refuses operations that only make sense where a working
// copy of the database exists.
//
// A linked worktree is pinned to one branch by git and addresses that branch's
// database directly. There is no working copy to swap out and nothing to save
// back, so checkout and update have no meaning there.
func (b *Brancher) ensureMainWorktree(operation string) error {
	ctx := gitContext()
	if ctx == nil || !ctx.IsLinked {
		return nil
	}
	return fmt.Errorf(
		"cannot %s from a linked worktree: git pins it to branch '%s', which already has its own database\n"+
			"  run 'pgbranch env' to see this worktree's database, or %s from the main worktree at %s",
		operation, ctx.Branch, operation, ctx.MainRoot)
}

// EnsureCheckoutAllowed reports whether Checkout would be permitted, so callers
// can fail before printing progress they are about to contradict. Checkout
// re-checks; this is for presentation, not for safety.
func (b *Brancher) EnsureCheckoutAllowed(name string) error {
	if err := b.ensureMainWorktree("checkout"); err != nil {
		return err
	}
	return b.ensureNotClaimed(name)
}

// WorkingDB reports the database this worktree should connect to.
//
// The main worktree keeps using the configured database, so an existing
// DATABASE_URL never has to change. A linked worktree addresses its branch's
// database directly, which is what lets several branches be live at once.
func (b *Brancher) WorkingDB() (string, error) {
	ctx := gitContext()
	if ctx == nil || !ctx.IsLinked {
		return b.Config.Database, nil
	}
	if ctx.Branch == "" {
		return "", errors.New("worktree has a detached HEAD; no branch database to use")
	}
	branch, ok := b.Metadata.GetBranch(ctx.Branch)
	if !ok {
		return "", fmt.Errorf("branch '%s' has no database; run 'pgbranch branch %s'", ctx.Branch, ctx.Branch)
	}
	return branch.Snapshot, nil
}

// CreateBranchFrom creates a branch whose database is copied from source.
//
// PostgreSQL refuses `CREATE DATABASE ... TEMPLATE t` while anything is
// connected to t, and CreateDatabaseFromTemplate works around that by
// terminating those connections. Templating from the live working database
// would therefore drop the running application's connections every time a
// worktree is created. Copying from another branch's database avoids that,
// because nothing is connected to it.
func (b *Brancher) CreateBranchFrom(name, sourceBranch string) error {
	if b.Metadata.BranchExists(name) {
		return fmt.Errorf("branch '%s' already exists", name)
	}

	snapshotDBName := storage.SnapshotDBName(b.Config.Database, name)

	sourceDB := b.Config.Database
	if sourceBranch != "" {
		source, ok := b.Metadata.GetBranch(sourceBranch)
		if !ok {
			return fmt.Errorf("source branch '%s' does not exist", sourceBranch)
		}
		sourceDB = source.Snapshot
	}

	if err := b.Client.CreateDatabaseFromTemplate(sourceDB, snapshotDBName); err != nil {
		return fmt.Errorf("failed to create database for branch '%s': %w", name, err)
	}

	b.Metadata.AddBranch(name, sourceBranch, snapshotDBName)

	if err := b.Metadata.Save(); err != nil {
		b.Client.DeleteSnapshot(snapshotDBName)
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// WorktreeState describes the database situation of the current worktree.
type WorktreeState struct {
	IsLinked   bool
	Branch     string
	Database   string
	MainDB     string
	Created    bool   // the database was created by this call
	WorktreeAt string // filesystem root of this worktree
}

// Inspect reports the worktree's database situation without changing anything.
// Database is empty when the branch has no database yet.
func (b *Brancher) Inspect() (*WorktreeState, error) {
	ctx := gitContext()
	if ctx == nil {
		return &WorktreeState{Database: b.Config.Database, MainDB: b.Config.Database}, nil
	}

	state := &WorktreeState{
		IsLinked:   ctx.IsLinked,
		Branch:     ctx.Branch,
		MainDB:     b.Config.Database,
		WorktreeAt: ctx.Root,
	}
	if !ctx.IsLinked {
		state.Database = b.Config.Database
		return state, nil
	}
	if branch, ok := b.Metadata.GetBranch(ctx.Branch); ok {
		state.Database = branch.Snapshot
	}
	return state, nil
}

// EnsureWorktreeDatabase guarantees that the current worktree has a database,
// creating one from parentBranch when the branch is new. It is idempotent, so
// it is safe from a post-checkout hook and from every session start.
//
// In the main worktree it reports state without touching anything: switching
// the main working database is `pgbranch checkout`'s job, not this function's.
func (b *Brancher) EnsureWorktreeDatabase(parentBranch string) (*WorktreeState, error) {
	ctx := gitContext()
	if ctx == nil {
		return nil, gitrepo.ErrNotARepository
	}

	state := &WorktreeState{
		IsLinked:   ctx.IsLinked,
		Branch:     ctx.Branch,
		MainDB:     b.Config.Database,
		WorktreeAt: ctx.Root,
	}

	if !ctx.IsLinked {
		state.Database = b.Config.Database
		return state, nil
	}
	if ctx.Branch == "" {
		return nil, errors.New("worktree has a detached HEAD; check out a branch first")
	}

	if branch, ok := b.Metadata.GetBranch(ctx.Branch); ok {
		state.Database = branch.Snapshot
		return state, nil
	}

	// Creating a branch is a read-modify-write of metadata that every worktree
	// shares, so re-read it under the lock: another worktree may have been
	// created since this process loaded it.
	err := storage.WithLock(func() error {
		meta, err := storage.LoadMetadata()
		if err != nil {
			return err
		}
		b.Metadata = meta

		if branch, ok := meta.GetBranch(ctx.Branch); ok {
			state.Database = branch.Snapshot
			return nil // another process got here first
		}

		// Prefer the parent's database as the template. Falling back to the
		// main working database is correct but costs the caller's connections.
		if parentBranch == "" {
			parentBranch = meta.CurrentBranch
		}
		if _, ok := meta.GetBranch(parentBranch); !ok {
			parentBranch = ""
		}

		if err := b.CreateBranchFrom(ctx.Branch, parentBranch); err != nil {
			return err
		}

		branch, _ := b.Metadata.GetBranch(ctx.Branch)
		if strings.HasPrefix(ctx.Branch, AgentBranchPrefix) {
			branch.Ephemeral = true
			if err := b.Metadata.Save(); err != nil {
				return fmt.Errorf("failed to save metadata: %w", err)
			}
		}
		state.Database = branch.Snapshot
		state.Created = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	return state, nil
}

// EnvFilePath is where the current worktree's env file lives.
func EnvFilePath() (string, error) {
	ctx := gitContext()
	if ctx == nil {
		root, err := config.GetRootDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(root, EnvFileName), nil
	}
	return filepath.Join(ctx.Root, config.DirName, EnvFileName), nil
}

// WriteEnvFile records the worktree's DATABASE_URL in a file pgbranch owns.
//
// pgbranch deliberately does not edit the user's .env: a fresh worktree has no
// .env to edit (it is gitignored and so absent from a clean checkout), and
// rewriting a DSN in place risks mangling credentials and query parameters.
// Sourcing this file, or `eval $(pgbranch env)`, is the supported path.
func (b *Brancher) WriteEnvFile() (string, error) {
	dbName, err := b.WorkingDB()
	if err != nil {
		return "", err
	}
	path, err := EnvFilePath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", fmt.Errorf("failed to create %s: %w", filepath.Dir(path), err)
	}

	// The file carries a password; keep it off other users' terminals.
	body := fmt.Sprintf("# Written by pgbranch. Do not edit.\nDATABASE_URL=%s\nPGDATABASE=%s\n",
		b.Config.ConnectionURLForDB(dbName), dbName)
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		return "", fmt.Errorf("failed to write %s: %w", path, err)
	}
	return path, nil
}

// OrphanedAgentBranches lists agent-created branches whose worktree is gone.
//
// Neither git nor Claude Code emits a reliable signal when a worktree is
// removed — `git worktree remove` fires no hook, and Claude's WorktreeRemove
// hook does not fire on session end — so these are reclaimed by reconciliation.
func (b *Brancher) OrphanedAgentBranches() ([]string, error) {
	ctx := gitContext()
	if ctx == nil {
		return nil, nil
	}
	worktrees, err := gitrepo.List()
	if err != nil {
		return nil, err
	}

	live := make(map[string]bool, len(worktrees))
	for _, wt := range worktrees {
		if wt.Branch != "" {
			live[wt.Branch] = true
		}
	}

	var orphans []string
	for name, branch := range b.Metadata.Branches {
		if !branch.Ephemeral || live[name] {
			continue
		}
		orphans = append(orphans, name)
	}
	return orphans, nil
}
