package core

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/le-vlad/pgbranch/internal/storage"
	"github.com/le-vlad/pgbranch/pkg/config"
)

// These exercise the worktree bookkeeping, which never reaches PostgreSQL. The
// paths that do create databases are covered by the integration tests.

func gitRun(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func chdir(t *testing.T, dir string) {
	t.Helper()
	orig, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() { _ = os.Chdir(orig) })
}

// newRepoWithWorktree returns a repository on branch "main" plus a linked
// worktree on branch "feature".
func newRepoWithWorktree(t *testing.T) (mainRoot, worktree string) {
	t.Helper()
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	require.NoError(t, err)

	gitRun(t, root, "init", "-q", "-b", "main")
	gitRun(t, root, "config", "user.email", "test@example.com")
	gitRun(t, root, "config", "user.name", "test")
	require.NoError(t, os.WriteFile(filepath.Join(root, "a.txt"), []byte("hi"), 0644))
	gitRun(t, root, "add", "a.txt")
	gitRun(t, root, "commit", "-qm", "init")

	wt := filepath.Join(root, "wt")
	gitRun(t, root, "worktree", "add", "-q", wt, "-b", "feature")
	return root, wt
}

func testBrancher(branches ...string) *Brancher {
	meta := storage.NewMetadata()
	for _, name := range branches {
		meta.AddBranch(name, "", storage.SnapshotDBName("appdb", name))
	}
	if len(branches) > 0 {
		meta.CurrentBranch = branches[0]
	}
	return &Brancher{
		Config:   &config.Config{Database: "appdb", Host: "localhost", Port: 5432, User: "u"},
		Metadata: meta,
	}
}

// The main worktree keeps the configured database, so an existing DATABASE_URL
// never has to change.
func TestWorkingDBInMainWorktree(t *testing.T) {
	root, _ := newRepoWithWorktree(t)
	chdir(t, root)

	db, err := testBrancher("main", "feature").WorkingDB()
	require.NoError(t, err)
	assert.Equal(t, "appdb", db)
}

// A linked worktree addresses its branch's database directly, which is what
// lets several branches be live at once.
func TestWorkingDBInLinkedWorktree(t *testing.T) {
	_, wt := newRepoWithWorktree(t)
	chdir(t, wt)

	db, err := testBrancher("main", "feature").WorkingDB()
	require.NoError(t, err)
	assert.Equal(t, storage.SnapshotDBName("appdb", "feature"), db)
	assert.NotEqual(t, "appdb", db)
}

func TestWorkingDBInLinkedWorktreeWithoutBranchDatabase(t *testing.T) {
	_, wt := newRepoWithWorktree(t)
	chdir(t, wt)

	_, err := testBrancher("main").WorkingDB()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no database")
}

func TestWorkingDBOutsideGitRepository(t *testing.T) {
	dir := t.TempDir()
	chdir(t, dir)
	if err := exec.Command("git", "rev-parse", "--git-dir").Run(); err == nil {
		t.Skip("temp dir is inside a git repository")
	}

	db, err := testBrancher("main").WorkingDB()
	require.NoError(t, err)
	assert.Equal(t, "appdb", db)
}

// Git refuses to check one branch out in two worktrees; pgbranch commands
// bypass git and need the same rule, or they clobber a live database.
func TestCheckoutRefusesBranchClaimedByAnotherWorktree(t *testing.T) {
	root, wt := newRepoWithWorktree(t)
	chdir(t, root)

	err := testBrancher("main", "feature").EnsureCheckoutAllowed("feature")

	require.Error(t, err)
	var claimed *ErrBranchClaimed
	require.ErrorAs(t, err, &claimed)
	assert.Equal(t, "feature", claimed.Branch)
	assert.Equal(t, wt, claimed.Worktree)
}

func TestCheckoutAllowedForUnclaimedBranch(t *testing.T) {
	root, _ := newRepoWithWorktree(t)
	chdir(t, root)

	assert.NoError(t, testBrancher("main", "other").EnsureCheckoutAllowed("other"))
}

// A linked worktree is pinned to its branch by git, so there is no working copy
// to swap and nothing to save back.
func TestCheckoutRefusedInsideLinkedWorktree(t *testing.T) {
	_, wt := newRepoWithWorktree(t)
	chdir(t, wt)

	err := testBrancher("main", "feature").EnsureCheckoutAllowed("main")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "linked worktree")
}

func TestInspectReportsLinkedWorktree(t *testing.T) {
	root, wt := newRepoWithWorktree(t)
	chdir(t, wt)

	state, err := testBrancher("main", "feature").Inspect()
	require.NoError(t, err)

	assert.True(t, state.IsLinked)
	assert.Equal(t, "feature", state.Branch)
	assert.Equal(t, "appdb", state.MainDB)
	assert.Equal(t, wt, state.WorktreeAt)
	assert.NotEqual(t, root, state.WorktreeAt)
}

func TestInspectDoesNotCreateAnything(t *testing.T) {
	_, wt := newRepoWithWorktree(t)
	chdir(t, wt)

	brancher := testBrancher("main") // "feature" deliberately absent
	state, err := brancher.Inspect()
	require.NoError(t, err)

	assert.Empty(t, state.Database, "branch has no database yet")
	assert.False(t, brancher.Metadata.BranchExists("feature"), "Inspect must not create a branch")
}

func TestEnvFileGoesInsideTheWorktree(t *testing.T) {
	root, wt := newRepoWithWorktree(t)
	chdir(t, wt)

	brancher := testBrancher("main", "feature")
	path, err := brancher.WriteEnvFile()
	require.NoError(t, err)

	assert.Equal(t, filepath.Join(wt, config.DirName, EnvFileName), path)

	body, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(body), storage.SnapshotDBName("appdb", "feature"))
	assert.NotContains(t, string(body), "/appdb?", "must not point at the main database")

	// The env file carries a password.
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())

	// The main worktree must not have acquired one.
	_, err = os.Stat(filepath.Join(root, config.DirName, EnvFileName))
	assert.True(t, os.IsNotExist(err))
}

// Nothing announces a worktree's removal, so orphans are found by comparing
// metadata against `git worktree list`.
func TestOrphanedAgentBranches(t *testing.T) {
	root, _ := newRepoWithWorktree(t)
	chdir(t, root)

	brancher := testBrancher("main", "feature")
	brancher.Metadata.AddBranch("worktree-gone", "", "appdb_pgbranch_worktree_gone_aaaaaa").Ephemeral = true
	brancher.Metadata.AddBranch("worktree-live", "", "appdb_pgbranch_worktree_live_bbbbbb").Ephemeral = true
	gitRun(t, root, "worktree", "add", "-q", filepath.Join(root, "live"), "-b", "worktree-live")

	orphans, err := brancher.OrphanedAgentBranches()
	require.NoError(t, err)

	assert.Equal(t, []string{"worktree-gone"}, orphans)
}

// A human's branch is not disposable, even once its worktree is gone.
func TestOrphanedAgentBranchesIgnoresNonEphemeralBranches(t *testing.T) {
	root, _ := newRepoWithWorktree(t)
	chdir(t, root)

	brancher := testBrancher("main")
	brancher.Metadata.AddBranch("abandoned", "", "appdb_pgbranch_abandoned_cccccc")

	orphans, err := brancher.OrphanedAgentBranches()
	require.NoError(t, err)
	assert.Empty(t, orphans)
}
