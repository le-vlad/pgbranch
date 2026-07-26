package gitrepo

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func run(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %v\n%s", args, err, out)
	}
}

func chdir(t *testing.T, dir string) {
	t.Helper()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })
}

// newRepo builds a repository with one commit and returns its root.
func newRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	run(t, root, "git", "init", "-q", "-b", "main")
	run(t, root, "git", "config", "user.email", "test@example.com")
	run(t, root, "git", "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("hi"), 0644); err != nil {
		t.Fatal(err)
	}
	run(t, root, "git", "add", "a.txt")
	run(t, root, "git", "commit", "-qm", "init")
	return root
}

func TestDiscoverMainWorktree(t *testing.T) {
	root := newRepo(t)
	chdir(t, root)

	ctx, err := Discover()
	if err != nil {
		t.Fatal(err)
	}
	if ctx.IsLinked {
		t.Error("main worktree reported as linked")
	}
	if ctx.GitDir != ctx.CommonDir {
		t.Errorf("main worktree: GitDir %q != CommonDir %q", ctx.GitDir, ctx.CommonDir)
	}
	if ctx.Branch != "main" {
		t.Errorf("Branch = %q, want main", ctx.Branch)
	}
	if ctx.Root != root || ctx.MainRoot != root {
		t.Errorf("Root = %q, MainRoot = %q, want %q", ctx.Root, ctx.MainRoot, root)
	}
}

func TestDiscoverLinkedWorktree(t *testing.T) {
	root := newRepo(t)
	wt := filepath.Join(root, "wt")
	run(t, root, "git", "worktree", "add", "-q", wt, "-b", "feature")
	chdir(t, wt)

	ctx, err := Discover()
	if err != nil {
		t.Fatal(err)
	}
	if !ctx.IsLinked {
		t.Error("linked worktree reported as main")
	}
	if ctx.GitDir == ctx.CommonDir {
		t.Error("linked worktree: GitDir should differ from CommonDir")
	}
	if ctx.Branch != "feature" {
		t.Errorf("Branch = %q, want feature", ctx.Branch)
	}
	if ctx.Root != wt {
		t.Errorf("Root = %q, want %q", ctx.Root, wt)
	}
	if ctx.MainRoot != root {
		t.Errorf("MainRoot = %q, want %q", ctx.MainRoot, root)
	}
}

// A nested worktree mirrors Claude Code's .claude/worktrees/<name> layout,
// where walking up the filesystem would wrongly find the main worktree.
func TestDiscoverNestedWorktreeIsStillLinked(t *testing.T) {
	root := newRepo(t)
	wt := filepath.Join(root, ".claude", "worktrees", "agent")
	run(t, root, "git", "worktree", "add", "-q", wt, "-b", "worktree-agent")
	chdir(t, wt)

	ctx, err := Discover()
	if err != nil {
		t.Fatal(err)
	}
	if !ctx.IsLinked {
		t.Error("nested worktree must be detected as linked")
	}
	if ctx.MainRoot != root {
		t.Errorf("MainRoot = %q, want %q", ctx.MainRoot, root)
	}
}

func TestDiscoverDetachedHead(t *testing.T) {
	root := newRepo(t)
	chdir(t, root)
	run(t, root, "git", "checkout", "-q", "--detach")

	ctx, err := Discover()
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Branch != "" {
		t.Errorf("Branch = %q, want empty for detached HEAD", ctx.Branch)
	}
}

func TestDiscoverOutsideRepository(t *testing.T) {
	dir := t.TempDir()
	chdir(t, dir)

	// t.TempDir may sit under a repository on some machines; only assert the
	// error when git agrees there is no repository.
	if err := exec.Command("git", "rev-parse", "--git-dir").Run(); err == nil {
		t.Skip("temp dir is inside a git repository")
	}
	if _, err := Discover(); err != ErrNotARepository {
		t.Errorf("Discover() error = %v, want ErrNotARepository", err)
	}
}

// Hooks live in the common directory. Deriving them from --git-dir inside a
// linked worktree yields a path git never reads.
func TestHooksDirIsSharedAcrossWorktrees(t *testing.T) {
	root := newRepo(t)
	wt := filepath.Join(root, "wt")
	run(t, root, "git", "worktree", "add", "-q", wt, "-b", "feature")

	chdir(t, root)
	fromMain, err := HooksDir()
	if err != nil {
		t.Fatal(err)
	}

	if err := os.Chdir(wt); err != nil {
		t.Fatal(err)
	}
	fromWorktree, err := HooksDir()
	if err != nil {
		t.Fatal(err)
	}

	if fromMain != fromWorktree {
		t.Errorf("hooks dir differs between worktrees: %q vs %q", fromMain, fromWorktree)
	}
	if !filepath.IsAbs(fromWorktree) {
		t.Errorf("hooks dir %q is not absolute", fromWorktree)
	}
}

func TestHooksDirHonoursCoreHooksPath(t *testing.T) {
	root := newRepo(t)
	custom := filepath.Join(root, "myhooks")
	if err := os.MkdirAll(custom, 0755); err != nil {
		t.Fatal(err)
	}
	run(t, root, "git", "config", "core.hooksPath", custom)
	chdir(t, root)

	got, err := HooksDir()
	if err != nil {
		t.Fatal(err)
	}
	if got != custom {
		t.Errorf("HooksDir() = %q, want %q", got, custom)
	}
}

func TestListReportsMainWorktreeFirst(t *testing.T) {
	root := newRepo(t)
	wt := filepath.Join(root, "wt")
	run(t, root, "git", "worktree", "add", "-q", wt, "-b", "feature")
	chdir(t, wt)

	worktrees, err := List()
	if err != nil {
		t.Fatal(err)
	}
	if len(worktrees) != 2 {
		t.Fatalf("got %d worktrees, want 2", len(worktrees))
	}
	if worktrees[0].Path != root {
		t.Errorf("first worktree = %q, want main worktree %q", worktrees[0].Path, root)
	}
	if worktrees[0].Branch != "main" || worktrees[1].Branch != "feature" {
		t.Errorf("branches = %q, %q; want main, feature", worktrees[0].Branch, worktrees[1].Branch)
	}
}

func TestClaimedBy(t *testing.T) {
	root := newRepo(t)
	wt := filepath.Join(root, "wt")
	run(t, root, "git", "worktree", "add", "-q", wt, "-b", "feature")
	chdir(t, root)

	ctx, err := Discover()
	if err != nil {
		t.Fatal(err)
	}

	claim, ok := ctx.ClaimedBy("feature")
	if !ok {
		t.Fatal("feature should be claimed by the linked worktree")
	}
	if claim.Path != wt {
		t.Errorf("claim path = %q, want %q", claim.Path, wt)
	}

	// A worktree does not claim its own branch against itself.
	if _, ok := ctx.ClaimedBy("main"); ok {
		t.Error("main is checked out here; it must not report as claimed elsewhere")
	}
	if _, ok := ctx.ClaimedBy("nonexistent"); ok {
		t.Error("unknown branch reported as claimed")
	}
	if _, ok := ctx.ClaimedBy(""); ok {
		t.Error("empty branch reported as claimed")
	}
}

func TestListDetectsLockedWorktree(t *testing.T) {
	root := newRepo(t)
	wt := filepath.Join(root, "wt")
	run(t, root, "git", "worktree", "add", "-q", wt, "-b", "feature")
	run(t, root, "git", "worktree", "lock", wt)
	chdir(t, root)

	worktrees, err := List()
	if err != nil {
		t.Fatal(err)
	}
	if len(worktrees) != 2 || !worktrees[1].Locked {
		t.Errorf("locked worktree not detected: %+v", worktrees)
	}
}
