// Package gitrepo resolves the git context pgbranch operates in.
//
// pgbranch has to answer three questions that plain filesystem inspection
// cannot: where git looks for hooks, whether the current directory is a linked
// worktree, and which worktree (if any) already holds a given branch. Git
// answers all three, so this package shells out rather than guessing from
// directory layout. Directory layout in particular is misleading: Claude Code
// nests its worktrees at .claude/worktrees/<name> inside the main worktree, so
// walking up from a worktree can reach the main worktree's files.
package gitrepo

import (
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

// ErrNotARepository is returned by Discover when the working directory is not
// inside a git repository. Callers are expected to degrade gracefully: pgbranch
// works fine outside git, it just cannot offer worktree awareness.
var ErrNotARepository = errors.New("not a git repository")

// Worktree is one entry from `git worktree list`.
type Worktree struct {
	Path   string
	Head   string
	Branch string // empty when the worktree has a detached HEAD
	Locked bool
}

// Context describes the git worktree the process is running in.
type Context struct {
	// GitDir is this worktree's private git directory. For the main worktree
	// it equals CommonDir; for a linked worktree it is
	// <CommonDir>/worktrees/<name>.
	GitDir string

	// CommonDir is the git directory shared by every worktree of the repo.
	CommonDir string

	// Root is the top level of the current worktree.
	Root string

	// MainRoot is the top level of the main worktree.
	MainRoot string

	// Branch is the checked-out branch, or "" when HEAD is detached.
	Branch string

	// IsLinked reports whether this is a linked worktree rather than the main one.
	IsLinked bool
}

func git(args ...string) (string, error) {
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// resolve makes a git-reported path absolute and follows symlinks, so that
// paths originating from different git commands compare equal. On macOS this
// matters because /tmp is a symlink to /private/tmp.
func resolve(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		return real, nil
	}
	return abs, nil
}

// Discover inspects the working directory and returns its git context.
// It returns ErrNotARepository when there is no repository to inspect.
func Discover() (*Context, error) {
	if _, err := git("rev-parse", "--git-dir"); err != nil {
		return nil, ErrNotARepository
	}

	gitDir, err := git("rev-parse", "--absolute-git-dir")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve git dir: %w", err)
	}
	commonDir, err := git("rev-parse", "--git-common-dir")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve git common dir: %w", err)
	}
	root, err := git("rev-parse", "--show-toplevel")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve worktree root: %w", err)
	}

	// --git-common-dir may be reported relative to the working directory.
	if gitDir, err = resolve(gitDir); err != nil {
		return nil, err
	}
	if commonDir, err = resolve(commonDir); err != nil {
		return nil, err
	}
	if root, err = resolve(root); err != nil {
		return nil, err
	}

	// A detached HEAD has no symbolic ref; that is not an error.
	branch, _ := git("symbolic-ref", "--quiet", "--short", "HEAD")

	worktrees, err := List()
	if err != nil {
		return nil, err
	}
	mainRoot := root
	if len(worktrees) > 0 {
		mainRoot = worktrees[0].Path
	}

	return &Context{
		GitDir:    gitDir,
		CommonDir: commonDir,
		Root:      root,
		MainRoot:  mainRoot,
		Branch:    branch,
		IsLinked:  gitDir != commonDir,
	}, nil
}

// HooksDir returns the directory git actually reads hooks from.
//
// This must not be derived from --git-dir: hooks live in the common directory,
// so a --git-dir-based path inside a linked worktree points somewhere git never
// looks. `rev-parse --git-path hooks` resolves the common directory and honours
// core.hooksPath (husky, lefthook), which is the behaviour we want.
func HooksDir() (string, error) {
	path, err := git("rev-parse", "--git-path", "hooks")
	if err != nil {
		return "", ErrNotARepository
	}
	// The result is relative to the working directory when git feels like it.
	return resolve(path)
}

// List returns every worktree of the repository. The main worktree is first,
// which git guarantees.
func List() ([]Worktree, error) {
	out, err := git("worktree", "list", "--porcelain")
	if err != nil {
		return nil, fmt.Errorf("failed to list worktrees: %w", err)
	}

	var (
		worktrees []Worktree
		current   *Worktree
	)
	flush := func() {
		if current != nil {
			worktrees = append(worktrees, *current)
			current = nil
		}
	}

	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "worktree "):
			flush()
			path, err := resolve(strings.TrimPrefix(line, "worktree "))
			if err != nil {
				return nil, err
			}
			current = &Worktree{Path: path}
		case current == nil:
			continue
		case strings.HasPrefix(line, "HEAD "):
			current.Head = strings.TrimPrefix(line, "HEAD ")
		case strings.HasPrefix(line, "branch "):
			current.Branch = strings.TrimPrefix(strings.TrimPrefix(line, "branch "), "refs/heads/")
		case line == "locked" || strings.HasPrefix(line, "locked "):
			current.Locked = true
		}
	}
	flush()

	return worktrees, nil
}

// ClaimedBy reports the worktree that currently has branch checked out, other
// than the caller's own worktree.
//
// Git already refuses to check out one branch in two worktrees. pgbranch needs
// the same rule for its own commands, because `pgbranch checkout` bypasses git
// entirely and would otherwise clobber a database another worktree is using.
func (c *Context) ClaimedBy(branch string) (*Worktree, bool) {
	if branch == "" {
		return nil, false
	}
	worktrees, err := List()
	if err != nil {
		return nil, false
	}
	for i := range worktrees {
		wt := worktrees[i]
		if wt.Branch == branch && wt.Path != c.Root {
			return &wt, true
		}
	}
	return nil, false
}
