package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/le-vlad/pgbranch/internal/core"
	"github.com/le-vlad/pgbranch/internal/gitrepo"
	"github.com/le-vlad/pgbranch/pkg/config"
)

// hookMarker identifies a hook as pgbranch's, across versions. Byte-comparing
// the whole script made every change to it an unremovable "foreign hook".
const hookMarker = "pgbranch-managed-hook"

// postCheckoutHook delegates to the binary rather than reimplementing pgbranch
// in shell. The previous version grepped `pgbranch branch` output to decide what
// to do, and checked for a .pgbranch directory that never exists in a worktree.
const postCheckoutHook = `#!/bin/sh
# ` + hookMarker + `: switches the database branch when the git branch changes.
# Remove with: pgbranch hook uninstall

# Args: previous HEAD, new HEAD, 1 for a branch checkout and 0 for a file checkout.
[ "$3" = "1" ] || exit 0

# Do not break checkouts on machines without pgbranch installed.
command -v pgbranch >/dev/null 2>&1 || exit 0

exec pgbranch hook post-checkout "$1" "$2" "$3"
`

var hookCmd = &cobra.Command{
	Use:   "hook",
	Short: "Manage git hooks for automatic branch switching",
	Long: `Manage git hooks that automatically switch database branches
when you switch git branches.

The hook is shared by every worktree of the repository, and understands both:
in the main worktree it switches the working database, and in a linked worktree
it provisions that worktree its own database, leaving the main one untouched.

Subcommands:
  install   - Install the post-checkout git hook
  uninstall - Remove the post-checkout git hook`,
}

var hookInstallCmd = &cobra.Command{
	Use:   "install",
	Short: "Install git hook for automatic branch switching",
	Long: `Install a post-checkout git hook that keeps the database in step
with the git branch.

The hook is written to the directory git actually reads hooks from, which is
shared across worktrees and honours core.hooksPath.

Example:
  pgbranch hook install
  git checkout feature-x            # switches the database to feature-x
  git worktree add ../wt -b feat-y  # gives ../wt its own database`,
	RunE: runHookInstall,
}

var hookUninstallCmd = &cobra.Command{
	Use:   "uninstall",
	Short: "Remove the git hook",
	Long: `Remove the post-checkout git hook installed by pgbranch.

Example:
  pgbranch hook uninstall`,
	RunE: runHookUninstall,
}

// postCheckoutCmd is what the installed hook script invokes. Keeping the logic
// in Go means the hook can be a stable four-line stub.
var postCheckoutCmd = &cobra.Command{
	Use:    "post-checkout <prev-head> <new-head> <flag>",
	Short:  "Internal: run the post-checkout logic",
	Args:   cobra.MaximumNArgs(3),
	Hidden: true,
	RunE:   runPostCheckout,
}

func init() {
	hookCmd.AddCommand(hookInstallCmd)
	hookCmd.AddCommand(hookUninstallCmd)
	hookCmd.AddCommand(postCheckoutCmd)
}

func runHookInstall(cmd *cobra.Command, args []string) error {
	hooksDir, err := gitrepo.HooksDir()
	if err != nil {
		return fmt.Errorf("not a git repository")
	}

	if err := os.MkdirAll(hooksDir, 0755); err != nil {
		return fmt.Errorf("failed to create hooks directory: %w", err)
	}

	hookPath := filepath.Join(hooksDir, "post-checkout")

	if existing, err := os.ReadFile(hookPath); err == nil {
		switch {
		case string(existing) == postCheckoutHook:
			fmt.Println("pgbranch hook is already installed")
			return nil
		case strings.Contains(string(existing), hookMarker):
			// An older pgbranch hook. Upgrading is safe and expected.
			if err := os.WriteFile(hookPath, []byte(postCheckoutHook), 0755); err != nil {
				return fmt.Errorf("failed to upgrade hook: %w", err)
			}
			green := color.New(color.FgGreen).SprintFunc()
			fmt.Printf("%s Upgraded the pgbranch hook at %s\n", green("✓"), hookPath)
			return nil
		default:
			yellow := color.New(color.FgYellow).SprintFunc()
			fmt.Printf("%s A post-checkout hook already exists.\n", yellow("!"))
			fmt.Println("  To avoid conflicts, please manually integrate pgbranch into your existing hook:")
			fmt.Println("    pgbranch hook post-checkout \"$1\" \"$2\" \"$3\"")
			return fmt.Errorf("existing hook found at %s", hookPath)
		}
	}

	if err := os.WriteFile(hookPath, []byte(postCheckoutHook), 0755); err != nil {
		return fmt.Errorf("failed to write hook: %w", err)
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("%s Git hook installed at %s\n", green("✓"), hookPath)
	fmt.Println()
	fmt.Println("Switching git branches now switches the database branch.")
	fmt.Println("New worktrees get their own database; the main one is left alone.")

	return nil
}

func runHookUninstall(cmd *cobra.Command, args []string) error {
	hooksDir, err := gitrepo.HooksDir()
	if err != nil {
		return fmt.Errorf("not a git repository")
	}

	hookPath := filepath.Join(hooksDir, "post-checkout")

	content, err := os.ReadFile(hookPath)
	if os.IsNotExist(err) {
		fmt.Println("No post-checkout hook found")
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read hook: %w", err)
	}

	if !strings.Contains(string(content), hookMarker) {
		yellow := color.New(color.FgYellow).SprintFunc()
		fmt.Printf("%s The post-checkout hook was not installed by pgbranch.\n", yellow("!"))
		fmt.Println("  Refusing to remove it to avoid breaking your workflow.")
		return fmt.Errorf("hook was not installed by pgbranch")
	}

	if err := os.Remove(hookPath); err != nil {
		return fmt.Errorf("failed to remove hook: %w", err)
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("%s Git hook uninstalled successfully\n", green("✓"))

	return nil
}

// runPostCheckout must never fail a git checkout. Anything unexpected is
// reported and swallowed: a database that did not switch is recoverable, a
// checkout that refuses to complete is not.
func runPostCheckout(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true

	if len(args) == 3 && args[2] != "1" {
		return nil // file checkout, not a branch change
	}
	if !config.IsInitialized() {
		return nil
	}

	ctx, err := gitrepo.Discover()
	if err != nil || ctx.Branch == "" {
		return nil // not a repository, or a detached HEAD
	}

	brancher, err := core.NewBrancher()
	if err != nil {
		return nil
	}

	if ctx.IsLinked {
		return syncLinkedWorktree(brancher)
	}
	return syncMainWorktree(brancher, ctx.Branch)
}

// syncLinkedWorktree gives the worktree its own database, without disturbing
// the main working database.
func syncLinkedWorktree(brancher *core.Brancher) error {
	state, err := brancher.EnsureWorktreeDatabase("")
	if err != nil {
		warn("could not prepare a database for this worktree: %v", err)
		return nil
	}

	envPath, err := brancher.WriteEnvFile()
	if err != nil {
		warn("could not write the worktree env file: %v", err)
		return nil
	}

	green := color.New(color.FgGreen).SprintFunc()
	verb := "using"
	if state.Created {
		verb = "created"
	}
	fmt.Printf("%s pgbranch: %s database '%s' for this worktree (main database '%s' untouched)\n",
		green("✓"), verb, state.Database, state.MainDB)
	fmt.Printf("  connection string written to %s\n", relativeToCwd(envPath))
	return nil
}

// syncMainWorktree preserves the original behaviour: the configured database is
// swapped to match the branch, so DATABASE_URL never has to change.
func syncMainWorktree(brancher *core.Brancher, branch string) error {
	if brancher.CurrentBranch() == branch {
		return nil
	}

	if !brancher.Metadata.BranchExists(branch) {
		if err := brancher.CreateBranch(branch); err != nil {
			warn("could not create database branch '%s': %v", branch, err)
			return nil
		}
		fmt.Printf("pgbranch: created database branch '%s'\n", branch)
	}

	if err := brancher.Checkout(branch); err != nil {
		var claimed *core.ErrBranchClaimed
		if errors.As(err, &claimed) {
			warn("branch '%s' is in use by the worktree at %s; database not switched",
				claimed.Branch, claimed.Worktree)
			return nil
		}
		warn("could not switch database to '%s': %v", branch, err)
		return nil
	}

	fmt.Printf("pgbranch: switched database to branch '%s'\n", branch)
	return nil
}

func warn(format string, args ...any) {
	yellow := color.New(color.FgYellow).SprintFunc()
	fmt.Fprintf(os.Stderr, "%s pgbranch: %s\n", yellow("!"), fmt.Sprintf(format, args...))
}

func relativeToCwd(path string) string {
	cwd, err := os.Getwd()
	if err != nil {
		return path
	}
	if rel, err := filepath.Rel(cwd, path); err == nil && !strings.HasPrefix(rel, "..") {
		return rel
	}
	return path
}
