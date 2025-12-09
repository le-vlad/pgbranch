package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

const postCheckoutHook = `#!/bin/sh
# pgbranch post-checkout hook
# Automatically switches database branch when git branch changes

# post-checkout receives: previous HEAD, new HEAD, flag (1=branch checkout, 0=file checkout)
PREV_HEAD="$1"
NEW_HEAD="$2"
CHECKOUT_TYPE="$3"

# Only run on branch checkouts, not file checkouts
if [ "$CHECKOUT_TYPE" != "1" ]; then
    exit 0
fi

# Get the new branch name
BRANCH=$(git rev-parse --abbrev-ref HEAD)

# Skip if we're in detached HEAD state
if [ "$BRANCH" = "HEAD" ]; then
    exit 0
fi

# Check if pgbranch is initialized in this directory
if [ ! -d ".pgbranch" ]; then
    exit 0
fi

# Check if this branch exists in pgbranch
if pgbranch status 2>/dev/null | grep -q "Current branch:"; then
    # Try to checkout the branch, but don't fail if it doesn't exist
    if pgbranch checkout "$BRANCH" 2>/dev/null; then
        echo "pgbranch: Switched database to branch '$BRANCH'"
    fi
fi
`

var hookCmd = &cobra.Command{
	Use:   "hook",
	Short: "Manage git hooks for automatic branch switching",
	Long: `Manage git hooks that automatically switch database branches
when you switch git branches.

Subcommands:
  install   - Install the post-checkout git hook
  uninstall - Remove the post-checkout git hook`,
}

var hookInstallCmd = &cobra.Command{
	Use:   "install",
	Short: "Install git hook for automatic branch switching",
	Long: `Install a post-checkout git hook that automatically runs
'pgbranch checkout <branch>' when you switch git branches.

This allows seamless synchronization between your git branches
and database states.

Example:
  pgbranch hook install
  git checkout feature-x  # automatically runs: pgbranch checkout feature-x`,
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

func init() {
	hookCmd.AddCommand(hookInstallCmd)
	hookCmd.AddCommand(hookUninstallCmd)
}

func getGitHooksDir() (string, error) {
	// Find the git directory
	cmd := exec.Command("git", "rev-parse", "--git-dir")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("not a git repository")
	}

	gitDir := string(output[:len(output)-1])
	hooksDir := filepath.Join(gitDir, "hooks")

	return hooksDir, nil
}

func runHookInstall(cmd *cobra.Command, args []string) error {
	hooksDir, err := getGitHooksDir()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(hooksDir, 0755); err != nil {
		return fmt.Errorf("failed to create hooks directory: %w", err)
	}

	hookPath := filepath.Join(hooksDir, "post-checkout")

	if _, err := os.Stat(hookPath); err == nil {
		content, err := os.ReadFile(hookPath)
		if err != nil {
			return fmt.Errorf("failed to read existing hook: %w", err)
		}

		if string(content) == postCheckoutHook {
			fmt.Println("pgbranch hook is already installed")
			return nil
		}

		yellow := color.New(color.FgYellow).SprintFunc()
		fmt.Printf("%s A post-checkout hook already exists.\n", yellow("!"))
		fmt.Println("  To avoid conflicts, please manually integrate pgbranch into your existing hook.")
		fmt.Println("  Or backup and remove the existing hook, then run this command again.")
		return fmt.Errorf("existing hook found at %s", hookPath)
	}

	if err := os.WriteFile(hookPath, []byte(postCheckoutHook), 0755); err != nil {
		return fmt.Errorf("failed to write hook: %w", err)
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("%s Git hook installed successfully!\n", green("✓"))
	fmt.Println()
	fmt.Println("Now when you run 'git checkout <branch>', pgbranch will")
	fmt.Println("automatically switch to the matching database branch if it exists.")

	return nil
}

func runHookUninstall(cmd *cobra.Command, args []string) error {
	hooksDir, err := getGitHooksDir()
	if err != nil {
		return err
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

	if string(content) != postCheckoutHook {
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
