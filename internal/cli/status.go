package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/le-vlad/pgbranch/internal/core"
	"github.com/le-vlad/pgbranch/pkg/config"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show current branch and status",
	Long: `Show the current branch and repository status.

Example:
  pgbranch status`,
	RunE: runStatus,
}

func runStatus(cmd *cobra.Command, args []string) error {
	brancher, err := core.NewBrancher()
	if err != nil {
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	currentBranch, branchCount := brancher.Status()

	green := color.New(color.FgGreen).SprintFunc()
	cyan := color.New(color.FgCyan).SprintFunc()
	dim := color.New(color.Faint).SprintFunc()

	state, err := brancher.Inspect()
	if err != nil {
		return err
	}

	// In a linked worktree the branch is fixed by git and the database is the
	// branch's own, so reporting the configured database would be misleading.
	if state.IsLinked {
		fmt.Printf("Worktree: %s\n", cyan(state.WorktreeAt))
		fmt.Printf("Database: %s\n", cyan(state.Database))
		fmt.Printf("Host:     %s:%d\n", cfg.Host, cfg.Port)
		fmt.Println()
		fmt.Printf("On branch: %s %s\n", green(state.Branch), dim("(pinned by git)"))
		fmt.Printf("Main database %s is untouched by this worktree.\n", dim(state.MainDB))
		fmt.Printf("Branches:  %d\n", branchCount)
		return nil
	}

	fmt.Printf("Database: %s\n", cyan(cfg.Database))
	fmt.Printf("Host:     %s:%d\n", cfg.Host, cfg.Port)
	fmt.Println()

	if currentBranch == "" {
		yellow := color.New(color.FgYellow).SprintFunc()
		fmt.Printf("On branch: %s\n", yellow("(none)"))
	} else {
		fmt.Printf("On branch: %s\n", green(currentBranch))
	}

	fmt.Printf("Branches:  %d\n", branchCount)

	return nil
}
