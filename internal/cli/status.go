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
