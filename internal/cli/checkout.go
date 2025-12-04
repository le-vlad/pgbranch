package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/le-vlad/pgbranch/internal/core"
)

var checkoutCmd = &cobra.Command{
	Use:   "checkout <branch>",
	Short: "Switch to a different branch",
	Long: `Switch to a different branch by restoring its snapshot.

This will:
1. Drop the current database
2. Create a fresh database
3. Restore the branch's snapshot

Warning: Any uncommitted changes to the current database will be lost.

Example:
  pgbranch checkout main
  pgbranch checkout feature-x`,
	Args: cobra.ExactArgs(1),
	RunE: runCheckout,
}

func runCheckout(cmd *cobra.Command, args []string) error {
	brancher, err := core.NewBrancher()
	if err != nil {
		return err
	}

	name := args[0]

	if brancher.CurrentBranch() == name {
		fmt.Printf("Already on branch '%s'\n", name)
		return nil
	}

	yellow := color.New(color.FgYellow).SprintFunc()
	fmt.Printf("%s Switching to branch '%s'...\n", yellow("→"), name)

	if err := brancher.Checkout(name); err != nil {
		return err
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("%s Switched to branch '%s'\n", green("✓"), name)

	return nil
}
