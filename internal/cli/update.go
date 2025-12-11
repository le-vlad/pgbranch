package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/le-vlad/pgbranch/internal/core"
)

var updateCmd = &cobra.Command{
	Use:   "update [branch]",
	Short: "Update a branch snapshot with current database state",
	Long: `Update a branch's snapshot to match the current database state.

Without arguments, updates the current branch.
With a name argument, updates the specified branch.

This is useful when you want to actualize a snapshot without switching branches.

Examples:
  pgbranch update           # Update current branch
  pgbranch update main      # Update 'main' branch`,
	Args: cobra.MaximumNArgs(1),
	RunE: runUpdate,
}

func runUpdate(cmd *cobra.Command, args []string) error {
	brancher, err := core.NewBrancher()
	if err != nil {
		return err
	}

	var name string
	if len(args) == 0 {
		name = brancher.CurrentBranch()
		if name == "" {
			return fmt.Errorf("no current branch. Specify a branch name or checkout a branch first")
		}
	} else {
		name = args[0]
	}

	yellow := color.New(color.FgYellow).SprintFunc()
	fmt.Printf("%s Updating branch '%s'...\n", yellow("→"), name)

	if err := brancher.UpdateBranch(name); err != nil {
		return err
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("%s Updated branch '%s' with current database state\n", green("✓"), name)

	return nil
}
