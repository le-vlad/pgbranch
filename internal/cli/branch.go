package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/le-vlad/pgbranch/internal/core"
)

var branchCmd = &cobra.Command{
	Use:   "branch [name]",
	Short: "List or create branches",
	Long: `List all branches or create a new branch.

Without arguments, lists all branches.
With a name argument, creates a new branch from the current database state.

Examples:
  pgbranch branch           # List all branches
  pgbranch branch main      # Create branch 'main'
  pgbranch branch feature-x # Create branch 'feature-x'`,
	Args: cobra.MaximumNArgs(1),
	RunE: runBranch,
}

func runBranch(cmd *cobra.Command, args []string) error {
	brancher, err := core.NewBrancher()
	if err != nil {
		return err
	}

	if len(args) == 0 {
		return listBranches(brancher)
	}

	name := args[0]
	return createBranch(brancher, name)
}

func listBranches(b *core.Brancher) error {
	branches := b.ListBranches()

	if len(branches) == 0 {
		fmt.Println("No branches yet. Create one with: pgbranch branch <name>")
		return nil
	}

	green := color.New(color.FgGreen).SprintFunc()

	for _, info := range branches {
		if info.IsCurrent {
			fmt.Printf("* %s\n", green(info.Name))
		} else {
			fmt.Printf("  %s\n", info.Name)
		}
	}

	return nil
}

func createBranch(b *core.Brancher, name string) error {
	if err := b.CreateBranch(name); err != nil {
		return err
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("%s Created branch '%s'\n", green("✓"), name)

	return nil
}
