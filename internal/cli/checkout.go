package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/le-vlad/pgbranch/internal/core"
)

func showStaleWarning(brancher *core.Brancher) {
	staleBranches := brancher.GetStaleBranches(core.DefaultStaleDays)
	if len(staleBranches) == 0 {
		return
	}

	yellow := color.New(color.FgYellow, color.Bold).SprintFunc()
	orange := color.New(color.FgHiYellow).SprintFunc()

	fmt.Println()
	fmt.Printf("%s You have %s stale branch(es) not accessed in %d+ days.\n",
		yellow("!"),
		orange(fmt.Sprintf("%d", len(staleBranches))),
		core.DefaultStaleDays,
	)
	fmt.Printf("  Run '%s' to clean up stale database clones.\n", orange("pgbranch prune"))
}

var autoCreateBranch bool

var checkoutCmd = &cobra.Command{
	Use:   "checkout <branch>",
	Short: "Switch to a different branch",
	Long: `Switch to a different branch by restoring its snapshot.

This will:
1. Save the current branch's database state
2. Drop the current database
3. Restore the target branch's snapshot

Use -b to create a new branch and switch to it.

Example:
  pgbranch checkout main
  pgbranch checkout feature-x
  pgbranch checkout -b new-feature`,
	Args: cobra.ExactArgs(1),
	RunE: runCheckout,
}

func init() {
	checkoutCmd.Flags().BoolVarP(&autoCreateBranch, "branch", "b", false, "Create a new branch and switch to it")
}

func runCheckout(cmd *cobra.Command, args []string) error {
	brancher, err := core.NewBrancher()
	if err != nil {
		return err
	}

	name := args[0]

	if autoCreateBranch {
		if brancher.Metadata.BranchExists(name) {
			return fmt.Errorf("fatal: a branch named '%s' already exists", name)
		}

		yellow := color.New(color.FgYellow).SprintFunc()
		fmt.Printf("%s Creating branch '%s'...\n", yellow("→"), name)

		if err := brancher.CreateBranch(name); err != nil {
			return err
		}
	}

	if brancher.CurrentBranch() == name {
		fmt.Printf("Already on branch '%s'\n", name)
		return nil
	}

	yellow := color.New(color.FgYellow).SprintFunc()
	currentBranch := brancher.CurrentBranch()
	if currentBranch != "" {
		fmt.Printf("%s Saving branch '%s'...\n", yellow("→"), currentBranch)
	}
	fmt.Printf("%s Switching to branch '%s'...\n", yellow("→"), name)

	if err := brancher.Checkout(name); err != nil {
		return err
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("%s Switched to branch '%s'\n", green("✓"), name)

	showStaleWarning(brancher)

	return nil
}
