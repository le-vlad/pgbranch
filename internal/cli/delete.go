package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/le-vlad/pgbranch/internal/core"
)

var deleteForce bool

var deleteCmd = &cobra.Command{
	Use:     "delete <branch>",
	Aliases: []string{"rm"},
	Short:   "Delete a branch",
	Long: `Delete a branch and its snapshot.

Cannot delete the current branch unless --force is used.

Example:
  pgbranch delete feature-x
  pgbranch delete main --force`,
	Args: cobra.ExactArgs(1),
	RunE: runDelete,
}

func init() {
	deleteCmd.Flags().BoolVarP(&deleteForce, "force", "f", false, "Force delete even if current branch")
}

func runDelete(cmd *cobra.Command, args []string) error {
	brancher, err := core.NewBrancher()
	if err != nil {
		return err
	}

	name := args[0]

	if err := brancher.DeleteBranch(name, deleteForce); err != nil {
		return err
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("%s Deleted branch '%s'\n", green("✓"), name)

	return nil
}
