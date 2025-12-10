package cli

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pgbranch",
	Short: "Git-style branching for PostgreSQL databases",
	Long: `pgbranch - A CLI tool for managing PostgreSQL database branches.

Create, switch, and manage database snapshots just like git branches.
Perfect for local development when you need to work with different
database states.

Example workflow:
  pgbranch init -d myapp_dev
  pgbranch branch main
  pgbranch branch feature-x
  pgbranch checkout main
  pgbranch delete feature-x

Share snapshots with your team:
  pgbranch remote add origin /shared/snapshots
  pgbranch push main
  pgbranch pull main`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(branchCmd)
	rootCmd.AddCommand(checkoutCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(logCmd)
	rootCmd.AddCommand(hookCmd)
	rootCmd.AddCommand(pruneCmd)

	rootCmd.AddCommand(newRemoteCmd())
	rootCmd.AddCommand(newPushCmd())
	rootCmd.AddCommand(newPullCmd())
	rootCmd.AddCommand(newKeysCmd())
}
