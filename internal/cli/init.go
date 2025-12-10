package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/le-vlad/pgbranch/internal/core"
	"github.com/le-vlad/pgbranch/internal/credentials"
	"github.com/le-vlad/pgbranch/pkg/config"
)

var (
	initDatabase string
	initHost     string
	initPort     int
	initUser     string
	initPassword string
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize pgbranch for a database",
	Long: `Initialize pgbranch in the current directory.

This creates a .pgbranch directory to store configuration,
metadata, and database snapshots.

Example:
  pgbranch init -d myapp_dev
  pgbranch init -d myapp_dev -h localhost -p 5432 -U postgres`,
	RunE: runInit,
}

func init() {
	initCmd.Flags().StringVarP(&initDatabase, "database", "d", "", "Database name (required)")
	initCmd.Flags().StringVarP(&initHost, "host", "H", "localhost", "PostgreSQL host")
	initCmd.Flags().IntVarP(&initPort, "port", "p", 5432, "PostgreSQL port")
	initCmd.Flags().StringVarP(&initUser, "user", "U", "postgres", "PostgreSQL user")
	initCmd.Flags().StringVarP(&initPassword, "password", "W", "", "PostgreSQL password")
	initCmd.MarkFlagRequired("database")
}

func runInit(cmd *cobra.Command, args []string) error {
	if config.IsInitialized() {
		return fmt.Errorf("pgbranch already initialized in this directory")
	}

	if err := core.Initialize(initDatabase, initHost, initPort, initUser, initPassword); err != nil {
		return err
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("%s Initialized pgbranch for database '%s'\n", green("✓"), initDatabase)

	if !credentials.KeyExists() {
		keyPath, _ := credentials.GetKeyPath()
		_, _, err := credentials.EnsureKey()
		if err != nil {
			fmt.Printf("\nWarning: failed to generate encryption key: %v\n", err)
		} else {
			fmt.Printf("%s Generated encryption key at %s\n", green("✓"), keyPath)
		}
	}

	fmt.Println("\nNext steps:")
	fmt.Println("  pgbranch branch main    # Create your first branch")

	return nil
}
