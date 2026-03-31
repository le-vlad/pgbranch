package cli

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/le-vlad/pgbranch/internal/grace"
	"github.com/spf13/cobra"
)

func newGraceCmd() *cobra.Command {
	var (
		configPath   string
		keepSlot     bool
		schemaOnly   bool
		snapshotOnly bool
	)

	cmd := &cobra.Command{
		Use:   "grace",
		Short: "Graceful PG-to-PG database migration via logical replication",
		Long: `Migrate a PostgreSQL database to another PostgreSQL instance using logical replication.

Grace reads a YAML configuration file describing source and target databases,
then performs: schema copy → initial data snapshot → live WAL streaming.

The migration shows table-by-table progress and supports resume on interruption.

Example:
  pgbranch grace -c migration.yaml
  pgbranch grace -c migration.yaml --schema-only
  pgbranch grace -c migration.yaml --snapshot-only
  pgbranch grace -c migration.yaml --keep

Requirements:
  - Source PostgreSQL must have wal_level=logical
  - Source user must have REPLICATION privilege
  - Tables must have a PRIMARY KEY (for UPDATE/DELETE replication)`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if configPath == "" {
				return fmt.Errorf("--config flag is required")
			}

			cfg, err := grace.LoadConfig(configPath)
			if err != nil {
				return err
			}

			mode := grace.RunFull
			if schemaOnly {
				mode = grace.RunSchemaOnly
			} else if snapshotOnly {
				mode = grace.RunSnapshotOnly
			}

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			migrator := grace.NewMigrator(cfg, keepSlot, mode)
			return migrator.Run(ctx)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to YAML configuration file (required)")
	cmd.Flags().BoolVar(&keepSlot, "keep", false, "Keep replication slot and publication on exit (for resume)")
	cmd.Flags().BoolVar(&schemaOnly, "schema-only", false, "Copy schema only, then exit")
	cmd.Flags().BoolVar(&snapshotOnly, "snapshot-only", false, "Copy schema and initial data, then exit (no streaming)")
	_ = cmd.MarkFlagRequired("config")

	return cmd
}
