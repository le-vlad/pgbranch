package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/jackc/pgx/v5"
	"github.com/le-vlad/pgbranch/internal/core"
	"github.com/le-vlad/pgbranch/internal/schema"
	"github.com/spf13/cobra"
)

func newMergeCmd() *cobra.Command {
	var (
		dryRun        bool
		migrationFile bool
		migrationDir  string
		force         bool
	)

	cmd := &cobra.Command{
		Use:   "merge <source> <target>",
		Short: "Merge schema changes from source branch into target branch",
		Long: `Merge schema changes from one branch into another.

This command computes the schema diff between source and target branches,
shows the changes, and applies them to the target branch snapshot.

The merge will:
1. Show all schema changes that will be applied
2. Warn about destructive changes (DROP TABLE, DROP COLUMN, etc.)
3. Require confirmation for destructive changes
4. Apply the changes to the target branch snapshot

Examples:
  # Merge feature branch into main
  pgbranch merge feature-auth main

  # Preview changes without applying (dry run)
  pgbranch merge feature-auth main --dry-run

  # Generate a migration file instead of applying
  pgbranch merge feature-auth main --migration-file

  # Force merge without confirmation prompts
  pgbranch merge feature-auth main --force`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sourceBranch := args[0]
			targetBranch := args[1]

			brancher, err := core.NewBrancher()
			if err != nil {
				return err
			}

			source, ok := brancher.Metadata.GetBranch(sourceBranch)
			if !ok {
				return fmt.Errorf("source branch '%s' does not exist", sourceBranch)
			}

			target, ok := brancher.Metadata.GetBranch(targetBranch)
			if !ok {
				return fmt.Errorf("target branch '%s' does not exist", targetBranch)
			}

			ctx := context.Background()

			fmt.Printf("Extracting schema from '%s'...\n", sourceBranch)
			sourceSchema, err := extractSchemaFromDB(ctx, brancher, source.Snapshot)
			if err != nil {
				return fmt.Errorf("failed to extract source schema: %w", err)
			}

			fmt.Printf("Extracting schema from '%s'...\n", targetBranch)
			targetSchema, err := extractSchemaFromDB(ctx, brancher, target.Snapshot)
			if err != nil {
				return fmt.Errorf("failed to extract target schema: %w", err)
			}

			changeSet := schema.Diff(targetSchema, sourceSchema)

			if changeSet.IsEmpty() {
				fmt.Printf("\nNo schema differences between '%s' and '%s'\n", sourceBranch, targetBranch)
				return nil
			}

			changeSet = schema.OrderChanges(changeSet)

			fmt.Printf("\nChanges to merge from '%s' → '%s':\n\n", sourceBranch, targetBranch)
			printDiffFull(changeSet)

			warnings, errs := schema.ValidateChanges(changeSet)

			if len(warnings) > 0 {
				yellow := color.New(color.FgYellow).SprintFunc()
				fmt.Printf("\n%s Warnings:\n", yellow("⚠"))
				for _, w := range warnings {
					fmt.Printf("  • %s\n", w)
				}
			}

			if len(errs) > 0 {
				red := color.New(color.FgRed).SprintFunc()
				fmt.Printf("\n%s Potential Issues:\n", red("✗"))
				for _, e := range errs {
					fmt.Printf("  • %s\n", e)
				}
			}

			if dryRun {
				fmt.Printf("\n--- Dry Run: SQL that would be executed ---\n\n")
				printDiffSQL(changeSet)
				return nil
			}

			if migrationFile {
				return writeMigrationFile(changeSet, sourceBranch, targetBranch, migrationDir)
			}

			if changeSet.HasDestructive() && !force {
				red := color.New(color.FgRed).SprintFunc()
				fmt.Printf("\n%s This merge contains %d destructive change(s) that may result in data loss.\n",
					red("⚠ WARNING:"), changeSet.DestructiveCount())

				if !confirmPrompt("Do you want to proceed?") {
					fmt.Println("Merge cancelled.")
					return nil
				}
			} else if !force {
				if !confirmPrompt(fmt.Sprintf("Apply %d change(s) to '%s'?", len(changeSet.Changes), targetBranch)) {
					fmt.Println("Merge cancelled.")
					return nil
				}
			}

			fmt.Printf("\nApplying changes to '%s'...\n", targetBranch)

			targetConnURL := brancher.Config.ConnectionURLForDB(target.Snapshot)
			conn, err := pgx.Connect(ctx, targetConnURL)
			if err != nil {
				return fmt.Errorf("failed to connect to target: %w", err)
			}
			defer conn.Close(ctx)

			applier := schema.NewApplier(conn)
			result, err := applier.Apply(ctx, changeSet)
			if err != nil {
				red := color.New(color.FgRed).SprintFunc()
				fmt.Printf("\n%s Merge failed: %v\n", red("✗"), err)
				if len(result.Failed) > 0 {
					fmt.Printf("\nFailed change:\n")
					for _, f := range result.Failed {
						fmt.Printf("  • %s\n", f.Change.Description())
						fmt.Printf("    SQL: %s\n", f.SQL)
						fmt.Printf("    Error: %v\n", f.Error)
					}
				}
				return err
			}

			green := color.New(color.FgGreen).SprintFunc()
			fmt.Printf("\n%s Successfully merged %d change(s) from '%s' into '%s'\n",
				green("✓"), len(result.Applied), sourceBranch, targetBranch)

			return nil
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show SQL without applying changes")
	cmd.Flags().BoolVar(&migrationFile, "migration-file", false, "Generate a migration file instead of applying")
	cmd.Flags().StringVar(&migrationDir, "migration-dir", "migrations", "Directory for migration files")
	cmd.Flags().BoolVarP(&force, "force", "f", false, "Skip confirmation prompts")

	return cmd
}

func confirmPrompt(message string) bool {
	reader := bufio.NewReader(os.Stdin)

	fmt.Printf("%s [y/N]: ", message)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}

	response = strings.ToLower(strings.TrimSpace(response))
	return response == "y" || response == "yes"
}

func writeMigrationFile(cs *schema.ChangeSet, source, target, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create migrations directory: %w", err)
	}

	timestamp := time.Now().Format("20060102150405")
	safeName := strings.ReplaceAll(source, "/", "_")
	safeName = strings.ReplaceAll(safeName, " ", "_")
	filename := fmt.Sprintf("%s_merge_%s.sql", timestamp, safeName)
	filepath := filepath.Join(dir, filename)

	gen := schema.NewSQLGenerator()
	description := fmt.Sprintf("Merge %s → %s", source, target)
	content := gen.GenerateMigrationFile(cs, description)

	if err := os.WriteFile(filepath, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write migration file: %w", err)
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("\n%s Migration file created: %s\n", green("✓"), filepath)

	return nil
}

func init() {
	rootCmd.AddCommand(newMergeCmd())
}
