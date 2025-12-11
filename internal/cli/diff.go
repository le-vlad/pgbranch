package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/jackc/pgx/v5"
	"github.com/le-vlad/pgbranch/internal/core"
	"github.com/le-vlad/pgbranch/internal/schema"
	"github.com/spf13/cobra"
)

func newDiffCmd() *cobra.Command {
	var (
		statOnly bool
		showSQL  bool
	)

	cmd := &cobra.Command{
		Use:   "diff <branch1> [branch2]",
		Short: "Show schema differences between branches",
		Long: `Compare the schema of two database branches and show the differences.

If only one branch is specified, it compares against the current working database.

Examples:
  # Compare two branches
  pgbranch diff main feature-auth

  # Compare a branch against current working database
  pgbranch diff main

  # Show summary only
  pgbranch diff main feature-auth --stat

  # Show SQL statements to migrate
  pgbranch diff main feature-auth --sql`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			brancher, err := core.NewBrancher()
			if err != nil {
				return err
			}

			ctx := context.Background()

			var fromDB, toDB string
			var fromName, toName string

			if len(args) == 1 {
				branchName := args[0]
				branch, ok := brancher.Metadata.GetBranch(branchName)
				if !ok {
					return fmt.Errorf("branch '%s' does not exist", branchName)
				}
				fromDB = branch.Snapshot
				fromName = branchName
				toDB = brancher.Config.Database
				toName = "(working)"
			} else {
				branch1Name := args[0]
				branch2Name := args[1]

				branch1, ok := brancher.Metadata.GetBranch(branch1Name)
				if !ok {
					return fmt.Errorf("branch '%s' does not exist", branch1Name)
				}
				branch2, ok := brancher.Metadata.GetBranch(branch2Name)
				if !ok {
					return fmt.Errorf("branch '%s' does not exist", branch2Name)
				}

				fromDB = branch1.Snapshot
				fromName = branch1Name
				toDB = branch2.Snapshot
				toName = branch2Name
			}

			fromSchema, err := extractSchemaFromDB(ctx, brancher, fromDB)
			if err != nil {
				return fmt.Errorf("failed to extract schema from '%s': %w", fromName, err)
			}

			toSchema, err := extractSchemaFromDB(ctx, brancher, toDB)
			if err != nil {
				return fmt.Errorf("failed to extract schema from '%s': %w", toName, err)
			}

			changeSet := schema.Diff(fromSchema, toSchema)

			if changeSet.IsEmpty() {
				fmt.Printf("No schema differences between '%s' and '%s'\n", fromName, toName)
				return nil
			}

			fmt.Printf("Comparing '%s' → '%s'\n\n", fromName, toName)

			if statOnly {
				printDiffStat(changeSet)
			} else if showSQL {
				printDiffSQL(changeSet)
			} else {
				printDiffFull(changeSet)
			}

			return nil
		},
	}

	cmd.Flags().BoolVar(&statOnly, "stat", false, "Show summary statistics only")
	cmd.Flags().BoolVar(&showSQL, "sql", false, "Show SQL statements to apply changes")

	return cmd
}

func extractSchemaFromDB(ctx context.Context, brancher *core.Brancher, dbName string) (*schema.Schema, error) {
	connURL := brancher.Config.ConnectionURLForDB(dbName)
	conn, err := pgx.Connect(ctx, connURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)

	return schema.ExtractFromConnection(ctx, conn, dbName)
}

func printDiffStat(cs *schema.ChangeSet) {
	summary := cs.Summary()

	green := color.New(color.FgGreen).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()

	var additions, deletions, modifications int

	for changeType, count := range summary {
		switch changeType {
		case schema.ChangeCreateTable, schema.ChangeAddColumn, schema.ChangeCreateIndex,
			schema.ChangeAddConstraint, schema.ChangeCreateEnum, schema.ChangeAddEnumValue,
			schema.ChangeCreateFunction:
			additions += count
		case schema.ChangeDropTable, schema.ChangeDropColumn, schema.ChangeDropIndex,
			schema.ChangeDropConstraint, schema.ChangeDropEnum, schema.ChangeDropFunction:
			deletions += count
		case schema.ChangeAlterColumn, schema.ChangeReplaceFunction:
			modifications += count
		}
	}

	fmt.Printf("Summary:\n")
	if additions > 0 {
		fmt.Printf("  %s %d addition(s)\n", green("+"), additions)
	}
	if deletions > 0 {
		fmt.Printf("  %s %d deletion(s)\n", red("-"), deletions)
	}
	if modifications > 0 {
		fmt.Printf("  %s %d modification(s)\n", yellow("~"), modifications)
	}

	if cs.HasDestructive() {
		fmt.Printf("\n  %s %d destructive change(s)\n",
			red("⚠"), cs.DestructiveCount())
	}
}

func printDiffFull(cs *schema.ChangeSet) {
	green := color.New(color.FgGreen).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()

	tableCreates := cs.ByType(schema.ChangeCreateTable)
	tableDrops := cs.ByType(schema.ChangeDropTable)

	for _, c := range tableCreates {
		change := c.(*schema.CreateTableChange)
		fmt.Printf("%s TABLE %s\n", green("+"), change.Table.FullName())
		for _, col := range change.Table.SortedColumns() {
			nullable := ""
			if !col.IsNullable {
				nullable = " NOT NULL"
			}
			fmt.Printf("    %s %s%s\n", col.Name, col.FullType(), nullable)
		}
		fmt.Println()
	}

	for _, c := range tableDrops {
		change := c.(*schema.DropTableChange)
		fmt.Printf("%s TABLE %s %s\n", red("-"), change.Table.FullName(), red("⚠ DESTRUCTIVE"))
		fmt.Println()
	}

	columnChanges := make(map[string][]schema.Change)
	for _, c := range cs.Changes {
		switch change := c.(type) {
		case *schema.AddColumnChange:
			columnChanges[change.TableName] = append(columnChanges[change.TableName], c)
		case *schema.DropColumnChange:
			columnChanges[change.TableName] = append(columnChanges[change.TableName], c)
		case *schema.AlterColumnChange:
			columnChanges[change.TableName] = append(columnChanges[change.TableName], c)
		}
	}

	for tableName, changes := range columnChanges {
		fmt.Printf("%s TABLE %s\n", yellow("~"), tableName)
		for _, c := range changes {
			switch change := c.(type) {
			case *schema.AddColumnChange:
				fmt.Printf("  %s COLUMN %s %s\n", green("+"), change.Column.Name, change.Column.FullType())
			case *schema.DropColumnChange:
				fmt.Printf("  %s COLUMN %s %s\n", red("-"), change.Column.Name, red("⚠ DESTRUCTIVE"))
			case *schema.AlterColumnChange:
				destructive := ""
				if change.IsDestructive() {
					destructive = " " + red("⚠ DESTRUCTIVE")
				}
				fmt.Printf("  %s COLUMN %s: %s%s\n", yellow("~"), change.ColumnName,
					formatAlteration(&change.Alteration), destructive)
			}
		}
		fmt.Println()
	}

	indexCreates := cs.ByType(schema.ChangeCreateIndex)
	indexDrops := cs.ByType(schema.ChangeDropIndex)

	if len(indexCreates) > 0 || len(indexDrops) > 0 {
		for _, c := range indexCreates {
			change := c.(*schema.CreateIndexChange)
			unique := ""
			if change.Index.IsUnique {
				unique = "UNIQUE "
			}
			fmt.Printf("%s %sINDEX %s on %s(%s)\n",
				green("+"), unique, change.Index.Name,
				change.Index.TableName, strings.Join(change.Index.Columns, ", "))
		}
		for _, c := range indexDrops {
			change := c.(*schema.DropIndexChange)
			fmt.Printf("%s INDEX %s\n", red("-"), change.Index.Name)
		}
		fmt.Println()
	}

	constraintCreates := cs.ByType(schema.ChangeAddConstraint)
	constraintDrops := cs.ByType(schema.ChangeDropConstraint)

	if len(constraintCreates) > 0 || len(constraintDrops) > 0 {
		for _, c := range constraintCreates {
			change := c.(*schema.AddConstraintChange)
			fmt.Printf("%s CONSTRAINT %s (%s) on %s\n",
				green("+"), change.Constraint.Name, change.Constraint.Type, change.TableName)
		}
		for _, c := range constraintDrops {
			change := c.(*schema.DropConstraintChange)
			destructive := ""
			if change.IsDestructive() {
				destructive = " " + red("⚠ DESTRUCTIVE")
			}
			fmt.Printf("%s CONSTRAINT %s (%s)%s\n",
				red("-"), change.Constraint.Name, change.Constraint.Type, destructive)
		}
		fmt.Println()
	}

	enumCreates := cs.ByType(schema.ChangeCreateEnum)
	enumDrops := cs.ByType(schema.ChangeDropEnum)
	enumValueAdds := cs.ByType(schema.ChangeAddEnumValue)

	if len(enumCreates) > 0 || len(enumDrops) > 0 || len(enumValueAdds) > 0 {
		for _, c := range enumCreates {
			change := c.(*schema.CreateEnumChange)
			fmt.Printf("%s ENUM %s (%s)\n",
				green("+"), change.Enum.FullName(), strings.Join(change.Enum.Values, ", "))
		}
		for _, c := range enumDrops {
			change := c.(*schema.DropEnumChange)
			fmt.Printf("%s ENUM %s %s\n", red("-"), change.Enum.FullName(), red("⚠ DESTRUCTIVE"))
		}
		for _, c := range enumValueAdds {
			change := c.(*schema.AddEnumValueChange)
			fmt.Printf("%s ENUM VALUE '%s' to %s\n", green("+"), change.Value, change.EnumName)
		}
		fmt.Println()
	}

	funcCreates := cs.ByType(schema.ChangeCreateFunction)
	funcDrops := cs.ByType(schema.ChangeDropFunction)
	funcReplaces := cs.ByType(schema.ChangeReplaceFunction)

	if len(funcCreates) > 0 || len(funcDrops) > 0 || len(funcReplaces) > 0 {
		for _, c := range funcCreates {
			change := c.(*schema.CreateFunctionChange)
			fmt.Printf("%s FUNCTION %s\n", green("+"), change.Function.Signature())
		}
		for _, c := range funcDrops {
			change := c.(*schema.DropFunctionChange)
			fmt.Printf("%s FUNCTION %s\n", red("-"), change.Function.Signature())
		}
		for _, c := range funcReplaces {
			change := c.(*schema.ReplaceFunctionChange)
			fmt.Printf("%s FUNCTION %s [body changed]\n", yellow("~"), change.NewFunction.Signature())
		}
		fmt.Println()
	}

	printDiffStat(cs)
}

func printDiffSQL(cs *schema.ChangeSet) {
	gen := schema.NewSQLGenerator()
	statements := gen.Generate(cs)

	for _, stmt := range statements {
		fmt.Println(stmt)
	}
}

func formatAlteration(alt *schema.ColumnAlteration) string {
	var parts []string

	if alt.TypeChanged {
		parts = append(parts, fmt.Sprintf("type %s → %s", alt.OldType, alt.NewType))
	}
	if alt.NullableChanged {
		if alt.NewNullable {
			parts = append(parts, "nullable")
		} else {
			parts = append(parts, "not null")
		}
	}
	if alt.DefaultChanged {
		if alt.NewDefault == nil {
			parts = append(parts, "drop default")
		} else {
			parts = append(parts, fmt.Sprintf("default %s", *alt.NewDefault))
		}
	}

	return strings.Join(parts, ", ")
}

func init() {
	rootCmd.AddCommand(newDiffCmd())
}
