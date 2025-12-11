package schema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Applier executes schema changes against a database.
type Applier struct {
	conn      *pgx.Conn
	generator *SQLGenerator
}

func NewApplier(conn *pgx.Conn) *Applier {
	gen := NewSQLGenerator()
	gen.IncludeComments = false
	return &Applier{
		conn:      conn,
		generator: gen,
	}
}

// ApplyResult contains the results of applying a ChangeSet.
type ApplyResult struct {
	Applied []Change
	Failed  []ChangeError
}

type ChangeError struct {
	Change Change
	SQL    string
	Error  error
}

func (r *ApplyResult) Success() bool {
	return len(r.Failed) == 0
}

// Apply executes all changes in the ChangeSet.
// Changes are applied in a transaction that is rolled back if any change fails.
func (a *Applier) Apply(ctx context.Context, cs *ChangeSet) (*ApplyResult, error) {
	result := &ApplyResult{
		Applied: make([]Change, 0, len(cs.Changes)),
		Failed:  make([]ChangeError, 0),
	}

	if cs.IsEmpty() {
		return result, nil
	}

	tx, err := a.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, change := range cs.Changes {
		sql := a.generator.GenerateChange(change)
		if sql == "" {
			continue
		}

		_, err := tx.Exec(ctx, sql)
		if err != nil {
			result.Failed = append(result.Failed, ChangeError{
				Change: change,
				SQL:    sql,
				Error:  err,
			})
			return result, fmt.Errorf("failed to apply change: %w", err)
		}

		result.Applied = append(result.Applied, change)
	}

	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return result, nil
}

// ApplyWithContinue applies changes but continues on errors.
// Each change is applied in its own transaction.
func (a *Applier) ApplyWithContinue(ctx context.Context, cs *ChangeSet) *ApplyResult {
	result := &ApplyResult{
		Applied: make([]Change, 0, len(cs.Changes)),
		Failed:  make([]ChangeError, 0),
	}

	for _, change := range cs.Changes {
		sql := a.generator.GenerateChange(change)
		if sql == "" {
			continue
		}

		_, err := a.conn.Exec(ctx, sql)
		if err != nil {
			result.Failed = append(result.Failed, ChangeError{
				Change: change,
				SQL:    sql,
				Error:  err,
			})
		} else {
			result.Applied = append(result.Applied, change)
		}
	}

	return result
}

// DryRun validates that all changes can be generated as SQL without executing them.
func (a *Applier) DryRun(cs *ChangeSet) ([]string, error) {
	statements := make([]string, 0, len(cs.Changes))

	for _, change := range cs.Changes {
		sql := a.generator.GenerateChange(change)
		if sql == "" {
			return nil, fmt.Errorf("cannot generate SQL for change: %s", change.Description())
		}
		statements = append(statements, sql)
	}

	return statements, nil
}

// OrderChanges reorders changes for safe application.
// This ensures dependencies are respected (e.g., create enums before tables that use them).
func OrderChanges(cs *ChangeSet) *ChangeSet {
	ordered := NewChangeSet()

	// Order of operations:
	// 1. Create enums (tables may depend on them)
	// 2. Add enum values
	// 3. Create tables
	// 4. Add columns
	// 5. Create indexes
	// 6. Add constraints
	// 7. Create/replace functions
	// 8. Drop constraints (before dropping columns)
	// 9. Drop indexes
	// 10. Alter columns
	// 11. Drop columns
	// 12. Drop tables
	// 13. Drop enums
	// 14. Drop functions

	order := []ChangeType{
		ChangeCreateEnum,
		ChangeAddEnumValue,
		ChangeCreateTable,
		ChangeAddColumn,
		ChangeCreateIndex,
		ChangeAddConstraint,
		ChangeCreateFunction,
		ChangeReplaceFunction,
		ChangeDropConstraint,
		ChangeDropIndex,
		ChangeAlterColumn,
		ChangeDropColumn,
		ChangeDropTable,
		ChangeDropEnum,
		ChangeDropFunction,
	}

	for _, ct := range order {
		for _, c := range cs.ByType(ct) {
			ordered.Add(c)
		}
	}

	return ordered
}

// ValidateChanges checks if changes can be safely applied.
// Returns warnings and errors.
func ValidateChanges(cs *ChangeSet) (warnings []string, errors []string) {
	for _, c := range cs.Changes {
		switch change := c.(type) {
		case *AlterColumnChange:
			if change.Alteration.TypeChanged {
				oldType := change.Alteration.OldType
				newType := change.Alteration.NewType

				if isNumericType(oldType) && isStringType(newType) {
					warnings = append(warnings,
						fmt.Sprintf("Changing %s from %s to %s may lose precision",
							change.ObjectName(), oldType, newType))
				}
				if isStringType(oldType) && isNumericType(newType) {
					errors = append(errors,
						fmt.Sprintf("Changing %s from %s to %s may fail if data cannot be converted",
							change.ObjectName(), oldType, newType))
				}
			}

			if change.Alteration.NullableChanged && !change.Alteration.NewNullable {
				warnings = append(warnings,
					fmt.Sprintf("Setting %s to NOT NULL may fail if column contains NULL values",
						change.ObjectName()))
			}

		case *DropColumnChange:
			warnings = append(warnings,
				fmt.Sprintf("Dropping column %s will permanently delete all data in that column",
					change.ObjectName()))

		case *DropTableChange:
			warnings = append(warnings,
				fmt.Sprintf("Dropping table %s will permanently delete all data in that table",
					change.ObjectName()))
		}
	}

	return warnings, errors
}

func isNumericType(t string) bool {
	numericTypes := []string{"integer", "int", "bigint", "smallint", "decimal", "numeric", "real", "double"}
	for _, nt := range numericTypes {
		if t == nt || fmt.Sprintf("%s", t) == nt {
			return true
		}
	}
	return false
}

func isStringType(t string) bool {
	return t == "text" || t == "varchar" || t == "character varying" || t == "char" || t == "character"
}
