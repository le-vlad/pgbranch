package grace

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/le-vlad/pgbranch/internal/schema"
)

// CopySchema extracts DDL from the source database and applies it to the target.
// It reuses the existing internal/schema package for extraction, changeset building,
// and ordered application.
func CopySchema(ctx context.Context, sourceConn, targetConn *pgx.Conn, tables []string, sourceDB string) error {
	extracted, err := schema.ExtractFromConnection(ctx, sourceConn, sourceDB)
	if err != nil {
		return fmt.Errorf("failed to extract schema from source: %w", err)
	}

	// Build a set of requested tables for filtering.
	requested := buildTableSet(tables)

	// Step 1: Create sequences first (needed for SERIAL/BIGSERIAL columns).
	if err := createSequences(ctx, sourceConn, targetConn, extracted, requested); err != nil {
		return fmt.Errorf("failed to create sequences: %w", err)
	}

	cs := schema.NewChangeSet()

	// Add enums that are used by the requested tables.
	usedEnums := findUsedEnums(extracted, requested)
	for _, enum := range extracted.SortedEnums() {
		if usedEnums[enum.FullName()] {
			cs.Add(&schema.CreateEnumChange{Enum: enum})
		}
	}

	// Collect FK constraints to add after all PKs/tables are created.
	var fkConstraints []schema.Change

	// Add tables with their columns.
	// We strip all non-PK constraints from the table clone because generateCreateTable
	// includes them inline, which can fail if referenced tables don't exist yet.
	for _, table := range extracted.SortedTables() {
		if !isTableRequested(table, requested) {
			continue
		}

		tableForCreate := cloneTableWithoutNonPKConstraints(table)
		cs.Add(&schema.CreateTableChange{Table: tableForCreate})

		// Add indexes that are NOT backing a constraint (PK, UNIQUE).
		// Constraint-backing indexes are created automatically by AddConstraintChange.
		constraintNames := make(map[string]bool)
		for _, con := range table.Constraints {
			constraintNames[con.Name] = true
		}
		for _, idx := range table.SortedIndexes() {
			if idx.IsPrimary || constraintNames[idx.Name] {
				continue
			}
			cs.Add(&schema.CreateIndexChange{Index: idx})
		}

		// Add PK and UNIQUE/CHECK constraints immediately.
		// Defer FK constraints to a later pass (after all tables + PKs exist).
		for _, con := range table.SortedConstraints() {
			if con.Type == schema.ConstraintForeignKey {
				if con.RefTable != "" && !isRefTableRequested(con.RefTable, table.Schema, requested) {
					continue
				}
				fkConstraints = append(fkConstraints, &schema.AddConstraintChange{
					TableName:  table.FullName(),
					Constraint: con,
				})
				continue
			}
			cs.Add(&schema.AddConstraintChange{
				TableName:  table.FullName(),
				Constraint: con,
			})
		}
	}

	// Add functions.
	for _, fn := range extracted.SortedFunctions() {
		cs.Add(&schema.CreateFunctionChange{Function: fn})
	}

	if cs.IsEmpty() {
		return fmt.Errorf("no schema objects found for the requested tables")
	}

	// Apply main schema (tables, PKs, indexes, enums, functions) first.
	ordered := schema.OrderChanges(cs)
	applier := schema.NewApplier(targetConn)

	result, err := applier.Apply(ctx, ordered)
	if err != nil {
		return fmt.Errorf("failed to apply schema (applied %d, failed at: %s): %w",
			len(result.Applied), failedDescription(result), err)
	}

	// Apply FK constraints in a second pass (all referenced tables + PKs now exist).
	if len(fkConstraints) > 0 {
		fkSet := schema.NewChangeSet()
		for _, fk := range fkConstraints {
			fkSet.Add(fk)
		}
		fkResult, err := applier.Apply(ctx, fkSet)
		if err != nil {
			return fmt.Errorf("failed to apply FK constraints (applied %d, failed at: %s): %w",
				len(fkResult.Applied), failedDescription(fkResult), err)
		}
	}

	// Step 3: Set sequence ownership to link sequences to their columns.
	if err := setSequenceOwnership(ctx, sourceConn, targetConn, extracted, requested); err != nil {
		return fmt.Errorf("failed to set sequence ownership: %w", err)
	}

	return nil
}

// createSequences queries sequences from the source that are used by the requested tables
// and creates them on the target before tables are created.
func createSequences(ctx context.Context, sourceConn, targetConn *pgx.Conn, s *schema.Schema, requested map[string]bool) error {
	rows, err := sourceConn.Query(ctx, `
		SELECT
			n.nspname AS schema_name,
			c.relname AS sequence_name,
			pg_get_serial_sequence_definition(n.nspname, c.relname) AS definition
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'S'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY n.nspname, c.relname
	`)
	if err != nil {
		// pg_get_serial_sequence_definition may not exist — fall back to simpler approach.
		return createSequencesFallback(ctx, sourceConn, targetConn, s, requested)
	}
	defer rows.Close()

	// If the query worked but returned nothing useful, just use fallback.
	rows.Close()
	return createSequencesFallback(ctx, sourceConn, targetConn, s, requested)
}

// createSequencesFallback creates sequences by querying pg_class and pg_sequences.
func createSequencesFallback(ctx context.Context, sourceConn, targetConn *pgx.Conn, s *schema.Schema, requested map[string]bool) error {
	rows, err := sourceConn.Query(ctx, `
		SELECT
			schemaname,
			sequencename,
			data_type,
			start_value,
			min_value,
			max_value,
			increment_by,
			cycle
		FROM pg_sequences
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY schemaname, sequencename
	`)
	if err != nil {
		return fmt.Errorf("failed to query sequences: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			schemaName, seqName, dataType string
			startVal, minVal, maxVal, inc int64
			cycle                         bool
		)
		if err := rows.Scan(&schemaName, &seqName, &dataType, &startVal, &minVal, &maxVal, &inc, &cycle); err != nil {
			return fmt.Errorf("failed to scan sequence: %w", err)
		}

		// Check if this sequence is used by any of the requested tables.
		// Sequences for SERIAL columns follow the pattern: tablename_colname_seq
		used := false
		for _, table := range s.Tables {
			if !isTableRequested(table, requested) {
				continue
			}
			for _, col := range table.Columns {
				if col.DefaultValue != nil && strings.Contains(*col.DefaultValue, seqName) {
					used = true
					break
				}
			}
			if used {
				break
			}
		}

		if !used {
			continue
		}

		qualifiedName := pgx.Identifier{schemaName, seqName}.Sanitize()
		cycleStr := "NO CYCLE"
		if cycle {
			cycleStr = "CYCLE"
		}

		createSQL := fmt.Sprintf(
			"CREATE SEQUENCE IF NOT EXISTS %s AS %s INCREMENT BY %d MINVALUE %d MAXVALUE %d START WITH %d %s",
			qualifiedName, dataType, inc, minVal, maxVal, startVal, cycleStr,
		)

		if _, err := targetConn.Exec(ctx, createSQL); err != nil {
			return fmt.Errorf("failed to create sequence %s: %w", qualifiedName, err)
		}
	}

	return rows.Err()
}

// setSequenceOwnership sets OWNED BY on sequences to link them to their table columns.
func setSequenceOwnership(ctx context.Context, sourceConn, targetConn *pgx.Conn, s *schema.Schema, requested map[string]bool) error {
	rows, err := sourceConn.Query(ctx, `
		SELECT
			seq_ns.nspname AS seq_schema,
			seq_class.relname AS seq_name,
			tab_ns.nspname AS tab_schema,
			tab_class.relname AS tab_name,
			a.attname AS col_name
		FROM pg_depend d
		JOIN pg_class seq_class ON seq_class.oid = d.objid AND seq_class.relkind = 'S'
		JOIN pg_namespace seq_ns ON seq_ns.oid = seq_class.relnamespace
		JOIN pg_class tab_class ON tab_class.oid = d.refobjid AND tab_class.relkind = 'r'
		JOIN pg_namespace tab_ns ON tab_ns.oid = tab_class.relnamespace
		JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
		WHERE d.deptype = 'a'
		  AND seq_ns.nspname NOT IN ('pg_catalog', 'information_schema')
	`)
	if err != nil {
		return nil // not critical
	}
	defer rows.Close()

	for rows.Next() {
		var seqSchema, seqName, tabSchema, tabName, colName string
		if err := rows.Scan(&seqSchema, &seqName, &tabSchema, &tabName, &colName); err != nil {
			continue
		}

		tableKey := tabSchema + "." + tabName
		if !requested[tableKey] {
			continue
		}

		sql := fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s.%s",
			pgx.Identifier{seqSchema, seqName}.Sanitize(),
			pgx.Identifier{tabSchema, tabName}.Sanitize(),
			pgx.Identifier{colName}.Sanitize(),
		)

		if _, err := targetConn.Exec(ctx, sql); err != nil {
			// Non-fatal — ownership is nice to have.
			continue
		}
	}

	return nil
}

// buildTableSet creates a lookup set from the table list.
// Handles both "schema.table" and bare "table" (defaults to public).
func buildTableSet(tables []string) map[string]bool {
	set := make(map[string]bool, len(tables))
	for _, t := range tables {
		s, tbl := parseTableName(t)
		set[s+"."+tbl] = true
	}
	return set
}

func isTableRequested(table *schema.Table, requested map[string]bool) bool {
	key := table.Schema + "." + table.Name
	return requested[key]
}

func isRefTableRequested(refTable, currentSchema string, requested map[string]bool) bool {
	if strings.Contains(refTable, ".") {
		return requested[refTable]
	}
	return requested[currentSchema+"."+refTable]
}

// findUsedEnums scans columns of requested tables for enum types.
func findUsedEnums(s *schema.Schema, requested map[string]bool) map[string]bool {
	enumNames := make(map[string]bool)
	for _, enum := range s.Enums {
		enumNames[enum.Name] = true
	}

	used := make(map[string]bool)
	for _, table := range s.Tables {
		if !isTableRequested(table, requested) {
			continue
		}
		for _, col := range table.Columns {
			dt := strings.ToLower(col.DataType)
			if col.IsArray {
				dt = strings.ToLower(col.ElementType)
			}
			if enumNames[dt] {
				// Find the enum's full name.
				for _, enum := range s.Enums {
					if enum.Name == dt {
						used[enum.FullName()] = true
					}
				}
			}
		}
	}

	return used
}

// cloneTableWithoutNonPKConstraints creates a shallow copy of the table with only PK constraints.
// This ensures generateCreateTable doesn't emit inline ALTER TABLE for FKs/UNIQUE/CHECK.
func cloneTableWithoutNonPKConstraints(table *schema.Table) *schema.Table {
	clone := schema.NewTable(table.Name, table.Schema)
	clone.Columns = table.Columns
	clone.Indexes = table.Indexes

	// Only keep PRIMARY KEY constraints.
	for name, con := range table.Constraints {
		if con.Type == schema.ConstraintPrimaryKey {
			clone.Constraints[name] = con
		}
	}
	return clone
}

func failedDescription(result *schema.ApplyResult) string {
	if len(result.Failed) == 0 {
		return "unknown"
	}
	f := result.Failed[0]
	return fmt.Sprintf("%s: %s", f.Change.Description(), f.Error)
}
