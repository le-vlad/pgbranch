package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/le-vlad/pgbranch/internal/schema"
)

func CopySchema(ctx context.Context, sourceConn, targetConn *pgx.Conn, tables []string, sourceDB string) error {
	extracted, err := schema.ExtractFromConnection(ctx, sourceConn, sourceDB)
	if err != nil {
		return fmt.Errorf("failed to extract schema from source: %w", err)
	}

	requested := buildTableSet(tables)

	if err := createSequences(ctx, sourceConn, targetConn, extracted, requested); err != nil {
		return fmt.Errorf("failed to create sequences: %w", err)
	}

	cs := schema.NewChangeSet()

	usedEnums := findUsedEnums(extracted, requested)
	for _, enum := range extracted.SortedEnums() {
		if usedEnums[enum.FullName()] {
			cs.Add(&schema.CreateEnumChange{Enum: enum})
		}
	}

	var fkConstraints []schema.Change

	for _, table := range extracted.SortedTables() {
		if !isTableRequested(table, requested) {
			continue
		}

		tableForCreate := cloneTableWithoutNonPKConstraints(table)
		cs.Add(&schema.CreateTableChange{Table: tableForCreate})

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

		for _, con := range table.SortedConstraints() {
			if con.Type == schema.ConstraintNotNull ||
				con.Type == schema.ConstraintTrigger ||
				con.Type == schema.ConstraintUnknown {
				continue
			}
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

	for _, fn := range extracted.SortedFunctions() {
		cs.Add(&schema.CreateFunctionChange{Function: fn})
	}

	if cs.IsEmpty() {
		return fmt.Errorf("no schema objects found for the requested tables")
	}

	ordered := schema.OrderChanges(cs)
	applier := schema.NewApplier(targetConn)

	result, err := applier.Apply(ctx, ordered)
	if err != nil {
		return fmt.Errorf("failed to apply schema (applied %d, failed at: %s): %w",
			len(result.Applied), failedDescription(result), err)
	}

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

	if err := setSequenceOwnership(ctx, sourceConn, targetConn, extracted, requested); err != nil {
		return fmt.Errorf("failed to set sequence ownership: %w", err)
	}

	return nil
}

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
		return createSequencesFallback(ctx, sourceConn, targetConn, s, requested)
	}
	defer rows.Close()

	rows.Close()
	return createSequencesFallback(ctx, sourceConn, targetConn, s, requested)
}

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

func cloneTableWithoutNonPKConstraints(table *schema.Table) *schema.Table {
	clone := schema.NewTable(table.Name, table.Schema)
	clone.Columns = table.Columns
	clone.Indexes = table.Indexes

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
