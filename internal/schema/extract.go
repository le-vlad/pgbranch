package schema

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Extractor extracts schema information from a PostgreSQL database.
type Extractor struct {
	conn *pgx.Conn
}

func NewExtractor(conn *pgx.Conn) *Extractor {
	return &Extractor{conn: conn}
}

func (e *Extractor) Extract(ctx context.Context, dbName string) (*Schema, error) {
	schema := NewSchema(dbName)

	enums, err := e.extractEnums(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract enums: %w", err)
	}
	for _, enum := range enums {
		schema.Enums[enum.Name] = enum
	}

	tables, err := e.extractTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract tables: %w", err)
	}
	for _, table := range tables {
		schema.Tables[table.Name] = table
	}

	for _, table := range schema.Tables {
		columns, err := e.extractColumns(ctx, table.Schema, table.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to extract columns for %s: %w", table.Name, err)
		}
		for _, col := range columns {
			table.Columns[col.Name] = col
		}
	}

	for _, table := range schema.Tables {
		indexes, err := e.extractIndexes(ctx, table.Schema, table.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to extract indexes for %s: %w", table.Name, err)
		}
		for _, idx := range indexes {
			table.Indexes[idx.Name] = idx
		}
	}

	for _, table := range schema.Tables {
		constraints, err := e.extractConstraints(ctx, table.Schema, table.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to extract constraints for %s: %w", table.Name, err)
		}
		for _, con := range constraints {
			table.Constraints[con.Name] = con
		}
	}

	functions, err := e.extractFunctions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract functions: %w", err)
	}
	for _, fn := range functions {
		schema.Functions[fn.Signature()] = fn
	}

	return schema, nil
}

func (e *Extractor) extractTables(ctx context.Context) ([]*Table, error) {
	query := `
		SELECT
			table_name,
			table_schema
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		  AND table_type = 'BASE TABLE'
		ORDER BY table_schema, table_name
	`

	rows, err := e.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []*Table
	for rows.Next() {
		var name, schema string
		if err := rows.Scan(&name, &schema); err != nil {
			return nil, err
		}
		tables = append(tables, NewTable(name, schema))
	}

	return tables, rows.Err()
}

func (e *Extractor) extractColumns(ctx context.Context, schemaName, tableName string) ([]*Column, error) {
	query := `
		SELECT
			column_name,
			data_type,
			is_nullable,
			column_default,
			ordinal_position,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			udt_name
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := e.conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*Column
	for rows.Next() {
		var (
			name, dataType, isNullable string
			defaultValue               *string
			position                   int
			charMaxLen                 *int
			numPrecision               *int
			numScale                   *int
			udtName                    string
		)

		if err := rows.Scan(
			&name, &dataType, &isNullable, &defaultValue,
			&position, &charMaxLen, &numPrecision, &numScale, &udtName,
		); err != nil {
			return nil, err
		}

		col := &Column{
			Name:             name,
			DataType:         dataType,
			IsNullable:       isNullable == "YES",
			DefaultValue:     defaultValue,
			Position:         position,
			CharMaxLength:    charMaxLen,
			NumericPrecision: numPrecision,
			NumericScale:     numScale,
		}

		if dataType == "ARRAY" {
			col.IsArray = true
			col.ElementType = strings.TrimPrefix(udtName, "_")
			col.DataType = col.ElementType
		}

		columns = append(columns, col)
	}

	return columns, rows.Err()
}

func (e *Extractor) extractIndexes(ctx context.Context, schemaName, tableName string) ([]*Index, error) {
	query := `
		SELECT
			i.relname AS index_name,
			am.amname AS index_type,
			ix.indisunique AS is_unique,
			ix.indisprimary AS is_primary,
			pg_get_indexdef(ix.indexrelid) AS definition,
			ARRAY(
				SELECT a.attname
				FROM unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
				JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
				ORDER BY k.ord
			) AS columns
		FROM pg_index ix
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_am am ON am.oid = i.relam
		WHERE n.nspname = $1 AND t.relname = $2
		ORDER BY i.relname
	`

	rows, err := e.conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []*Index
	for rows.Next() {
		var (
			name, indexType, definition string
			isUnique, isPrimary         bool
			columns                     []string
		)

		if err := rows.Scan(&name, &indexType, &isUnique, &isPrimary, &definition, &columns); err != nil {
			return nil, err
		}

		indexes = append(indexes, &Index{
			Name:       name,
			TableName:  tableName,
			Type:       indexType,
			IsUnique:   isUnique,
			IsPrimary:  isPrimary,
			Definition: definition,
			Columns:    columns,
		})
	}

	return indexes, rows.Err()
}

func (e *Extractor) extractConstraints(ctx context.Context, schemaName, tableName string) ([]*Constraint, error) {
	query := `
		SELECT
			con.conname AS constraint_name,
			CASE con.contype
				WHEN 'p' THEN 'PRIMARY KEY'
				WHEN 'f' THEN 'FOREIGN KEY'
				WHEN 'u' THEN 'UNIQUE'
				WHEN 'c' THEN 'CHECK'
				WHEN 'x' THEN 'EXCLUDE'
			END AS constraint_type,
			pg_get_constraintdef(con.oid) AS definition,
			ARRAY(
				SELECT a.attname
				FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
				JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
				ORDER BY k.ord
			) AS columns,
			frel.relname AS ref_table,
			ARRAY(
				SELECT a.attname
				FROM unnest(con.confkey) WITH ORDINALITY AS k(attnum, ord)
				JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum
				ORDER BY k.ord
			) AS ref_columns,
			CASE con.confdeltype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
			END AS on_delete,
			CASE con.confupdtype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
			END AS on_update
		FROM pg_constraint con
		JOIN pg_class t ON t.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		LEFT JOIN pg_class frel ON frel.oid = con.confrelid
		WHERE n.nspname = $1 AND t.relname = $2
		ORDER BY con.conname
	`

	rows, err := e.conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var constraints []*Constraint
	for rows.Next() {
		var (
			name, conType, definition string
			columns                   []string
			refTable                  *string
			refColumns                []string
			onDelete, onUpdate        *string
		)

		if err := rows.Scan(
			&name, &conType, &definition, &columns,
			&refTable, &refColumns, &onDelete, &onUpdate,
		); err != nil {
			return nil, err
		}

		con := &Constraint{
			Name:       name,
			Type:       ConstraintType(conType),
			TableName:  tableName,
			Definition: definition,
			Columns:    columns,
		}

		if refTable != nil {
			con.RefTable = *refTable
			con.RefColumns = refColumns
		}
		if onDelete != nil {
			con.OnDelete = *onDelete
		}
		if onUpdate != nil {
			con.OnUpdate = *onUpdate
		}

		constraints = append(constraints, con)
	}

	return constraints, rows.Err()
}

func (e *Extractor) extractEnums(ctx context.Context) ([]*Enum, error) {
	query := `
		SELECT
			t.typname AS enum_name,
			n.nspname AS enum_schema,
			ARRAY(
				SELECT e.enumlabel
				FROM pg_enum e
				WHERE e.enumtypid = t.oid
				ORDER BY e.enumsortorder
			) AS enum_values
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE t.typtype = 'e'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY n.nspname, t.typname
	`

	rows, err := e.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var enums []*Enum
	for rows.Next() {
		var name, schema string
		var values []string

		if err := rows.Scan(&name, &schema, &values); err != nil {
			return nil, err
		}

		enums = append(enums, &Enum{
			Name:   name,
			Schema: schema,
			Values: values,
		})
	}

	return enums, rows.Err()
}

func (e *Extractor) extractFunctions(ctx context.Context) ([]*Function, error) {
	query := `
		SELECT
			p.proname AS function_name,
			n.nspname AS function_schema,
			pg_get_function_arguments(p.oid) AS arguments,
			pg_get_function_result(p.oid) AS return_type,
			l.lanname AS language,
			pg_get_functiondef(p.oid) AS definition
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		JOIN pg_language l ON l.oid = p.prolang
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
		  AND p.prokind IN ('f', 'p')  -- functions and procedures
		ORDER BY n.nspname, p.proname
	`

	rows, err := e.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var functions []*Function
	for rows.Next() {
		var name, schema, args, returnType, language, definition string

		if err := rows.Scan(&name, &schema, &args, &returnType, &language, &definition); err != nil {
			return nil, err
		}

		hash := sha256.Sum256([]byte(definition))

		functions = append(functions, &Function{
			Name:       name,
			Schema:     schema,
			Arguments:  args,
			ReturnType: returnType,
			Language:   language,
			Definition: definition,
			BodyHash:   hex.EncodeToString(hash[:8]),
		})
	}

	return functions, rows.Err()
}

func ExtractFromConnection(ctx context.Context, conn *pgx.Conn, dbName string) (*Schema, error) {
	extractor := NewExtractor(conn)
	return extractor.Extract(ctx, dbName)
}

func ExtractFromURL(ctx context.Context, connURL string, dbName string) (*Schema, error) {
	conn, err := pgx.Connect(ctx, connURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)

	return ExtractFromConnection(ctx, conn, dbName)
}
