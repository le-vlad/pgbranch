package grace

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ValidateSource checks that the source PostgreSQL instance is configured for logical replication.
func ValidateSource(ctx context.Context, conn *pgx.Conn) error {
	var walLevel string
	if err := conn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return fmt.Errorf("failed to check wal_level: %w", err)
	}
	if walLevel != "logical" {
		return fmt.Errorf("source requires wal_level=logical (current: %s). "+
			"Set it with: ALTER SYSTEM SET wal_level = 'logical'; then restart PostgreSQL", walLevel)
	}

	var maxSlotsStr string
	if err := conn.QueryRow(ctx, "SHOW max_replication_slots").Scan(&maxSlotsStr); err != nil {
		return fmt.Errorf("failed to check max_replication_slots: %w", err)
	}

	var maxSlots int
	if _, err := fmt.Sscanf(maxSlotsStr, "%d", &maxSlots); err != nil {
		return fmt.Errorf("failed to parse max_replication_slots '%s': %w", maxSlotsStr, err)
	}

	var usedSlots int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots").Scan(&usedSlots); err != nil {
		return fmt.Errorf("failed to count replication slots: %w", err)
	}

	if usedSlots >= maxSlots {
		return fmt.Errorf("no available replication slots (used %d of %d). "+
			"Increase max_replication_slots or drop unused slots", usedSlots, maxSlots)
	}

	return nil
}

// ValidateTarget checks that the target PostgreSQL instance is accessible.
func ValidateTarget(ctx context.Context, conn *pgx.Conn) error {
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	return nil
}

// ResolveTables resolves the table list. If tables contains "*", it queries all user tables.
// Otherwise it validates that each specified table exists on the source.
func ResolveTables(ctx context.Context, conn *pgx.Conn, tables []string) ([]string, error) {
	if len(tables) == 1 && tables[0] == "*" {
		return queryAllTables(ctx, conn)
	}

	for _, t := range tables {
		schemaName, tableName := parseTableName(t)
		var exists bool
		err := conn.QueryRow(ctx,
			`SELECT EXISTS(
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE'
			)`, schemaName, tableName).Scan(&exists)
		if err != nil {
			return nil, fmt.Errorf("failed to check table %s: %w", t, err)
		}
		if !exists {
			return nil, fmt.Errorf("table %s does not exist on source", t)
		}
	}

	return tables, nil
}

func queryAllTables(ctx context.Context, conn *pgx.Conn) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT table_schema || '.' || table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		  AND table_type = 'BASE TABLE'
		ORDER BY table_schema, table_name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}

	if len(tables) == 0 {
		return nil, fmt.Errorf("no user tables found on source")
	}

	return tables, rows.Err()
}

// parseTableName splits "schema.table" into schema and table. Defaults to "public" if no schema.
func parseTableName(fullName string) (schema, table string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
}
