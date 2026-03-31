package grace

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// RunSnapshot performs the initial consistent data copy using the exported snapshot.
// It copies each table using the COPY protocol for maximum throughput.
func (r *Replicator) RunSnapshot(ctx context.Context, snapshotName string) error {
	// Open a transaction on the source with the exported snapshot for consistency.
	tx, err := r.sourceConn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("failed to begin snapshot transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if snapshotName != "" {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)); err != nil {
			return fmt.Errorf("failed to set transaction snapshot: %w", err)
		}
	}

	for _, table := range r.tables {
		tp := r.checkpoint.Tables[table]

		// Skip already completed tables (resume support).
		if tp != nil && tp.Status == TableComplete {
			r.trySend(TableDoneMsg{Table: table})
			continue
		}

		// Count total rows for progress tracking.
		schemaName, tableName := parseTableName(table)
		var totalRows int64
		err := tx.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s",
				pgx.Identifier{schemaName, tableName}.Sanitize()),
		).Scan(&totalRows)
		if err != nil {
			return fmt.Errorf("failed to count rows in %s: %w", table, err)
		}

		// Update checkpoint.
		if tp == nil {
			tp = &TableProgress{}
			r.checkpoint.Tables[table] = tp
		}
		tp.Status = TableInProgress
		tp.TotalRows = totalRows

		r.trySend(TableInitMsg{Table: table, TotalRows: totalRows})

		if totalRows == 0 {
			tp.Status = TableComplete
			r.trySend(TableDoneMsg{Table: table})
			_ = r.checkpoint.Save()
			continue
		}

		if err := r.copyTable(ctx, tx, schemaName, tableName, table, tp); err != nil {
			return fmt.Errorf("failed to copy table %s: %w", table, err)
		}

		tp.Status = TableComplete
		tp.RowsCopied = totalRows
		r.trySend(TableDoneMsg{Table: table})
		_ = r.checkpoint.Save()
	}

	return nil
}

// copyTable copies a single table from source to target using the COPY protocol.
// pgconn.CopyTo(ctx, w io.Writer, sql string) writes COPY data to a writer.
// pgconn.CopyFrom(ctx, r io.Reader, sql string) reads COPY data from a reader.
func (r *Replicator) copyTable(ctx context.Context, tx pgx.Tx, schemaName, tableName, fullTable string, tp *TableProgress) error {
	qualifiedName := pgx.Identifier{schemaName, tableName}.Sanitize()

	// Get column list from source.
	columns, err := getTableColumns(ctx, tx, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	colList := strings.Join(columns, ", ")

	// Read from source using COPY TO STDOUT into a buffer.
	copyOutSQL := fmt.Sprintf("COPY %s (%s) TO STDOUT WITH (FORMAT text)", qualifiedName, colList)

	var buf bytes.Buffer
	srcPgConn := tx.Conn().PgConn()
	_, err = srcPgConn.CopyTo(ctx, &buf, copyOutSQL)
	if err != nil {
		return fmt.Errorf("failed to COPY TO from source: %w", err)
	}

	if buf.Len() == 0 {
		return nil
	}

	// Count rows in the buffer for progress.
	data := buf.Bytes()
	rowCount := int64(0)
	for _, b := range data {
		if b == '\n' {
			rowCount++
		}
	}
	tp.RowsCopied = rowCount
	r.trySend(TableProgressMsg{Table: fullTable, RowsDelta: rowCount})

	// Write to target using COPY FROM STDIN.
	copyInSQL := fmt.Sprintf("COPY %s (%s) FROM STDIN WITH (FORMAT text)", qualifiedName, colList)

	tgtPgConn := r.targetConn.PgConn()
	result, err := tgtPgConn.CopyFrom(ctx, &buf, copyInSQL)
	if err != nil {
		return fmt.Errorf("failed to COPY INTO target: %w", err)
	}

	tp.RowsCopied = result.RowsAffected()

	return nil
}

// getTableColumns returns the ordered list of column names for a table.
func getTableColumns(ctx context.Context, conn interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, schemaName, tableName string) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}

	return columns, rows.Err()
}
