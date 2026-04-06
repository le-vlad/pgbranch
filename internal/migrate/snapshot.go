package migrate

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (r *Replicator) RunSnapshot(ctx context.Context, snapshotName string) error {
	ordered, err := r.sortTablesByFK(ctx)
	if err != nil {
		return fmt.Errorf("failed to resolve table copy order: %w", err)
	}

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

	for _, table := range ordered {
		tp := r.checkpoint.Tables[table]

		if tp != nil && tp.Status == TableComplete {
			r.trySend(TableDoneMsg{Table: table})
			continue
		}

		schemaName, tableName := parseTableName(table)
		var totalRows int64
		err := tx.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s",
				pgx.Identifier{schemaName, tableName}.Sanitize()),
		).Scan(&totalRows)
		if err != nil {
			return fmt.Errorf("failed to count rows in %s: %w", table, err)
		}

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

func (r *Replicator) sortTablesByFK(ctx context.Context) ([]string, error) {
	tableSet := make(map[string]bool, len(r.tables))
	for _, t := range r.tables {
		tableSet[t] = true
	}

	rows, err := r.sourceConn.Query(ctx, `
		SELECT
			cn.nspname || '.' || cc.relname AS child_table,
			pn.nspname || '.' || pc.relname AS parent_table
		FROM pg_constraint con
		JOIN pg_class cc ON cc.oid = con.conrelid
		JOIN pg_namespace cn ON cn.oid = cc.relnamespace
		JOIN pg_class pc ON pc.oid = con.confrelid
		JOIN pg_namespace pn ON pn.oid = pc.relnamespace
		WHERE con.contype = 'f'
		  AND cn.nspname NOT IN ('pg_catalog', 'information_schema')
	`)
	if err != nil {
		return r.tables, nil
	}
	defer rows.Close()

	deps := make(map[string][]string)
	for rows.Next() {
		var child, parent string
		if err := rows.Scan(&child, &parent); err != nil {
			return r.tables, nil
		}
		if tableSet[child] && tableSet[parent] && child != parent {
			deps[child] = append(deps[child], parent)
		}
	}
	if err := rows.Err(); err != nil {
		return r.tables, nil
	}

	inDegree := make(map[string]int, len(r.tables))
	outEdges := make(map[string][]string)
	for _, t := range r.tables {
		inDegree[t] = 0
	}
	for child, parents := range deps {
		for _, parent := range parents {
			outEdges[parent] = append(outEdges[parent], child)
			inDegree[child]++
		}
	}

	var queue []string
	for _, t := range r.tables {
		if inDegree[t] == 0 {
			queue = append(queue, t)
		}
	}

	var ordered []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		ordered = append(ordered, node)

		for _, child := range outEdges[node] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	if len(ordered) < len(r.tables) {
		seen := make(map[string]bool, len(ordered))
		for _, t := range ordered {
			seen[t] = true
		}
		for _, t := range r.tables {
			if !seen[t] {
				ordered = append(ordered, t)
			}
		}
	}

	return ordered, nil
}

func (r *Replicator) copyTable(ctx context.Context, tx pgx.Tx, schemaName, tableName, fullTable string, tp *TableProgress) error {
	qualifiedName := pgx.Identifier{schemaName, tableName}.Sanitize()

	columns, err := getTableColumns(ctx, tx, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	colList := strings.Join(columns, ", ")

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

	data := buf.Bytes()
	rowCount := int64(0)
	for _, b := range data {
		if b == '\n' {
			rowCount++
		}
	}
	tp.RowsCopied = rowCount
	r.trySend(TableProgressMsg{Table: fullTable, RowsDelta: rowCount})

	copyInSQL := fmt.Sprintf("COPY %s (%s) FROM STDIN WITH (FORMAT text)", qualifiedName, colList)

	tgtPgConn := r.targetConn.PgConn()
	result, err := tgtPgConn.CopyFrom(ctx, &buf, copyInSQL)
	if err != nil {
		return fmt.Errorf("failed to COPY INTO target: %w", err)
	}

	tp.RowsCopied = result.RowsAffected()

	return nil
}

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
