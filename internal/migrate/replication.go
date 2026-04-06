package migrate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type RelationInfo struct {
	SchemaName string
	TableName  string
	Columns    []ColumnInfo
}

func (r *RelationInfo) FullName() string {
	return r.SchemaName + "." + r.TableName
}

type ColumnInfo struct {
	Name    string
	IsKey   bool
	TypeOID uint32
}

type Replicator struct {
	replConn   *pgconn.PgConn
	sourceConn *pgx.Conn
	targetConn *pgx.Conn
	config     *Config
	tables     []string
	relations  map[uint32]*RelationInfo
	checkpoint *Checkpoint
	sendCh     chan any

	inserts int64
	updates int64
	deletes int64
	inTx    bool // whether the target connection is inside a transaction
}

func NewReplicator(cfg *Config, tables []string, checkpoint *Checkpoint, sendCh chan any) *Replicator {
	return &Replicator{
		config:     cfg,
		tables:     tables,
		checkpoint: checkpoint,
		relations:  make(map[uint32]*RelationInfo),
		sendCh:     sendCh,
	}
}

func (r *Replicator) Connect(ctx context.Context) error {
	sourceConn, err := pgx.Connect(ctx, r.config.Source.ConnectionURL())
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	r.sourceConn = sourceConn

	targetConn, err := pgx.Connect(ctx, r.config.Target.ConnectionURL())
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	r.targetConn = targetConn

	replConn, err := pgconn.Connect(ctx, r.config.Source.ReplicationURL())
	if err != nil {
		return fmt.Errorf("failed to create replication connection: %w", err)
	}
	r.replConn = replConn

	return nil
}

// connectFreshReplConn opens a new replication connection, closing the old one.
// This is needed because after CreateReplicationSlot the connection protocol
// state can be dirty on some providers.
func (r *Replicator) connectFreshReplConn(ctx context.Context) error {
	if r.replConn != nil {
		_ = r.replConn.Close(ctx)
		r.replConn = nil
	}

	conn, err := pgconn.Connect(ctx, r.config.Source.ReplicationURL())
	if err != nil {
		return fmt.Errorf("failed to create replication connection: %w", err)
	}
	r.replConn = conn
	return nil
}

func (r *Replicator) Setup(ctx context.Context) (snapshotName string, consistentLSN pglogrepl.LSN, err error) {
	var pubExists bool
	err = r.sourceConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
		r.config.PublicationName).Scan(&pubExists)
	if err != nil {
		return "", 0, fmt.Errorf("failed to check publication: %w", err)
	}

	if !pubExists {
		tableList := strings.Join(r.tables, ", ")
		pubSQL := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
			pgx.Identifier{r.config.PublicationName}.Sanitize(),
			tableList,
		)
		if _, err := r.sourceConn.Exec(ctx, pubSQL); err != nil {
			return "", 0, fmt.Errorf("failed to create publication: %w", err)
		}
	}

	var slotExists bool
	err = r.sourceConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		r.config.SlotName).Scan(&slotExists)
	if err != nil {
		return "", 0, fmt.Errorf("failed to check replication slot: %w", err)
	}

	if slotExists {
		var lsnStr *string
		err = r.sourceConn.QueryRow(ctx,
			"SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
			r.config.SlotName).Scan(&lsnStr)
		if err != nil {
			return "", 0, fmt.Errorf("failed to get slot LSN: %w", err)
		}
		if lsnStr != nil {
			consistentLSN, err = pglogrepl.ParseLSN(*lsnStr)
			if err != nil {
				return "", 0, fmt.Errorf("failed to parse LSN: %w", err)
			}
		}
		return r.checkpoint.SnapshotName, consistentLSN, nil
	}

	result, err := pglogrepl.CreateReplicationSlot(ctx, r.replConn,
		r.config.SlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
		},
	)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create replication slot: %w", err)
	}

	consistentLSN, err = pglogrepl.ParseLSN(result.ConsistentPoint)
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse consistent LSN: %w", err)
	}

	return result.SnapshotName, consistentLSN, nil
}

// StartStreaming begins the WAL streaming loop with automatic reconnection.
// It blocks until context is cancelled.
func (r *Replicator) StartStreaming(ctx context.Context) error {
	const maxRetries = 10
	retries := 0

	for {
		err := r.streamOnce(ctx)
		if err == nil || ctx.Err() != nil {
			return ctx.Err()
		}

		retries++
		if retries > maxRetries {
			return fmt.Errorf("streaming failed after %d retries: %w", maxRetries, err)
		}

		// Classify the error — only retry on connection/transient errors.
		if !isTransientError(err) {
			return err
		}

		r.trySend(PhaseMsg{Phase: fmt.Sprintf("reconnecting (attempt %d/%d)", retries, maxRetries)})

		// Rollback any in-flight target transaction.
		r.rollbackTarget(ctx)

		// Wait before reconnecting with exponential backoff (1s, 2s, 4s, ... capped at 30s).
		delay := time.Duration(1<<uint(retries-1)) * time.Second
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		// Reconnect replication connection.
		if err := r.connectFreshReplConn(ctx); err != nil {
			continue // will retry
		}

		r.trySend(PhaseMsg{Phase: "streaming"})
	}
}

// streamOnce runs a single streaming session. Returns nil only on graceful shutdown.
func (r *Replicator) streamOnce(ctx context.Context) error {
	// Always open a fresh replication connection for streaming.
	if err := r.connectFreshReplConn(ctx); err != nil {
		return err
	}

	var startLSN pglogrepl.LSN
	if r.checkpoint.ConfirmedLSN != "" {
		var err error
		startLSN, err = pglogrepl.ParseLSN(r.checkpoint.ConfirmedLSN)
		if err != nil {
			return fmt.Errorf("failed to parse confirmed LSN: %w", err)
		}
	} else if r.checkpoint.ConsistentLSN != "" {
		var err error
		startLSN, err = pglogrepl.ParseLSN(r.checkpoint.ConsistentLSN)
		if err != nil {
			return fmt.Errorf("failed to parse consistent LSN: %w", err)
		}
	}

	err := pglogrepl.StartReplication(ctx, r.replConn, r.config.SlotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", r.config.PublicationName),
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	standbyDeadline := time.Now().Add(10 * time.Second)
	checkpointDeadline := time.Now().Add(30 * time.Second)
	var lastReceivedLSN pglogrepl.LSN

	for {
		if ctx.Err() != nil {
			if lastReceivedLSN > 0 {
				_ = r.acknowledge(ctx, lastReceivedLSN)
			}
			return nil // graceful shutdown
		}

		if time.Now().After(standbyDeadline) {
			if lastReceivedLSN > 0 {
				if err := r.acknowledge(ctx, lastReceivedLSN); err != nil {
					return fmt.Errorf("failed to send standby status: %w", err)
				}
			}
			standbyDeadline = time.Now().Add(10 * time.Second)
		}

		if time.Now().After(checkpointDeadline) {
			if lastReceivedLSN > 0 {
				r.checkpoint.ConfirmedLSN = lastReceivedLSN.String()
				_ = r.checkpoint.Save()
			}
			checkpointDeadline = time.Now().Add(30 * time.Second)
		}

		recvCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		rawMsg, err := r.replConn.ReceiveMessage(recvCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			return fmt.Errorf("receive message failed: %w", err)
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse keepalive: %w", err)
				}
				if pkm.ReplyRequested {
					standbyDeadline = time.Time{} // force immediate reply
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse XLogData: %w", err)
				}

				if err := r.handleWALData(ctx, xld); err != nil {
					// If it's a target-side error, rollback and log but don't kill streaming.
					r.rollbackTarget(ctx)
					return fmt.Errorf("failed to handle WAL data: %w", err)
				}

				if xld.WALStart > lastReceivedLSN {
					lastReceivedLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				}
			}

		case *pgproto3.ErrorResponse:
			return fmt.Errorf("server error: %s (SQLSTATE %s)", msg.Message, msg.Code)

		default:
			// Ignore unknown message types.
		}
	}
}

func (r *Replicator) handleWALData(ctx context.Context, xld pglogrepl.XLogData) error {
	msg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return fmt.Errorf("failed to parse pgoutput message: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		r.relations[m.RelationID] = &RelationInfo{
			SchemaName: m.Namespace,
			TableName:  m.RelationName,
			Columns:    makeColumns(m),
		}

	case *pglogrepl.BeginMessage:
		_, err := r.targetConn.Exec(ctx, "BEGIN")
		if err != nil {
			return fmt.Errorf("failed to begin target tx: %w", err)
		}
		r.inTx = true

	case *pglogrepl.InsertMessage:
		rel, ok := r.relations[m.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation ID: %d", m.RelationID)
		}
		if err := r.applyInsert(ctx, rel, m); err != nil {
			return err
		}
		r.inserts++

	case *pglogrepl.UpdateMessage:
		rel, ok := r.relations[m.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation ID: %d", m.RelationID)
		}
		if err := r.applyUpdate(ctx, rel, m); err != nil {
			return err
		}
		r.updates++

	case *pglogrepl.DeleteMessage:
		rel, ok := r.relations[m.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation ID: %d", m.RelationID)
		}
		if err := r.applyDelete(ctx, rel, m); err != nil {
			return err
		}
		r.deletes++

	case *pglogrepl.CommitMessage:
		_, err := r.targetConn.Exec(ctx, "COMMIT")
		if err != nil {
			return fmt.Errorf("failed to commit target tx: %w", err)
		}
		r.inTx = false

		r.trySend(StreamingUpdateMsg{
			LSN:     pglogrepl.LSN(m.CommitLSN).String(),
			Inserts: r.inserts,
			Updates: r.updates,
			Deletes: r.deletes,
		})

	case *pglogrepl.TruncateMessage:
		for _, relID := range m.RelationIDs {
			rel, ok := r.relations[relID]
			if !ok {
				continue
			}
			_, err := r.targetConn.Exec(ctx,
				fmt.Sprintf("TRUNCATE TABLE %s", pgx.Identifier{rel.SchemaName, rel.TableName}.Sanitize()))
			if err != nil {
				return fmt.Errorf("failed to truncate %s: %w", rel.FullName(), err)
			}
		}

	// Silently ignore message types we don't act on (TypeMessage, OriginMessage, etc.)
	default:
	}

	return nil
}

// rollbackTarget issues a ROLLBACK on the target if we're mid-transaction.
func (r *Replicator) rollbackTarget(ctx context.Context) {
	if r.inTx {
		_, _ = r.targetConn.Exec(ctx, "ROLLBACK")
		r.inTx = false
	}
}

func (r *Replicator) applyInsert(ctx context.Context, rel *RelationInfo, msg *pglogrepl.InsertMessage) error {
	colNames, values := decodeTupleData(rel, msg.Tuple)
	if len(colNames) == 0 {
		return nil
	}

	placeholders := make([]string, len(colNames))
	quotedCols := make([]string, len(colNames))
	for i, name := range colNames {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		quotedCols[i] = pgx.Identifier{name}.Sanitize()
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		pgx.Identifier{rel.SchemaName, rel.TableName}.Sanitize(),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := r.targetConn.Exec(ctx, sql, values...)
	if err != nil {
		return fmt.Errorf("failed to insert into %s: %w", rel.FullName(), err)
	}
	return nil
}

func (r *Replicator) applyUpdate(ctx context.Context, rel *RelationInfo, msg *pglogrepl.UpdateMessage) error {
	colNames, values := decodeTupleData(rel, msg.NewTuple)
	if len(colNames) == 0 {
		return nil
	}

	keyCols, keyVals := findKeyValues(rel, msg)

	setClauses := make([]string, len(colNames))
	allValues := make([]any, 0, len(colNames)+len(keyCols))
	for i, name := range colNames {
		setClauses[i] = fmt.Sprintf("%s = $%d", pgx.Identifier{name}.Sanitize(), i+1)
		allValues = append(allValues, values[i])
	}

	whereClauses := make([]string, len(keyCols))
	for i, name := range keyCols {
		idx := len(colNames) + i + 1
		whereClauses[i] = fmt.Sprintf("%s = $%d", pgx.Identifier{name}.Sanitize(), idx)
		allValues = append(allValues, keyVals[i])
	}

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		pgx.Identifier{rel.SchemaName, rel.TableName}.Sanitize(),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	_, err := r.targetConn.Exec(ctx, sql, allValues...)
	if err != nil {
		return fmt.Errorf("failed to update %s: %w", rel.FullName(), err)
	}
	return nil
}

func (r *Replicator) applyDelete(ctx context.Context, rel *RelationInfo, msg *pglogrepl.DeleteMessage) error {
	var tuple *pglogrepl.TupleData
	if msg.OldTuple != nil {
		tuple = msg.OldTuple
	} else {
		return fmt.Errorf("delete on %s without old tuple data (check REPLICA IDENTITY)", rel.FullName())
	}

	keyCols, keyVals := decodeTupleDataForKeys(rel, tuple)
	if len(keyCols) == 0 {
		return fmt.Errorf("no key columns for delete on %s", rel.FullName())
	}

	whereClauses := make([]string, len(keyCols))
	for i, name := range keyCols {
		whereClauses[i] = fmt.Sprintf("%s = $%d", pgx.Identifier{name}.Sanitize(), i+1)
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s",
		pgx.Identifier{rel.SchemaName, rel.TableName}.Sanitize(),
		strings.Join(whereClauses, " AND "),
	)

	_, err := r.targetConn.Exec(ctx, sql, keyVals...)
	if err != nil {
		return fmt.Errorf("failed to delete from %s: %w", rel.FullName(), err)
	}
	return nil
}

func (r *Replicator) acknowledge(ctx context.Context, lsn pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, r.replConn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lsn,
			WALFlushPosition: lsn,
			WALApplyPosition: lsn,
		},
	)
}

func (r *Replicator) Cleanup(ctx context.Context) error {
	var errs []string

	if r.replConn != nil {
		if err := pglogrepl.DropReplicationSlot(ctx, r.replConn,
			r.config.SlotName,
			pglogrepl.DropReplicationSlotOptions{Wait: true},
		); err != nil {
			errs = append(errs, fmt.Sprintf("drop slot: %v", err))
		}
	}

	if r.sourceConn != nil {
		if _, err := r.sourceConn.Exec(ctx,
			fmt.Sprintf("DROP PUBLICATION IF EXISTS %s",
				pgx.Identifier{r.config.PublicationName}.Sanitize())); err != nil {
			errs = append(errs, fmt.Sprintf("drop publication: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (r *Replicator) Close() error {
	ctx := context.Background()
	var errs []string

	if r.replConn != nil {
		if err := r.replConn.Close(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("replication conn: %v", err))
		}
	}
	if r.sourceConn != nil {
		if err := r.sourceConn.Close(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("source conn: %v", err))
		}
	}
	if r.targetConn != nil {
		if err := r.targetConn.Close(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("target conn: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (r *Replicator) trySend(msg any) {
	select {
	case r.sendCh <- msg:
	default:
	}
}

// isTransientError returns true for connection/network errors that are worth retrying.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	s := err.Error()

	// pgconn connection-level errors.
	if strings.Contains(s, "server conn crashed") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "connection refused") ||
		strings.Contains(s, "EOF") ||
		strings.Contains(s, "receive message failed") {
		return true
	}

	// SQLSTATE-based classification.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code[:2] {
		case "08": // connection_exception family
			return true
		case "57": // operator_intervention (restart, crash recovery)
			return true
		}
	}

	return false
}

func makeColumns(msg *pglogrepl.RelationMessage) []ColumnInfo {
	cols := make([]ColumnInfo, len(msg.Columns))
	for i, c := range msg.Columns {
		cols[i] = ColumnInfo{
			Name:    c.Name,
			IsKey:   c.Flags == 1,
			TypeOID: c.DataType,
		}
	}
	return cols
}

func decodeTupleData(rel *RelationInfo, tuple *pglogrepl.TupleData) ([]string, []any) {
	if tuple == nil {
		return nil, nil
	}

	var colNames []string
	var values []any

	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}

		switch col.DataType {
		case 'n':
			colNames = append(colNames, rel.Columns[i].Name)
			values = append(values, nil)
		case 't':
			colNames = append(colNames, rel.Columns[i].Name)
			values = append(values, string(col.Data))
		case 'u':
			continue
		}
	}

	return colNames, values
}

func decodeTupleDataForKeys(rel *RelationInfo, tuple *pglogrepl.TupleData) ([]string, []any) {
	if tuple == nil {
		return nil, nil
	}

	var keyCols []string
	var keyVals []any

	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		if !rel.Columns[i].IsKey {
			continue
		}
		switch col.DataType {
		case 't':
			keyCols = append(keyCols, rel.Columns[i].Name)
			keyVals = append(keyVals, string(col.Data))
		case 'n':
			keyCols = append(keyCols, rel.Columns[i].Name)
			keyVals = append(keyVals, nil)
		}
	}

	return keyCols, keyVals
}

func findKeyValues(rel *RelationInfo, msg *pglogrepl.UpdateMessage) ([]string, []any) {
	if msg.OldTuple != nil {
		return decodeTupleDataForKeys(rel, msg.OldTuple)
	}

	var keyCols []string
	var keyVals []any
	if msg.NewTuple != nil {
		for i, col := range msg.NewTuple.Columns {
			if i >= len(rel.Columns) {
				break
			}
			if rel.Columns[i].IsKey {
				switch col.DataType {
				case 't':
					keyCols = append(keyCols, rel.Columns[i].Name)
					keyVals = append(keyVals, string(col.Data))
				}
			}
		}
	}

	return keyCols, keyVals
}
