package grace

import (
	"context"
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/jackc/pgx/v5"
	"golang.org/x/term"
	"os"
)

// RunMode controls how far the migration proceeds.
type RunMode int

const (
	RunFull         RunMode = iota // schema + snapshot + streaming
	RunSchemaOnly                  // schema copy only
	RunSnapshotOnly                // schema + snapshot, no streaming
)

// Migrator orchestrates the full grace migration pipeline.
type Migrator struct {
	config     *Config
	tables     []string
	checkpoint *Checkpoint
	replicator *Replicator
	keepSlot   bool
	mode       RunMode
	sendCh     chan any
}

// NewMigrator creates a new migration orchestrator.
func NewMigrator(cfg *Config, keepSlot bool, mode RunMode) *Migrator {
	return &Migrator{
		config:   cfg,
		keepSlot: keepSlot,
		mode:     mode,
		sendCh:   make(chan any, 100),
	}
}

// Run executes the migration. It blocks until complete or context is cancelled.
func (m *Migrator) Run(ctx context.Context) error {
	isTTY := term.IsTerminal(int(os.Stdout.Fd()))

	if isTTY {
		return m.runWithTUI(ctx)
	}
	return m.runWithPlainLog(ctx)
}

func (m *Migrator) runWithTUI(ctx context.Context) error {
	model := NewModel()
	program := tea.NewProgram(model, tea.WithAltScreen())

	// Run migration in background, send messages to TUI.
	go func() {
		err := m.executeMigration(ctx)
		program.Send(MigrationDoneMsg{Err: err})
	}()

	// Forward messages from migration goroutine to TUI.
	go func() {
		for msg := range m.sendCh {
			program.Send(msg)
		}
	}()

	finalModel, err := program.Run()
	if err != nil {
		return fmt.Errorf("TUI error: %w", err)
	}

	if fm, ok := finalModel.(Model); ok && fm.err != nil {
		return fm.err
	}

	return nil
}

func (m *Migrator) runWithPlainLog(ctx context.Context) error {
	logger := NewPlainLogger()

	// Forward messages from migration goroutine to plain logger.
	go func() {
		for msg := range m.sendCh {
			switch msg := msg.(type) {
			case PhaseMsg:
				logger.SetPhase(msg.Phase)
			case TableInitMsg:
				logger.TableInit(msg.Table, msg.TotalRows)
			case TableProgressMsg:
				// Batch progress — only log periodically.
			case TableDoneMsg:
				logger.TableDone(msg.Table)
			case StreamingUpdateMsg:
				logger.StreamingUpdate(msg.LSN, msg.Inserts, msg.Updates, msg.Deletes)
			}
		}
	}()

	return m.executeMigration(ctx)
}

func (m *Migrator) executeMigration(ctx context.Context) error {
	defer close(m.sendCh)

	// Load or create checkpoint.
	cp, err := LoadCheckpoint(m.config.CheckpointPath())
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}
	m.checkpoint = cp
	cp.SlotName = m.config.SlotName
	cp.PublicationName = m.config.PublicationName

	// Phase 0: Validate.
	m.send(PhaseMsg{Phase: "validate"})

	sourceConn, err := pgx.Connect(ctx, m.config.Source.ConnectionURL())
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceConn.Close(ctx)

	if err := ValidateSource(ctx, sourceConn); err != nil {
		return fmt.Errorf("source validation failed: %w", err)
	}

	targetConn, err := pgx.Connect(ctx, m.config.Target.ConnectionURL())
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetConn.Close(ctx)

	if err := ValidateTarget(ctx, targetConn); err != nil {
		return fmt.Errorf("target validation failed: %w", err)
	}

	// Resolve tables.
	tables, err := ResolveTables(ctx, sourceConn, m.config.Tables)
	if err != nil {
		return err
	}
	m.tables = tables
	cp.InitTables(tables)

	// Close validation connections — replicator creates its own.
	sourceConn.Close(ctx)
	targetConn.Close(ctx)

	// Phase 1: Schema copy (if not already done).
	if !cp.SchemaApplied {
		m.send(PhaseMsg{Phase: "schema"})

		srcConn, err := pgx.Connect(ctx, m.config.Source.ConnectionURL())
		if err != nil {
			return fmt.Errorf("failed to connect to source for schema: %w", err)
		}

		tgtConn, err := pgx.Connect(ctx, m.config.Target.ConnectionURL())
		if err != nil {
			srcConn.Close(ctx)
			return fmt.Errorf("failed to connect to target for schema: %w", err)
		}

		err = CopySchema(ctx, srcConn, tgtConn, tables, m.config.Source.Database)
		srcConn.Close(ctx)
		tgtConn.Close(ctx)

		if err != nil {
			return fmt.Errorf("schema copy failed: %w", err)
		}

		cp.SchemaApplied = true
		_ = cp.Save()
	}

	if m.mode == RunSchemaOnly {
		return nil
	}

	// Create replicator and connect.
	m.replicator = NewReplicator(m.config, tables, cp, m.sendCh)
	if err := m.replicator.Connect(ctx); err != nil {
		return err
	}
	defer m.cleanup(ctx)

	// Phase 2: Setup replication.
	m.send(PhaseMsg{Phase: "setup"})

	snapshotName, consistentLSN, err := m.replicator.Setup(ctx)
	if err != nil {
		return fmt.Errorf("replication setup failed: %w", err)
	}

	cp.SnapshotName = snapshotName
	cp.ConsistentLSN = consistentLSN.String()
	_ = cp.Save()

	// Phase 3: Initial snapshot (if not complete).
	if !cp.IsSnapshotComplete() {
		m.send(PhaseMsg{Phase: "snapshot"})
		cp.Phase = "snapshot"
		_ = cp.Save()

		if err := m.replicator.RunSnapshot(ctx, snapshotName); err != nil {
			return fmt.Errorf("snapshot failed: %w", err)
		}
	}

	if m.mode == RunSnapshotOnly {
		return nil
	}

	// Phase 4: WAL streaming.
	m.send(PhaseMsg{Phase: "streaming"})
	cp.Phase = "streaming"
	_ = cp.Save()

	if err := m.replicator.StartStreaming(ctx); err != nil {
		if ctx.Err() != nil {
			// Graceful shutdown — context was cancelled.
			return nil
		}
		return fmt.Errorf("streaming failed: %w", err)
	}

	return nil
}

func (m *Migrator) cleanup(ctx context.Context) {
	if m.replicator == nil {
		return
	}

	if !m.keepSlot {
		_ = m.replicator.Cleanup(ctx)
		_ = m.checkpoint.Delete()
	} else {
		_ = m.checkpoint.Save()
	}

	_ = m.replicator.Close()
}

func (m *Migrator) send(msg any) {
	select {
	case m.sendCh <- msg:
	default:
	}
}
