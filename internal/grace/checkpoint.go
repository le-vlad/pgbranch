package grace

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// TableStatus represents the replication status of a single table.
type TableStatus string

const (
	TablePending    TableStatus = "pending"
	TableInProgress TableStatus = "in_progress"
	TableComplete   TableStatus = "complete"
)

// Checkpoint tracks migration progress for resume support.
type Checkpoint struct {
	SlotName        string                    `json:"slot_name"`
	PublicationName string                    `json:"publication_name"`
	SnapshotName    string                    `json:"snapshot_name,omitempty"`
	ConsistentLSN   string                    `json:"consistent_lsn"`
	ConfirmedLSN    string                    `json:"confirmed_lsn"`
	Tables          map[string]*TableProgress `json:"tables"`
	Phase           string                    `json:"phase"`
	SchemaApplied   bool                      `json:"schema_applied"`
	UpdatedAt       time.Time                 `json:"updated_at"`

	path string
}

// TableProgress tracks replication progress for a single table.
type TableProgress struct {
	Status     TableStatus `json:"status"`
	RowsCopied int64       `json:"rows_copied"`
	TotalRows  int64       `json:"total_rows,omitempty"`
}

// LoadCheckpoint loads checkpoint from disk, or returns a new empty one if it doesn't exist.
func LoadCheckpoint(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Checkpoint{
				Tables: make(map[string]*TableProgress),
				Phase:  "init",
				path:   path,
			}, nil
		}
		return nil, fmt.Errorf("failed to read checkpoint: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("failed to parse checkpoint: %w", err)
	}
	cp.path = path

	if cp.Tables == nil {
		cp.Tables = make(map[string]*TableProgress)
	}

	return &cp, nil
}

// Save persists the checkpoint to disk.
func (c *Checkpoint) Save() error {
	c.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	if err := os.WriteFile(c.path, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}

	return nil
}

// Delete removes the checkpoint file from disk.
func (c *Checkpoint) Delete() error {
	if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete checkpoint: %w", err)
	}
	return nil
}

// IsSnapshotComplete returns true if all tables have completed their initial snapshot.
func (c *Checkpoint) IsSnapshotComplete() bool {
	if len(c.Tables) == 0 {
		return false
	}
	for _, tp := range c.Tables {
		if tp.Status != TableComplete {
			return false
		}
	}
	return true
}

// InitTables sets up table progress tracking for the given tables.
func (c *Checkpoint) InitTables(tables []string) {
	for _, t := range tables {
		if _, exists := c.Tables[t]; !exists {
			c.Tables[t] = &TableProgress{Status: TablePending}
		}
	}
}
