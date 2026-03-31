package grace

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	yaml := `
source:
  host: src-host
  port: 5433
  database: myapp
  user: replicator
  password: secret

target:
  host: tgt-host
  port: 5434
  database: myapp_target
  user: admin
  password: tgt-secret

tables:
  - public.users
  - public.orders

slot_name: test_slot
publication_name: test_pub
batch_size: 5000
`
	path := writeTestFile(t, "config.yaml", yaml)

	cfg, err := LoadConfig(path)
	require.NoError(t, err)

	assert.Equal(t, "src-host", cfg.Source.Host)
	assert.Equal(t, 5433, cfg.Source.Port)
	assert.Equal(t, "myapp", cfg.Source.Database)
	assert.Equal(t, "replicator", cfg.Source.User)
	assert.Equal(t, "secret", cfg.Source.Password)

	assert.Equal(t, "tgt-host", cfg.Target.Host)
	assert.Equal(t, 5434, cfg.Target.Port)
	assert.Equal(t, "myapp_target", cfg.Target.Database)

	assert.Equal(t, []string{"public.users", "public.orders"}, cfg.Tables)
	assert.Equal(t, "test_slot", cfg.SlotName)
	assert.Equal(t, "test_pub", cfg.PublicationName)
	assert.Equal(t, 5000, cfg.BatchSize)
}

func TestLoadConfig_Defaults(t *testing.T) {
	yaml := `
source:
  host: localhost
  database: db
  user: usr

target:
  host: remote
  database: db
  user: usr

tables: ["*"]
`
	path := writeTestFile(t, "config.yaml", yaml)

	cfg, err := LoadConfig(path)
	require.NoError(t, err)

	assert.Equal(t, 5432, cfg.Source.Port)
	assert.Equal(t, 5432, cfg.Target.Port)
	assert.Equal(t, defaultSlotName, cfg.SlotName)
	assert.Equal(t, defaultPublicationName, cfg.PublicationName)
	assert.Equal(t, defaultBatchSize, cfg.BatchSize)
}

func TestLoadConfig_MissingRequired(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		errMsg  string
	}{
		{
			name: "missing source host",
			yaml: `
source:
  database: db
  user: usr
target:
  host: h
  database: db
  user: usr
tables: ["*"]
`,
			errMsg: "source.host is required",
		},
		{
			name: "missing target database",
			yaml: `
source:
  host: h
  database: db
  user: usr
target:
  host: h
  user: usr
tables: ["*"]
`,
			errMsg: "target.database is required",
		},
		{
			name: "no tables",
			yaml: `
source:
  host: h
  database: db
  user: usr
target:
  host: h
  database: db
  user: usr
`,
			errMsg: "at least one table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTestFile(t, "config.yaml", tt.yaml)
			_, err := LoadConfig(path)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestDBConfig_ConnectionURL(t *testing.T) {
	tests := []struct {
		name     string
		config   DBConfig
		expected string
	}{
		{
			name: "with password and ssl",
			config: DBConfig{
				Host: "localhost", Port: 5432, Database: "mydb",
				User: "admin", Password: "secret", SSLMode: "require",
			},
			expected: "postgres://admin:secret@localhost:5432/mydb?sslmode=require",
		},
		{
			name: "without password default ssl",
			config: DBConfig{
				Host: "remote", Port: 5433, Database: "app",
				User: "reader", SSLMode: "prefer",
			},
			expected: "postgres://reader@remote:5433/app?sslmode=prefer",
		},
		{
			name: "ssl disabled",
			config: DBConfig{
				Host: "local", Port: 5432, Database: "db",
				User: "usr", SSLMode: "disable",
			},
			expected: "postgres://usr@local:5432/db?sslmode=disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.config.ConnectionURL())
		})
	}
}

func TestDBConfig_ReplicationURL(t *testing.T) {
	cfg := DBConfig{
		Host: "localhost", Port: 5432, Database: "mydb",
		User: "replicator", Password: "pass", SSLMode: "require",
	}
	url := cfg.ReplicationURL()
	assert.Contains(t, url, "replication=database")
	assert.Contains(t, url, "sslmode=require")
	assert.Contains(t, url, "replicator:pass@localhost:5432/mydb")
}

func TestCheckpoint_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.checkpoint.json")

	// Load non-existent — should return empty.
	cp, err := LoadCheckpoint(path)
	require.NoError(t, err)
	assert.Equal(t, "init", cp.Phase)
	assert.Empty(t, cp.Tables)

	// Set some state and save.
	cp.SlotName = "test_slot"
	cp.Phase = "snapshot"
	cp.ConsistentLSN = "0/1A234B8"
	cp.Tables["public.users"] = &TableProgress{
		Status:     TableInProgress,
		RowsCopied: 5000,
		TotalRows:  10000,
	}

	err = cp.Save()
	require.NoError(t, err)

	// Reload and verify.
	cp2, err := LoadCheckpoint(path)
	require.NoError(t, err)
	assert.Equal(t, "test_slot", cp2.SlotName)
	assert.Equal(t, "snapshot", cp2.Phase)
	assert.Equal(t, "0/1A234B8", cp2.ConsistentLSN)

	tp := cp2.Tables["public.users"]
	require.NotNil(t, tp)
	assert.Equal(t, TableInProgress, tp.Status)
	assert.Equal(t, int64(5000), tp.RowsCopied)
	assert.Equal(t, int64(10000), tp.TotalRows)
}

func TestCheckpoint_IsSnapshotComplete(t *testing.T) {
	cp := &Checkpoint{
		Tables: map[string]*TableProgress{
			"public.users":    {Status: TableComplete},
			"public.orders":   {Status: TableComplete},
			"public.products": {Status: TableInProgress},
		},
	}
	assert.False(t, cp.IsSnapshotComplete())

	cp.Tables["public.products"].Status = TableComplete
	assert.True(t, cp.IsSnapshotComplete())
}

func TestCheckpoint_IsSnapshotComplete_Empty(t *testing.T) {
	cp := &Checkpoint{Tables: make(map[string]*TableProgress)}
	assert.False(t, cp.IsSnapshotComplete())
}

func TestCheckpoint_InitTables(t *testing.T) {
	cp := &Checkpoint{
		Tables: map[string]*TableProgress{
			"public.users": {Status: TableComplete, RowsCopied: 100},
		},
	}

	cp.InitTables([]string{"public.users", "public.orders"})

	// Existing table should not be overwritten.
	assert.Equal(t, TableComplete, cp.Tables["public.users"].Status)
	assert.Equal(t, int64(100), cp.Tables["public.users"].RowsCopied)

	// New table should be added as pending.
	require.NotNil(t, cp.Tables["public.orders"])
	assert.Equal(t, TablePending, cp.Tables["public.orders"].Status)
}

func TestCheckpoint_Delete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.checkpoint.json")

	cp, _ := LoadCheckpoint(path)
	cp.SlotName = "test"
	_ = cp.Save()

	// File should exist.
	_, err := os.Stat(path)
	require.NoError(t, err)

	// Delete.
	err = cp.Delete()
	require.NoError(t, err)

	// File should be gone.
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func TestParseTableName(t *testing.T) {
	tests := []struct {
		input  string
		schema string
		table  string
	}{
		{"public.users", "public", "users"},
		{"myschema.orders", "myschema", "orders"},
		{"users", "public", "users"},
	}

	for _, tt := range tests {
		s, tbl := parseTableName(tt.input)
		assert.Equal(t, tt.schema, s, "input: %s", tt.input)
		assert.Equal(t, tt.table, tbl, "input: %s", tt.input)
	}
}

func TestFormatCount(t *testing.T) {
	assert.Equal(t, "0", formatCount(0))
	assert.Equal(t, "999", formatCount(999))
	assert.Equal(t, "1,000", formatCount(1000))
	assert.Equal(t, "50,432", formatCount(50432))
	assert.Equal(t, "1,234,567", formatCount(1234567))
}

func writeTestFile(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	err := os.WriteFile(path, []byte(content), 0644)
	require.NoError(t, err)
	return path
}
