package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/le-vlad/pgbranch/internal/testutil"
	"github.com/le-vlad/pgbranch/pkg/config"
)

func TestClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	cfg := pg.GetConfig()
	client := NewClient(cfg)

	t.Run("TestConnection", func(t *testing.T) {
		err := client.TestConnection()
		require.NoError(t, err)
	})

	t.Run("DatabaseExists", func(t *testing.T) {
		exists, err := client.DatabaseExists()
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("CreateAndDropDatabase", func(t *testing.T) {
		newCfg := &config.Config{
			Database: "test_create_drop_db",
			Host:     cfg.Host,
			Port:     cfg.Port,
			User:     cfg.User,
			Password: cfg.Password,
		}
		newClient := NewClient(newCfg)

		exists, err := newClient.DatabaseExists()
		require.NoError(t, err)
		assert.False(t, exists)

		err = newClient.CreateDatabase()
		require.NoError(t, err)

		exists, err = newClient.DatabaseExists()
		require.NoError(t, err)
		assert.True(t, exists)

		err = newClient.DropDatabase()
		require.NoError(t, err)

		exists, err = newClient.DatabaseExists()
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("TerminateConnections", func(t *testing.T) {
		err := client.TerminateConnections()
		require.NoError(t, err)
	})
}

func TestSnapshotAndRestoreIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	cfg := pg.GetConfig()
	client := NewClient(cfg)

	setupSQL := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100) UNIQUE
		);
		INSERT INTO users (name, email) VALUES
			('Alice', 'alice@example.com'),
			('Bob', 'bob@example.com'),
			('Charlie', 'charlie@example.com');
	`
	err = execSQL(ctx, cfg, setupSQL)
	require.NoError(t, err)

	snapshotDBName := cfg.Database + "_snapshot_test"

	t.Run("CreateSnapshot", func(t *testing.T) {
		err := client.CreateSnapshot(snapshotDBName)
		require.NoError(t, err)

		snapshotCfg := &config.Config{
			Database: snapshotDBName,
			Host:     cfg.Host,
			Port:     cfg.Port,
			User:     cfg.User,
			Password: cfg.Password,
		}
		snapshotClient := NewClient(snapshotCfg)
		exists, err := snapshotClient.DatabaseExists()
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("RestoreFromSnapshot", func(t *testing.T) {
		modifySQL := `
			DELETE FROM users WHERE name = 'Alice';
			INSERT INTO users (name, email) VALUES ('David', 'david@example.com');
		`
		err := execSQL(ctx, cfg, modifySQL)
		require.NoError(t, err)

		count, err := countRows(ctx, cfg, "users")
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		err = client.RestoreFromSnapshot(snapshotDBName)
		require.NoError(t, err)

		count, err = countRows(ctx, cfg, "users")
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		exists, err := rowExists(ctx, cfg, "users", "name", "Alice")
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = rowExists(ctx, cfg, "users", "name", "David")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("DeleteSnapshot", func(t *testing.T) {
		err := client.DeleteSnapshot(snapshotDBName)
		require.NoError(t, err)

		snapshotCfg := &config.Config{
			Database: snapshotDBName,
			Host:     cfg.Host,
			Port:     cfg.Port,
			User:     cfg.User,
			Password: cfg.Password,
		}
		snapshotClient := NewClient(snapshotCfg)
		exists, err := snapshotClient.DatabaseExists()
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestCreateSnapshotDBAndRestoreFromSnapshotDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	cfg := pg.GetConfig()

	setupSQL := `
		CREATE TABLE products (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			price DECIMAL(10, 2)
		);
		INSERT INTO products (name, price) VALUES
			('Widget', 9.99),
			('Gadget', 19.99);
	`
	err = execSQL(ctx, cfg, setupSQL)
	require.NoError(t, err)

	snapshotDBName := cfg.Database + "_helper_test_snapshot"

	err = CreateSnapshotDB(cfg, snapshotDBName)
	require.NoError(t, err)

	snapshotCfg := &config.Config{
		Database: snapshotDBName,
		Host:     cfg.Host,
		Port:     cfg.Port,
		User:     cfg.User,
		Password: cfg.Password,
	}
	snapshotClient := NewClient(snapshotCfg)
	exists, err := snapshotClient.DatabaseExists()
	require.NoError(t, err)
	assert.True(t, exists)

	err = execSQL(ctx, cfg, "DELETE FROM products")
	require.NoError(t, err)

	count, err := countRows(ctx, cfg, "products")
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	err = RestoreFromSnapshotDB(cfg, snapshotDBName)
	require.NoError(t, err)

	count, err = countRows(ctx, cfg, "products")
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	err = DeleteSnapshotDB(cfg, snapshotDBName)
	require.NoError(t, err)
}

func execSQL(ctx context.Context, cfg *config.Config, sql string) error {
	conn, err := pgx.Connect(ctx, cfg.ConnectionURLForDB(cfg.Database))
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, sql)
	return err
}

func countRows(ctx context.Context, cfg *config.Config, table string) (int, error) {
	conn, err := pgx.Connect(ctx, cfg.ConnectionURLForDB(cfg.Database))
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func rowExists(ctx context.Context, cfg *config.Config, table, column, value string) (bool, error) {
	conn, err := pgx.Connect(ctx, cfg.ConnectionURLForDB(cfg.Database))
	if err != nil {
		return false, err
	}
	defer conn.Close(ctx)

	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM " + table + " WHERE " + column + " = $1)"
	err = conn.QueryRow(ctx, query, value).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func TestIsCriticalRestoreError(t *testing.T) {
	tests := []struct {
		name   string
		stderr string
		want   bool
	}{
		{
			name:   "empty stderr",
			stderr: "",
			want:   false,
		},
		{
			name:   "no ERROR keyword",
			stderr: "pg_restore: warning: some warning message",
			want:   false,
		},
		{
			name:   "non-critical unrecognized configuration parameter",
			stderr: "pg_restore: ERROR: unrecognized configuration parameter \"some_param\"",
			want:   false,
		},
		{
			name:   "non-critical errors ignored on restore",
			stderr: "pg_restore: ERROR: unrecognized configuration parameter \"some_param\"\npg_restore: warning: errors ignored on restore: 1",
			want:   false,
		},
		{
			name:   "critical error relation does not exist",
			stderr: "pg_restore: ERROR: relation \"users\" does not exist",
			want:   true,
		},
		{
			name:   "both non-critical and critical errors",
			stderr: "pg_restore: ERROR: unrecognized configuration parameter \"some_param\"\npg_restore: ERROR: relation \"users\" does not exist",
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCriticalRestoreError(tt.stderr)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildDumpArgs(t *testing.T) {
	cfg := &config.Config{
		Host: "localhost",
		Port: 5432,
		User: "testuser",
	}
	client := NewClient(cfg)

	t.Run("nil opts", func(t *testing.T) {
		args := client.buildDumpArgs("mydb", nil)
		expected := []string{
			"-h", "localhost",
			"-p", "5432",
			"-U", "testuser",
			"-Fc",
			"--no-password",
			"mydb",
		}
		assert.Equal(t, expected, args)
	})

	t.Run("schema only", func(t *testing.T) {
		args := client.buildDumpArgs("mydb", &DumpOptions{SchemaOnly: true})
		assert.Contains(t, args, "--schema-only")
	})

	t.Run("data only", func(t *testing.T) {
		args := client.buildDumpArgs("mydb", &DumpOptions{DataOnly: true})
		assert.Contains(t, args, "--data-only")
	})

	t.Run("exclude tables", func(t *testing.T) {
		args := client.buildDumpArgs("mydb", &DumpOptions{
			ExcludeTables: []string{"table_a", "table_b"},
		})
		assert.Contains(t, args, "--exclude-table")

		excludeCount := 0
		for i, arg := range args {
			if arg == "--exclude-table" {
				excludeCount++
				if i+1 < len(args) {
					assert.Contains(t, []string{"table_a", "table_b"}, args[i+1])
				}
			}
		}
		assert.Equal(t, 2, excludeCount)
	})
}

func TestBuildRestoreArgs(t *testing.T) {
	cfg := &config.Config{
		Host: "dbhost",
		Port: 5433,
		User: "restoreuser",
	}
	client := NewClient(cfg)

	args := client.buildRestoreArgs("targetdb")
	expected := []string{
		"-h", "dbhost",
		"-p", "5433",
		"-U", "restoreuser",
		"-d", "targetdb",
		"--no-password",
		"--no-owner",
		"--no-privileges",
	}
	assert.Equal(t, expected, args)
}

func TestBuildEnv(t *testing.T) {
	t.Run("without password", func(t *testing.T) {
		cfg := &config.Config{
			Host: "localhost",
			Port: 5432,
			User: "testuser",
		}
		client := NewClient(cfg)
		env := client.buildEnv()

		for _, v := range env {
			assert.NotContains(t, v, "PGPASSWORD=")
		}
	})

	t.Run("with password", func(t *testing.T) {
		cfg := &config.Config{
			Host:     "localhost",
			Port:     5432,
			User:     "testuser",
			Password: "secret123",
		}
		client := NewClient(cfg)
		env := client.buildEnv()

		found := false
		for _, v := range env {
			if v == "PGPASSWORD=secret123" {
				found = true
				break
			}
		}
		assert.True(t, found, "expected PGPASSWORD=secret123 in env")
	})
}

func TestSanitizeIdentifier(t *testing.T) {
	t.Run("simple name", func(t *testing.T) {
		assert.Equal(t, `"mydb"`, sanitizeIdentifier("mydb"))
	})

	t.Run("name with double quotes", func(t *testing.T) {
		assert.Equal(t, `"my""db"`, sanitizeIdentifier(`my"db`))
	})
}

func newMockClient(cfg *config.Config) *Client {
	return &Client{Config: cfg}
}

func TestDumpDatabase_Success(t *testing.T) {
	cfg := &config.Config{Host: "localhost", Port: 5432, User: "testuser"}
	client := newMockClient(cfg)
	client.runDump = func(ctx context.Context, args []string, env []string, w io.Writer) error {
		_, err := w.Write([]byte("dump-output"))
		return err
	}

	var buf bytes.Buffer
	err := client.DumpDatabase(context.Background(), "mydb", &buf, nil)
	require.NoError(t, err)
	assert.Equal(t, "dump-output", buf.String())
}

func TestDumpDatabase_Error(t *testing.T) {
	cfg := &config.Config{Host: "localhost", Port: 5432, User: "testuser"}
	client := newMockClient(cfg)
	client.runDump = func(ctx context.Context, args []string, env []string, w io.Writer) error {
		return errors.New("dump failed")
	}

	var buf bytes.Buffer
	err := client.DumpDatabase(context.Background(), "mydb", &buf, nil)
	require.Error(t, err)
	assert.Equal(t, "dump failed", err.Error())
}

func TestDumpDatabase_PassesCorrectArgs(t *testing.T) {
	cfg := &config.Config{Host: "dbhost", Port: 5433, User: "admin", Password: "secret"}
	client := newMockClient(cfg)

	var capturedArgs []string
	var capturedEnv []string
	client.runDump = func(ctx context.Context, args []string, env []string, w io.Writer) error {
		capturedArgs = args
		capturedEnv = env
		return nil
	}

	var buf bytes.Buffer
	err := client.DumpDatabase(context.Background(), "testdb", &buf, &DumpOptions{SchemaOnly: true})
	require.NoError(t, err)

	assert.Contains(t, capturedArgs, "-h")
	assert.Contains(t, capturedArgs, "dbhost")
	assert.Contains(t, capturedArgs, "-p")
	assert.Contains(t, capturedArgs, "5433")
	assert.Contains(t, capturedArgs, "-U")
	assert.Contains(t, capturedArgs, "admin")
	assert.Contains(t, capturedArgs, "-Fc")
	assert.Contains(t, capturedArgs, "testdb")
	assert.Contains(t, capturedArgs, "--schema-only")

	found := false
	for _, v := range capturedEnv {
		if v == "PGPASSWORD=secret" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected PGPASSWORD=secret in env")
}

func TestRestoreDatabase_Success(t *testing.T) {
	cfg := &config.Config{Host: "localhost", Port: 5432, User: "testuser"}
	client := newMockClient(cfg)
	client.runRestore = func(ctx context.Context, args []string, env []string, r io.Reader) (string, error) {
		return "", nil
	}

	err := client.RestoreDatabase(context.Background(), "mydb", bytes.NewReader(nil))
	require.NoError(t, err)
}

func TestRestoreDatabase_NonCriticalError(t *testing.T) {
	cfg := &config.Config{Host: "localhost", Port: 5432, User: "testuser"}
	client := newMockClient(cfg)
	client.runRestore = func(ctx context.Context, args []string, env []string, r io.Reader) (string, error) {
		return `pg_restore: ERROR: unrecognized configuration parameter "some_param"`, fmt.Errorf("exit status 1")
	}

	err := client.RestoreDatabase(context.Background(), "mydb", bytes.NewReader(nil))
	require.NoError(t, err)
}

func TestRestoreDatabase_CriticalError(t *testing.T) {
	cfg := &config.Config{Host: "localhost", Port: 5432, User: "testuser"}
	client := newMockClient(cfg)
	client.runRestore = func(ctx context.Context, args []string, env []string, r io.Reader) (string, error) {
		return `pg_restore: ERROR: relation "foo" does not exist`, fmt.Errorf("exit status 1")
	}

	err := client.RestoreDatabase(context.Background(), "mydb", bytes.NewReader(nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pg_restore failed")
}

func TestRestoreDatabase_PassesCorrectArgs(t *testing.T) {
	cfg := &config.Config{Host: "restorehost", Port: 5434, User: "restoreuser"}
	client := newMockClient(cfg)

	var capturedArgs []string
	client.runRestore = func(ctx context.Context, args []string, env []string, r io.Reader) (string, error) {
		capturedArgs = args
		return "", nil
	}

	err := client.RestoreDatabase(context.Background(), "targetdb", bytes.NewReader(nil))
	require.NoError(t, err)

	expected := []string{
		"-h", "restorehost",
		"-p", "5434",
		"-U", "restoreuser",
		"-d", "targetdb",
		"--no-password",
		"--no-owner",
		"--no-privileges",
	}
	assert.Equal(t, expected, capturedArgs)
}

func TestDumpSnapshotToWriter_DelegatesToDump(t *testing.T) {
	cfg := &config.Config{Host: "localhost", Port: 5432, User: "testuser"}
	client := newMockClient(cfg)
	client.runDump = func(ctx context.Context, args []string, env []string, w io.Writer) error {
		_, err := w.Write([]byte("snapshot-data"))
		return err
	}

	var buf bytes.Buffer
	err := client.DumpSnapshotToWriter(context.Background(), "snap_db", &buf)
	require.NoError(t, err)
	assert.Equal(t, "snapshot-data", buf.String())
}
