package postgres

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

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

func TestDumpAndRestoreIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	tmpDir, err := os.MkdirTemp("", "pgbranch-dump-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

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
	err = execSQL(cfg, setupSQL)
	require.NoError(t, err)

	t.Run("Dump", func(t *testing.T) {
		dumpPath := filepath.Join(tmpDir, "test.dump")

		err := client.Dump(dumpPath)
		require.NoError(t, err)

		info, err := os.Stat(dumpPath)
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(0))
	})

	t.Run("RestoreClean", func(t *testing.T) {
		dumpPath := filepath.Join(tmpDir, "restore_test.dump")

		err := client.Dump(dumpPath)
		require.NoError(t, err)

		modifySQL := `
			DELETE FROM users WHERE name = 'Alice';
			INSERT INTO users (name, email) VALUES ('David', 'david@example.com');
		`
		err = execSQL(cfg, modifySQL)
		require.NoError(t, err)

		count, err := countRows(cfg, "users")
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		err = client.RestoreClean(dumpPath)
		require.NoError(t, err)

		count, err = countRows(cfg, "users")
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		exists, err := rowExists(cfg, "users", "name", "Alice")
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = rowExists(cfg, "users", "name", "David")
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestDumpToPathAndRestoreFromPath(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	tmpDir, err := os.MkdirTemp("", "pgbranch-helper-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

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
	err = execSQL(cfg, setupSQL)
	require.NoError(t, err)

	dumpPath := filepath.Join(tmpDir, "helper_test.dump")

	err = DumpToPath(cfg, dumpPath)
	require.NoError(t, err)

	_, err = os.Stat(dumpPath)
	require.NoError(t, err)

	err = execSQL(cfg, "DELETE FROM products")
	require.NoError(t, err)

	count, err := countRows(cfg, "products")
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	err = RestoreFromPath(cfg, dumpPath)
	require.NoError(t, err)

	count, err = countRows(cfg, "products")
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func execSQL(cfg *config.Config, sql string) error {
	cmd := exec.Command("psql",
		"-h", cfg.Host,
		"-p", fmt.Sprintf("%d", cfg.Port),
		"-U", cfg.User,
		"-d", cfg.Database,
		"-c", sql,
	)

	if cfg.Password != "" {
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", cfg.Password))
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("psql error: %s, output: %s", err, string(output))
	}
	return nil
}

func countRows(cfg *config.Config, table string) (int, error) {
	cmd := exec.Command("psql",
		"-h", cfg.Host,
		"-p", fmt.Sprintf("%d", cfg.Port),
		"-U", cfg.User,
		"-d", cfg.Database,
		"-tAc", fmt.Sprintf("SELECT COUNT(*) FROM %s", table),
	)

	if cfg.Password != "" {
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", cfg.Password))
	}

	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("psql error: %w", err)
	}

	count, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		return 0, fmt.Errorf("failed to parse count: %w", err)
	}

	return count, nil
}

func rowExists(cfg *config.Config, table, column, value string) (bool, error) {
	cmd := exec.Command("psql",
		"-h", cfg.Host,
		"-p", fmt.Sprintf("%d", cfg.Port),
		"-U", cfg.User,
		"-d", cfg.Database,
		"-tAc", fmt.Sprintf("SELECT 1 FROM %s WHERE %s = '%s' LIMIT 1", table, column, value),
	)

	if cfg.Password != "" {
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", cfg.Password))
	}

	output, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("psql error: %w", err)
	}

	return strings.TrimSpace(string(output)) == "1", nil
}
