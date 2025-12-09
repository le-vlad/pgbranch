package postgres

import (
	"context"
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
