package grace

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/le-vlad/pgbranch/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraceSchemaOnlyCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	source, target := startSourceTarget(t, ctx)
	defer source.Stop(ctx)
	defer target.Stop(ctx)

	// Create schema on source.
	srcConn := connectTo(t, ctx, source)
	defer srcConn.Close(ctx)

	_, err := srcConn.Exec(ctx, `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE
		);
		CREATE TABLE orders (
			id SERIAL PRIMARY KEY,
			user_id INTEGER REFERENCES users(id),
			total NUMERIC(10,2),
			created_at TIMESTAMP DEFAULT now()
		);
	`)
	require.NoError(t, err)

	// Run schema copy.
	tgtConn := connectTo(t, ctx, target)
	defer tgtConn.Close(ctx)

	tables := []string{"public.users", "public.orders"}
	err = CopySchema(ctx, srcConn, tgtConn, tables, source.Database)
	require.NoError(t, err)

	// Verify tables exist on target.
	var usersExist, ordersExist bool
	err = tgtConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'users')").Scan(&usersExist)
	require.NoError(t, err)
	assert.True(t, usersExist)

	err = tgtConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'orders')").Scan(&ordersExist)
	require.NoError(t, err)
	assert.True(t, ordersExist)

	// Verify columns exist.
	var colCount int
	err = tgtConn.QueryRow(ctx,
		"SELECT count(*) FROM information_schema.columns WHERE table_name = 'users'").Scan(&colCount)
	require.NoError(t, err)
	assert.Equal(t, 3, colCount) // id, name, email
}

func TestGraceFullMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	source, target := startSourceTarget(t, ctx)
	defer source.Stop(ctx)
	defer target.Stop(ctx)

	// Create schema and seed data on source.
	srcConn := connectTo(t, ctx, source)

	_, err := srcConn.Exec(ctx, `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		);
		INSERT INTO users (name, email) VALUES
			('Alice', 'alice@example.com'),
			('Bob', 'bob@example.com'),
			('Charlie', 'charlie@example.com');
	`)
	require.NoError(t, err)
	srcConn.Close(ctx)

	// Run snapshot-only migration.
	cfg := &Config{
		Source: DBConfig{
			Host:     source.Host,
			Port:     source.Port,
			Database: source.Database,
			User:     source.User,
			Password: source.Password,
			SSLMode:  "disable",
		},
		Target: DBConfig{
			Host:     target.Host,
			Port:     target.Port,
			Database: target.Database,
			User:     target.User,
			Password: target.Password,
			SSLMode:  "disable",
		},
		Tables:          []string{"public.users"},
		SlotName:        fmt.Sprintf("test_slot_%d", time.Now().UnixNano()),
		PublicationName: fmt.Sprintf("test_pub_%d", time.Now().UnixNano()),
		BatchSize:       1000,
		configDir:       t.TempDir(),
	}

	migrator := NewMigrator(cfg, false, RunSnapshotOnly)

	timeoutCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = migrator.Run(timeoutCtx)
	require.NoError(t, err)

	// Verify data on target.
	tgtConn := connectTo(t, ctx, target)
	defer tgtConn.Close(ctx)

	var count int
	err = tgtConn.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	var name string
	err = tgtConn.QueryRow(ctx, "SELECT name FROM users WHERE email = 'alice@example.com'").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Alice", name)
}

func TestValidateSource_WalLevel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start with wal_level=logical.
	source := startLogicalPG(t, ctx)
	defer source.Stop(ctx)

	conn := connectTo(t, ctx, source)
	defer conn.Close(ctx)

	err := ValidateSource(ctx, conn)
	require.NoError(t, err)
}

func TestResolveTables_Wildcard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	source := startLogicalPG(t, ctx)
	defer source.Stop(ctx)

	conn := connectTo(t, ctx, source)
	defer conn.Close(ctx)

	_, err := conn.Exec(ctx, `
		CREATE TABLE test_a (id SERIAL PRIMARY KEY);
		CREATE TABLE test_b (id SERIAL PRIMARY KEY);
	`)
	require.NoError(t, err)

	tables, err := ResolveTables(ctx, conn, []string{"*"})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(tables), 2)

	// Check our tables are in the list.
	found := map[string]bool{}
	for _, t := range tables {
		found[t] = true
	}
	assert.True(t, found["public.test_a"])
	assert.True(t, found["public.test_b"])
}

func TestResolveTables_Specific(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	source := startLogicalPG(t, ctx)
	defer source.Stop(ctx)

	conn := connectTo(t, ctx, source)
	defer conn.Close(ctx)

	_, err := conn.Exec(ctx, "CREATE TABLE resolve_test (id SERIAL PRIMARY KEY)")
	require.NoError(t, err)

	// Existing table should pass.
	tables, err := ResolveTables(ctx, conn, []string{"public.resolve_test"})
	require.NoError(t, err)
	assert.Equal(t, []string{"public.resolve_test"}, tables)

	// Non-existent table should fail.
	_, err = ResolveTables(ctx, conn, []string{"public.nonexistent"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

// --- Helpers ---

func startSourceTarget(t *testing.T, ctx context.Context) (*testutil.TestPostgres, *testutil.TestPostgres) {
	t.Helper()

	source := startLogicalPG(t, ctx)
	target := startPlainPG(t, ctx)

	return source, target
}

func startLogicalPG(t *testing.T, ctx context.Context) *testutil.TestPostgres {
	t.Helper()

	pg, err := testutil.StartPostgresContainerWithArgs(ctx, []string{
		"-c", "wal_level=logical",
		"-c", "max_replication_slots=4",
		"-c", "max_wal_senders=4",
	})
	require.NoError(t, err)
	return pg
}

func startPlainPG(t *testing.T, ctx context.Context) *testutil.TestPostgres {
	t.Helper()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	return pg
}

func connectTo(t *testing.T, ctx context.Context, pg *testutil.TestPostgres) *pgx.Conn {
	t.Helper()

	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		pg.User, pg.Password, pg.Host, pg.Port, pg.Database)

	conn, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	return conn
}
