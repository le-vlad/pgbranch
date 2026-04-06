package migrate

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

	cfg := migrationConfig(t, source, target, []string{"public.users"})

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

func TestE2E_InitialSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	source, target := startSourceTarget(t, ctx)
	defer source.Stop(ctx)
	defer target.Stop(ctx)

	srcConn := connectTo(t, ctx, source)
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
		INSERT INTO users (name, email) VALUES
			('Alice', 'alice@example.com'),
			('Bob', 'bob@example.com'),
			('Charlie', 'charlie@example.com');
		INSERT INTO orders (user_id, total) VALUES (1, 99.99), (2, 149.50), (1, 25.00);
	`)
	require.NoError(t, err)
	srcConn.Close(ctx)

	cfg := migrationConfig(t, source, target, []string{"public.users", "public.orders"})
	migrator := NewMigrator(cfg, false, RunSnapshotOnly)

	timeoutCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	require.NoError(t, migrator.Run(timeoutCtx))

	tgtConn := connectTo(t, ctx, target)
	defer tgtConn.Close(ctx)

	var userCount, orderCount int
	require.NoError(t, tgtConn.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&userCount))
	assert.Equal(t, 3, userCount)

	require.NoError(t, tgtConn.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&orderCount))
	assert.Equal(t, 3, orderCount)

	var fkExists bool
	require.NoError(t, tgtConn.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM pg_constraint
			WHERE contype = 'f' AND conrelid = 'orders'::regclass
		)
	`).Scan(&fkExists))
	assert.True(t, fkExists)

	var total float64
	require.NoError(t, tgtConn.QueryRow(ctx,
		"SELECT total FROM orders WHERE user_id = (SELECT id FROM users WHERE email = 'bob@example.com')").Scan(&total))
	assert.InDelta(t, 149.50, total, 0.01)
}

func TestE2E_LogicalReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	source, target := startSourceTarget(t, ctx)
	defer source.Stop(ctx)
	defer target.Stop(ctx)

	srcConn := connectTo(t, ctx, source)
	_, err := srcConn.Exec(ctx, `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE
		);
		INSERT INTO users (name, email) VALUES
			('Alice', 'alice@example.com'),
			('Bob', 'bob@example.com');
	`)
	require.NoError(t, err)
	srcConn.Close(ctx)

	cfg := migrationConfig(t, source, target, []string{"public.users"})
	migrator := NewMigrator(cfg, false, RunFull)

	migrateCtx, migrateCancel := context.WithCancel(ctx)
	defer migrateCancel()

	migrateErr := make(chan error, 1)
	go func() {
		migrateErr <- migrator.Run(migrateCtx)
	}()

	tgtConn := connectTo(t, ctx, target)
	defer tgtConn.Close(ctx)

	require.Eventually(t, func() bool {
		var count int
		err := tgtConn.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&count)
		return err == nil && count == 2
	}, 30*time.Second, 500*time.Millisecond, "snapshot data not replicated to target")

	src := connectTo(t, ctx, source)
	defer src.Close(ctx)

	_, err = src.Exec(ctx, "INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@example.com')")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		var count int
		err := tgtConn.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&count)
		return err == nil && count == 3
	}, 30*time.Second, 500*time.Millisecond, "INSERT not replicated")

	_, err = src.Exec(ctx, "UPDATE users SET name = 'Alice Updated' WHERE email = 'alice@example.com'")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		var name string
		err := tgtConn.QueryRow(ctx, "SELECT name FROM users WHERE email = 'alice@example.com'").Scan(&name)
		return err == nil && name == "Alice Updated"
	}, 30*time.Second, 500*time.Millisecond, "UPDATE not replicated")

	_, err = src.Exec(ctx, "DELETE FROM users WHERE email = 'bob@example.com'")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		var count int
		err := tgtConn.QueryRow(ctx, "SELECT count(*) FROM users WHERE email = 'bob@example.com'").Scan(&count)
		return err == nil && count == 0
	}, 30*time.Second, 500*time.Millisecond, "DELETE not replicated")

	migrateCancel()

	select {
	case err := <-migrateErr:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("migrator did not shut down")
	}
}

func TestE2E_ReplicationDataTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	source, target := startSourceTarget(t, ctx)
	defer source.Stop(ctx)
	defer target.Stop(ctx)

	srcConn := connectTo(t, ctx, source)
	_, err := srcConn.Exec(ctx, `
		CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');
		CREATE TABLE typetest (
			id            SERIAL PRIMARY KEY,
			col_smallint  SMALLINT,
			col_bigint    BIGINT,
			col_real      REAL,
			col_double    DOUBLE PRECISION,
			col_numeric   NUMERIC(12,4),
			col_bool      BOOLEAN,
			col_text      TEXT,
			col_varchar   VARCHAR(100),
			col_char      CHAR(5),
			col_bytea     BYTEA,
			col_date      DATE,
			col_time      TIME,
			col_timetz    TIMETZ,
			col_ts        TIMESTAMP,
			col_tstz      TIMESTAMPTZ,
			col_interval  INTERVAL,
			col_uuid      UUID,
			col_json      JSON,
			col_jsonb     JSONB,
			col_inet      INET,
			col_cidr      CIDR,
			col_int_arr   INTEGER[],
			col_text_arr  TEXT[],
			col_enum      mood,
			col_nullable  TEXT
		);
		INSERT INTO typetest (
			col_smallint, col_bigint, col_real, col_double, col_numeric,
			col_bool, col_text, col_varchar, col_char, col_bytea,
			col_date, col_time, col_timetz, col_ts, col_tstz,
			col_interval, col_uuid, col_json, col_jsonb,
			col_inet, col_cidr, col_int_arr, col_text_arr,
			col_enum, col_nullable
		) VALUES (
			42, 9223372036854775807, 3.14, 2.718281828459045, 12345.6789,
			true, 'hello world', 'varchar value', 'abc  ', '\xDEADBEEF',
			'2025-06-15', '13:45:30', '13:45:30+02', '2025-06-15 13:45:30', '2025-06-15 13:45:30+00',
			'1 year 2 months 3 days', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
			'{"key": "value"}', '{"nested": {"num": 42}}',
			'192.168.1.1/24', '10.0.0.0/8',
			ARRAY[1,2,3], ARRAY['a','b','c'],
			'happy', NULL
		);
	`)
	require.NoError(t, err)
	srcConn.Close(ctx)

	cfg := migrationConfig(t, source, target, []string{"public.typetest"})
	migrator := NewMigrator(cfg, false, RunFull)

	migrateCtx, migrateCancel := context.WithCancel(ctx)
	defer migrateCancel()

	migrateErr := make(chan error, 1)
	go func() {
		migrateErr <- migrator.Run(migrateCtx)
	}()

	tgtConn := connectTo(t, ctx, target)
	defer tgtConn.Close(ctx)

	require.Eventually(t, func() bool {
		var count int
		err := tgtConn.QueryRow(ctx, "SELECT count(*) FROM typetest").Scan(&count)
		return err == nil && count == 1
	}, 30*time.Second, 500*time.Millisecond, "snapshot not replicated")

	var (
		colSmallint int16
		colBigint   int64
		colReal     float32
		colDouble   float64
		colNumeric  string
		colBool     bool
		colText     string
		colVarchar  string
		colChar     string
		colBytea    []byte
		colDate     string
		colUUID     string
		colJSON     string
		colJSONB    string
		colInet     string
		colCIDR     string
		colIntArr   string
		colTextArr  string
		colEnum     string
		colNullable *string
	)

	err = tgtConn.QueryRow(ctx, `
		SELECT col_smallint, col_bigint, col_real, col_double, col_numeric::text,
			col_bool, col_text, col_varchar, col_char, col_bytea,
			col_date::text, col_uuid::text, col_json::text, col_jsonb::text,
			col_inet::text, col_cidr::text,
			col_int_arr::text, col_text_arr::text,
			col_enum::text, col_nullable
		FROM typetest WHERE id = 1
	`).Scan(
		&colSmallint, &colBigint, &colReal, &colDouble, &colNumeric,
		&colBool, &colText, &colVarchar, &colChar, &colBytea,
		&colDate, &colUUID, &colJSON, &colJSONB,
		&colInet, &colCIDR,
		&colIntArr, &colTextArr,
		&colEnum, &colNullable,
	)
	require.NoError(t, err)

	assert.Equal(t, int16(42), colSmallint)
	assert.Equal(t, int64(9223372036854775807), colBigint)
	assert.InDelta(t, float32(3.14), colReal, 0.01)
	assert.InDelta(t, 2.718281828459045, colDouble, 1e-10)
	assert.Equal(t, "12345.6789", colNumeric)
	assert.True(t, colBool)
	assert.Equal(t, "hello world", colText)
	assert.Equal(t, "varchar value", colVarchar)
	assert.Equal(t, "abc  ", colChar)
	assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, colBytea)
	assert.Equal(t, "2025-06-15", colDate)
	assert.Equal(t, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", colUUID)
	assert.JSONEq(t, `{"key": "value"}`, colJSON)
	assert.JSONEq(t, `{"nested": {"num": 42}}`, colJSONB)
	assert.Equal(t, "192.168.1.1/24", colInet)
	assert.Equal(t, "10.0.0.0/8", colCIDR)
	assert.Equal(t, "{1,2,3}", colIntArr)
	assert.Equal(t, "{a,b,c}", colTextArr)
	assert.Equal(t, "happy", colEnum)
	assert.Nil(t, colNullable)

	src := connectTo(t, ctx, source)
	defer src.Close(ctx)

	_, err = src.Exec(ctx, `
		INSERT INTO typetest (
			col_smallint, col_bigint, col_real, col_double, col_numeric,
			col_bool, col_text, col_varchar, col_char, col_bytea,
			col_date, col_time, col_timetz, col_ts, col_tstz,
			col_interval, col_uuid, col_json, col_jsonb,
			col_inet, col_cidr, col_int_arr, col_text_arr,
			col_enum, col_nullable
		) VALUES (
			-1, -9223372036854775808, -0.5, 1e308, 0.0001,
			false, '', 'x', 'z    ', '\x00',
			'1970-01-01', '00:00:00', '00:00:00+00', '1970-01-01 00:00:00', '1970-01-01 00:00:00+00',
			'0 seconds', '00000000-0000-0000-0000-000000000000',
			'[]', '{"a": [1, 2]}',
			'::1', '::0/0',
			ARRAY[]::integer[], ARRAY['with space', 'with,comma'],
			'sad', 'not null'
		)
	`)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		var count int
		err := tgtConn.QueryRow(ctx, "SELECT count(*) FROM typetest").Scan(&count)
		return err == nil && count == 2
	}, 30*time.Second, 500*time.Millisecond, "INSERT with varied types not replicated")

	_, err = src.Exec(ctx, `UPDATE typetest SET col_jsonb = '{"updated": true}', col_enum = 'neutral', col_int_arr = '{10,20}' WHERE id = 1`)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		var jsonb, enum, arr string
		err := tgtConn.QueryRow(ctx, `SELECT col_jsonb::text, col_enum::text, col_int_arr::text FROM typetest WHERE id = 1`).Scan(&jsonb, &enum, &arr)
		if err != nil {
			return false
		}
		return enum == "neutral" && arr == "{10,20}"
	}, 30*time.Second, 500*time.Millisecond, "UPDATE of complex types not replicated")

	migrateCancel()

	select {
	case err := <-migrateErr:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("migrator did not shut down")
	}
}

// --- Helpers ---

func migrationConfig(t *testing.T, source, target *testutil.TestPostgres, tables []string) *Config {
	t.Helper()
	return &Config{
		Source: DBConfig{
			Host: source.Host, Port: source.Port, Database: source.Database,
			User: source.User, Password: source.Password, SSLMode: "disable",
		},
		Target: DBConfig{
			Host: target.Host, Port: target.Port, Database: target.Database,
			User: target.User, Password: target.Password, SSLMode: "disable",
		},
		Tables:          tables,
		SlotName:        fmt.Sprintf("test_slot_%d", time.Now().UnixNano()),
		PublicationName: fmt.Sprintf("test_pub_%d", time.Now().UnixNano()),
		BatchSize:       1000,
		configDir:       t.TempDir(),
	}
}

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
