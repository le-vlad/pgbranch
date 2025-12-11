package core

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/le-vlad/pgbranch/internal/postgres"
	"github.com/le-vlad/pgbranch/internal/storage"
	"github.com/le-vlad/pgbranch/internal/testutil"
	"github.com/le-vlad/pgbranch/pkg/config"
)

func TestInitialize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	testDir := testutil.SetupTestDir(t)
	defer testDir.Cleanup(t)

	cfg := pg.GetConfig()

	err = Initialize(cfg.Database, cfg.Host, cfg.Port, cfg.User, cfg.Password)
	require.NoError(t, err)

	assert.True(t, config.IsInitialized())

	loadedCfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, cfg.Database, loadedCfg.Database)
	assert.Equal(t, cfg.Host, loadedCfg.Host)
	assert.Equal(t, cfg.Port, loadedCfg.Port)
	assert.Equal(t, cfg.User, loadedCfg.User)

	meta, err := storage.LoadMetadata()
	require.NoError(t, err)
	assert.Empty(t, meta.CurrentBranch)
	assert.Len(t, meta.Branches, 0)
}

func TestBrancherOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	testDir := testutil.SetupTestDir(t)
	defer testDir.Cleanup(t)

	cfg := pg.GetConfig()

	err = Initialize(cfg.Database, cfg.Host, cfg.Port, cfg.User, cfg.Password)
	require.NoError(t, err)

	setupSQL := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100) UNIQUE
		);
		INSERT INTO users (name, email) VALUES
			('Alice', 'alice@example.com'),
			('Bob', 'bob@example.com');
	`
	err = execSQL(ctx, cfg, setupSQL)
	require.NoError(t, err)

	brancher, err := NewBrancher()
	require.NoError(t, err)

	t.Run("CreateBranch", func(t *testing.T) {
		err := brancher.CreateBranch("main")
		require.NoError(t, err)

		branch, ok := brancher.Metadata.GetBranch("main")
		assert.True(t, ok)
		assert.Equal(t, "main", branch.Name)

		expectedSnapshotDB := storage.SnapshotDBName(cfg.Database, "main")
		assert.Equal(t, expectedSnapshotDB, branch.Snapshot)

		snapshotCfg := &config.Config{
			Database: branch.Snapshot,
			Host:     cfg.Host,
			Port:     cfg.Port,
			User:     cfg.User,
			Password: cfg.Password,
		}
		snapshotClient := postgres.NewClient(snapshotCfg)
		exists, err := snapshotClient.DatabaseExists()
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("CreateBranchDuplicate", func(t *testing.T) {
		err := brancher.CreateBranch("main")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("ListBranches", func(t *testing.T) {
		branches := brancher.ListBranches()
		assert.Len(t, branches, 1)
		assert.Equal(t, "main", branches[0].Name)
	})

	t.Run("CreateSecondBranch", func(t *testing.T) {
		brancher.Metadata.CurrentBranch = "main"
		brancher.Metadata.Save()

		err := brancher.CreateBranch("feature-1")
		require.NoError(t, err)

		branch, ok := brancher.Metadata.GetBranch("feature-1")
		assert.True(t, ok)
		assert.Equal(t, "main", branch.Parent)

		branches := brancher.ListBranches()
		assert.Len(t, branches, 2)
	})

	t.Run("Status", func(t *testing.T) {
		brancher.Metadata.CurrentBranch = "feature-1"
		brancher.Metadata.Save()

		currentBranch, count := brancher.Status()
		assert.Equal(t, "feature-1", currentBranch)
		assert.Equal(t, 2, count)
	})
}

func TestCheckoutWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	testDir := testutil.SetupTestDir(t)
	defer testDir.Cleanup(t)

	cfg := pg.GetConfig()

	err = Initialize(cfg.Database, cfg.Host, cfg.Port, cfg.User, cfg.Password)
	require.NoError(t, err)

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

	brancher, err := NewBrancher()
	require.NoError(t, err)

	err = brancher.CreateBranch("main")
	require.NoError(t, err)
	brancher.Metadata.CurrentBranch = "main"
	brancher.Metadata.Save()

	count, err := countRows(ctx, cfg, "products")
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	modifySQL := `
		DELETE FROM products WHERE name = 'Widget';
		INSERT INTO products (name, price) VALUES ('SuperWidget', 29.99);
		UPDATE products SET price = 24.99 WHERE name = 'Gadget';
	`
	err = execSQL(ctx, cfg, modifySQL)
	require.NoError(t, err)

	count, err = countRows(ctx, cfg, "products")
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	exists, err := rowExists(ctx, cfg, "products", "name", "Widget")
	require.NoError(t, err)
	assert.False(t, exists)

	exists, err = rowExists(ctx, cfg, "products", "name", "SuperWidget")
	require.NoError(t, err)
	assert.True(t, exists)

	err = brancher.Checkout("main")
	require.NoError(t, err)

	assert.Equal(t, "main", brancher.Metadata.CurrentBranch)

	count, err = countRows(ctx, cfg, "products")
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	exists, err = rowExists(ctx, cfg, "products", "name", "Widget")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = rowExists(ctx, cfg, "products", "name", "SuperWidget")
	require.NoError(t, err)
	assert.False(t, exists)

	price, err := getProductPrice(ctx, cfg, "Gadget")
	require.NoError(t, err)
	assert.Equal(t, "19.99", price)
}

func TestDeleteBranch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	testDir := testutil.SetupTestDir(t)
	defer testDir.Cleanup(t)

	cfg := pg.GetConfig()

	err = Initialize(cfg.Database, cfg.Host, cfg.Port, cfg.User, cfg.Password)
	require.NoError(t, err)

	err = execSQL(ctx, cfg, "CREATE TABLE test (id SERIAL PRIMARY KEY)")
	require.NoError(t, err)

	brancher, err := NewBrancher()
	require.NoError(t, err)

	err = brancher.CreateBranch("main")
	require.NoError(t, err)
	err = brancher.CreateBranch("feature-1")
	require.NoError(t, err)
	brancher.Metadata.CurrentBranch = "main"
	brancher.Metadata.Save()

	feature1Branch, _ := brancher.Metadata.GetBranch("feature-1")
	feature1SnapshotDB := feature1Branch.Snapshot

	err = brancher.DeleteBranch("feature-1", false)
	require.NoError(t, err)

	assert.False(t, brancher.Metadata.BranchExists("feature-1"))

	snapshotCfg := &config.Config{
		Database: feature1SnapshotDB,
		Host:     cfg.Host,
		Port:     cfg.Port,
		User:     cfg.User,
		Password: cfg.Password,
	}
	snapshotClient := postgres.NewClient(snapshotCfg)
	exists, err := snapshotClient.DatabaseExists()
	require.NoError(t, err)
	assert.False(t, exists)

	err = brancher.DeleteBranch("main", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot delete current branch")

	err = brancher.DeleteBranch("main", true)
	require.NoError(t, err)
	assert.Empty(t, brancher.Metadata.CurrentBranch)
}

func TestUpdateBranch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	testDir := testutil.SetupTestDir(t)
	defer testDir.Cleanup(t)

	cfg := pg.GetConfig()

	err = Initialize(cfg.Database, cfg.Host, cfg.Port, cfg.User, cfg.Password)
	require.NoError(t, err)

	err = execSQL(ctx, cfg, "CREATE TABLE items (id SERIAL PRIMARY KEY, name VARCHAR(100)); INSERT INTO items (name) VALUES ('Item1')")
	require.NoError(t, err)

	brancher, err := NewBrancher()
	require.NoError(t, err)

	err = brancher.CreateBranch("main")
	require.NoError(t, err)
	brancher.Metadata.CurrentBranch = "main"
	brancher.Metadata.Save()

	err = execSQL(ctx, cfg, "INSERT INTO items (name) VALUES ('Item2'), ('Item3'), ('Item4'), ('Item5')")
	require.NoError(t, err)

	err = brancher.UpdateBranch("main")
	require.NoError(t, err)

	branch, _ := brancher.Metadata.GetBranch("main")
	snapshotCfg := &config.Config{
		Database: branch.Snapshot,
		Host:     cfg.Host,
		Port:     cfg.Port,
		User:     cfg.User,
		Password: cfg.Password,
	}
	snapshotClient := postgres.NewClient(snapshotCfg)
	exists, err := snapshotClient.DatabaseExists()
	require.NoError(t, err)
	assert.True(t, exists)

	count, err := countRowsInDB(ctx, snapshotCfg, "items")
	require.NoError(t, err)
	assert.Equal(t, 5, count)
}

func countRowsInDB(ctx context.Context, cfg *config.Config, table string) (int, error) {
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

func TestCheckoutNonExistentBranch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	testDir := testutil.SetupTestDir(t)
	defer testDir.Cleanup(t)

	cfg := pg.GetConfig()

	err = Initialize(cfg.Database, cfg.Host, cfg.Port, cfg.User, cfg.Password)
	require.NoError(t, err)

	brancher, err := NewBrancher()
	require.NoError(t, err)

	err = brancher.Checkout("non-existent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestFullE2EWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	testDir := testutil.SetupTestDir(t)
	defer testDir.Cleanup(t)

	cfg := pg.GetConfig()

	err = Initialize(cfg.Database, cfg.Host, cfg.Port, cfg.User, cfg.Password)
	require.NoError(t, err)

	setupSQL := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) UNIQUE NOT NULL,
			email VARCHAR(100) UNIQUE NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE posts (
			id SERIAL PRIMARY KEY,
			user_id INTEGER REFERENCES users(id),
			title VARCHAR(200) NOT NULL,
			content TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		INSERT INTO users (username, email) VALUES
			('alice', 'alice@example.com'),
			('bob', 'bob@example.com');

		INSERT INTO posts (user_id, title, content) VALUES
			(1, 'Hello World', 'My first post'),
			(2, 'Test Post', 'Just testing');
	`
	err = execSQL(ctx, cfg, setupSQL)
	require.NoError(t, err)

	brancher, err := NewBrancher()
	require.NoError(t, err)

	err = brancher.CreateBranch("main")
	require.NoError(t, err)
	brancher.Metadata.CurrentBranch = "main"
	brancher.Metadata.Save()

	userCount, err := countRows(ctx, cfg, "users")
	require.NoError(t, err)
	assert.Equal(t, 2, userCount)

	postCount, err := countRows(ctx, cfg, "posts")
	require.NoError(t, err)
	assert.Equal(t, 2, postCount)

	featureSQL := `
		CREATE TABLE comments (
			id SERIAL PRIMARY KEY,
			post_id INTEGER REFERENCES posts(id),
			user_id INTEGER REFERENCES users(id),
			content TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		INSERT INTO comments (post_id, user_id, content) VALUES
			(1, 2, 'Great post!'),
			(1, 1, 'Thanks!'),
			(2, 1, 'Nice test');

		INSERT INTO users (username, email) VALUES ('charlie', 'charlie@example.com');
	`
	err = execSQL(ctx, cfg, featureSQL)
	require.NoError(t, err)

	userCount, err = countRows(ctx, cfg, "users")
	require.NoError(t, err)
	assert.Equal(t, 3, userCount)

	commentCount, err := countRows(ctx, cfg, "comments")
	require.NoError(t, err)
	assert.Equal(t, 3, commentCount)

	err = brancher.CreateBranch("feature-add-comments")
	require.NoError(t, err)

	err = brancher.Checkout("main")
	require.NoError(t, err)

	userCount, err = countRows(ctx, cfg, "users")
	require.NoError(t, err)
	assert.Equal(t, 2, userCount)

	_, err = countRows(ctx, cfg, "comments")
	require.Error(t, err)

	exists, err := rowExists(ctx, cfg, "users", "username", "charlie")
	require.NoError(t, err)
	assert.False(t, exists)

	err = brancher.Checkout("feature-add-comments")
	require.NoError(t, err)

	userCount, err = countRows(ctx, cfg, "users")
	require.NoError(t, err)
	assert.Equal(t, 3, userCount)

	commentCount, err = countRows(ctx, cfg, "comments")
	require.NoError(t, err)
	assert.Equal(t, 3, commentCount)

	exists, err = rowExists(ctx, cfg, "users", "username", "charlie")
	require.NoError(t, err)
	assert.True(t, exists)

	err = brancher.Checkout("main")
	require.NoError(t, err)

	err = brancher.DeleteBranch("feature-add-comments", false)
	require.NoError(t, err)

	branches := brancher.ListBranches()
	assert.Len(t, branches, 1)
	assert.Equal(t, "main", branches[0].Name)
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

func getProductPrice(ctx context.Context, cfg *config.Config, productName string) (string, error) {
	conn, err := pgx.Connect(ctx, cfg.ConnectionURLForDB(cfg.Database))
	if err != nil {
		return "", err
	}
	defer conn.Close(ctx)

	var price string
	err = conn.QueryRow(ctx, "SELECT price FROM products WHERE name = $1", productName).Scan(&price)
	if err != nil {
		return "", err
	}

	return price, nil
}

func TestCheckoutAutoSave(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := testutil.StartPostgresContainer(ctx)
	require.NoError(t, err)
	defer pg.Stop(ctx)

	testDir := testutil.SetupTestDir(t)
	defer testDir.Cleanup(t)

	cfg := pg.GetConfig()

	err = Initialize(cfg.Database, cfg.Host, cfg.Port, cfg.User, cfg.Password)
	require.NoError(t, err)

	setupSQL := `
		CREATE TABLE items (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		);
		INSERT INTO items (name) VALUES ('original_item');
	`
	err = execSQL(ctx, cfg, setupSQL)
	require.NoError(t, err)

	brancher, err := NewBrancher()
	require.NoError(t, err)

	err = brancher.CreateBranch("main")
	require.NoError(t, err)
	brancher.Metadata.CurrentBranch = "main"
	brancher.Metadata.Save()

	err = brancher.CreateBranch("feature")
	require.NoError(t, err)

	err = brancher.Checkout("feature")
	require.NoError(t, err)

	featureSQL := `
		INSERT INTO items (name) VALUES ('feature_item');
		UPDATE items SET name = 'modified_original' WHERE name = 'original_item';
	`
	err = execSQL(ctx, cfg, featureSQL)
	require.NoError(t, err)

	count, err := countRows(ctx, cfg, "items")
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	exists, err := rowExists(ctx, cfg, "items", "name", "feature_item")
	require.NoError(t, err)
	assert.True(t, exists)

	err = brancher.Checkout("main")
	require.NoError(t, err)

	count, err = countRows(ctx, cfg, "items")
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	exists, err = rowExists(ctx, cfg, "items", "name", "original_item")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = rowExists(ctx, cfg, "items", "name", "feature_item")
	require.NoError(t, err)
	assert.False(t, exists)

	err = brancher.Checkout("feature")
	require.NoError(t, err)

	count, err = countRows(ctx, cfg, "items")
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	exists, err = rowExists(ctx, cfg, "items", "name", "feature_item")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = rowExists(ctx, cfg, "items", "name", "modified_original")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = rowExists(ctx, cfg, "items", "name", "original_item")
	require.NoError(t, err)
	assert.False(t, exists)
}
