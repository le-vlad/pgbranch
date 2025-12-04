package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/le-vlad/pgbranch/pkg/config"
)

const (
	TestDBName   = "pgbranch_test"
	TestUser     = "postgres"
	TestPassword = "postgres"
)

type TestPostgres struct {
	Container testcontainers.Container
	Host      string
	Port      int
	Database  string
	User      string
	Password  string
}

func StartPostgresContainer(ctx context.Context) (*TestPostgres, error) {
	pgContainer, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase(TestDBName),
		postgres.WithUsername(TestUser),
		postgres.WithPassword(TestPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres container: %w", err)
	}

	host, err := pgContainer.Host(ctx)
	if err != nil {
		pgContainer.Terminate(ctx)
		return nil, fmt.Errorf("failed to get container host: %w", err)
	}

	mappedPort, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		pgContainer.Terminate(ctx)
		return nil, fmt.Errorf("failed to get mapped port: %w", err)
	}

	return &TestPostgres{
		Container: pgContainer,
		Host:      host,
		Port:      mappedPort.Int(),
		Database:  TestDBName,
		User:      TestUser,
		Password:  TestPassword,
	}, nil
}

func (tp *TestPostgres) Stop(ctx context.Context) error {
	if tp.Container != nil {
		return tp.Container.Terminate(ctx)
	}
	return nil
}

func (tp *TestPostgres) GetConfig() *config.Config {
	return &config.Config{
		Database: tp.Database,
		Host:     tp.Host,
		Port:     tp.Port,
		User:     tp.User,
		Password: tp.Password,
	}
}

type TestDir struct {
	Path     string
	Original string
}

func SetupTestDir(t *testing.T) *TestDir {
	t.Helper()

	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}

	tmpDir, err := os.MkdirTemp("", "pgbranch-test-*")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}

	if err := os.Chdir(tmpDir); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to change to temp directory: %v", err)
	}

	return &TestDir{
		Path:     tmpDir,
		Original: originalDir,
	}
}

func (td *TestDir) Cleanup(t *testing.T) {
	t.Helper()

	if err := os.Chdir(td.Original); err != nil {
		t.Errorf("failed to restore original directory: %v", err)
	}

	if err := os.RemoveAll(td.Path); err != nil {
		t.Errorf("failed to remove temp directory: %v", err)
	}
}

func (td *TestDir) CreatePgbranchDir(t *testing.T) string {
	t.Helper()

	pgbranchDir := filepath.Join(td.Path, ".pgbranch")
	snapshotsDir := filepath.Join(pgbranchDir, "snapshots")

	if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
		t.Fatalf("failed to create pgbranch directories: %v", err)
	}

	return pgbranchDir
}

func (td *TestDir) WriteConfig(t *testing.T, cfg *config.Config) {
	t.Helper()

	td.CreatePgbranchDir(t)

	if err := cfg.Save(); err != nil {
		t.Fatalf("failed to save config: %v", err)
	}
}

type IntegrationTest struct {
	T        *testing.T
	Ctx      context.Context
	Postgres *TestPostgres
	TestDir  *TestDir
}

func SetupIntegrationTest(t *testing.T) *IntegrationTest {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pg, err := StartPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	testDir := SetupTestDir(t)

	return &IntegrationTest{
		T:        t,
		Ctx:      ctx,
		Postgres: pg,
		TestDir:  testDir,
	}
}

func (it *IntegrationTest) Cleanup() {
	it.TestDir.Cleanup(it.T)
	if err := it.Postgres.Stop(it.Ctx); err != nil {
		it.T.Errorf("failed to stop postgres container: %v", err)
	}
}

func (it *IntegrationTest) GetConfig() *config.Config {
	return it.Postgres.GetConfig()
}

func (it *IntegrationTest) InitializePgbranch() {
	it.T.Helper()
	it.TestDir.WriteConfig(it.T, it.GetConfig())
}
