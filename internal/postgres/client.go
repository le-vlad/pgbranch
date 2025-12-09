package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/le-vlad/pgbranch/pkg/config"
)

type Client struct {
	Config *config.Config
}

func NewClient(cfg *config.Config) *Client {
	return &Client{Config: cfg}
}

func (c *Client) connect(ctx context.Context, dbName string) (*pgx.Conn, error) {
	connStr := c.Config.ConnectionURLForDB(dbName)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database %s: %w", dbName, err)
	}
	return conn, nil
}

func (c *Client) connectAdmin(ctx context.Context) (*pgx.Conn, error) {
	return c.connect(ctx, "postgres")
}

func (c *Client) DatabaseExists() (bool, error) {
	ctx := context.Background()
	conn, err := c.connectAdmin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check database existence: %w", err)
	}
	defer conn.Close(ctx)

	var exists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
		c.Config.Database,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check database existence: %w", err)
	}

	return exists, nil
}

func (c *Client) CreateDatabase() error {
	ctx := context.Background()
	conn, err := c.connectAdmin(ctx)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{c.Config.Database}.Sanitize()))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	return nil
}

func (c *Client) DropDatabase() error {
	ctx := context.Background()
	conn, err := c.connectAdmin(ctx)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", pgx.Identifier{c.Config.Database}.Sanitize()))
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}
	return nil
}

func (c *Client) TerminateConnections() error {
	return c.TerminateConnectionsTo(c.Config.Database)
}

func (c *Client) TestConnection() error {
	ctx := context.Background()
	conn, err := c.connectAdmin(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer conn.Close(ctx)

	err = conn.Ping(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	return nil
}

func (c *Client) CreateDatabaseFromTemplate(templateDB, newDB string) error {
	ctx := context.Background()

	c.TerminateConnectionsTo(templateDB)

	conn, err := c.connectAdmin(ctx)
	if err != nil {
		return fmt.Errorf("failed to create database from template: %w", err)
	}
	defer conn.Close(ctx)

	query := fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s",
		pgx.Identifier{newDB}.Sanitize(),
		pgx.Identifier{templateDB}.Sanitize(),
	)
	_, err = conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create database from template: %w", err)
	}
	return nil
}

func (c *Client) TerminateConnectionsTo(dbName string) error {
	ctx := context.Background()
	conn, err := c.connectAdmin(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close(ctx)

	_, _ = conn.Exec(ctx, `
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = $1 AND pid <> pg_backend_pid()
	`, dbName)

	return nil
}

func (c *Client) DropDatabaseByName(dbName string) error {
	ctx := context.Background()

	c.TerminateConnectionsTo(dbName)

	conn, err := c.connectAdmin(ctx)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", pgx.Identifier{dbName}.Sanitize()))
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}
	return nil
}
