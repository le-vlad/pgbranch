package postgres

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/le-vlad/pgbranch/pkg/config"
)

// Client wraps PostgreSQL operations
type Client struct {
	Config *config.Config
}

// NewClient creates a new PostgreSQL client
func NewClient(cfg *config.Config) *Client {
	return &Client{Config: cfg}
}

// buildEnv returns environment variables for pg commands
func (c *Client) buildEnv() []string {
	env := []string{
		fmt.Sprintf("PGHOST=%s", c.Config.Host),
		fmt.Sprintf("PGPORT=%d", c.Config.Port),
		fmt.Sprintf("PGUSER=%s", c.Config.User),
		fmt.Sprintf("PGDATABASE=%s", c.Config.Database),
	}
	if c.Config.Password != "" {
		env = append(env, fmt.Sprintf("PGPASSWORD=%s", c.Config.Password))
	}
	return env
}

// DatabaseExists checks if the database exists
func (c *Client) DatabaseExists() (bool, error) {
	cmd := exec.Command("psql",
		"-h", c.Config.Host,
		"-p", fmt.Sprintf("%d", c.Config.Port),
		"-U", c.Config.User,
		"-d", "postgres",
		"-tAc", fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname='%s'", c.Config.Database),
	)

	if c.Config.Password != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", c.Config.Password))
	}

	output, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("failed to check database existence: %w", err)
	}

	return strings.TrimSpace(string(output)) == "1", nil
}

// CreateDatabase creates a new database
func (c *Client) CreateDatabase() error {
	cmd := exec.Command("createdb",
		"-h", c.Config.Host,
		"-p", fmt.Sprintf("%d", c.Config.Port),
		"-U", c.Config.User,
		c.Config.Database,
	)

	if c.Config.Password != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", c.Config.Password))
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to create database: %s", string(output))
	}
	return nil
}

// DropDatabase drops the database
func (c *Client) DropDatabase() error {
	cmd := exec.Command("dropdb",
		"-h", c.Config.Host,
		"-p", fmt.Sprintf("%d", c.Config.Port),
		"-U", c.Config.User,
		"--if-exists",
		c.Config.Database,
	)

	if c.Config.Password != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", c.Config.Password))
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to drop database: %s", string(output))
	}
	return nil
}

// TerminateConnections terminates all connections to the database
func (c *Client) TerminateConnections() error {
	query := fmt.Sprintf(`
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = '%s' AND pid <> pg_backend_pid()
	`, c.Config.Database)

	cmd := exec.Command("psql",
		"-h", c.Config.Host,
		"-p", fmt.Sprintf("%d", c.Config.Port),
		"-U", c.Config.User,
		"-d", "postgres",
		"-c", query,
	)

	if c.Config.Password != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", c.Config.Password))
	}

	_, err := cmd.CombinedOutput()
	if err != nil {
		// Not critical if this fails
		return nil
	}
	return nil
}

// TestConnection tests the connection to PostgreSQL
func (c *Client) TestConnection() error {
	cmd := exec.Command("psql",
		"-h", c.Config.Host,
		"-p", fmt.Sprintf("%d", c.Config.Port),
		"-U", c.Config.User,
		"-d", "postgres",
		"-c", "SELECT 1",
	)

	if c.Config.Password != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", c.Config.Password))
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %s", string(output))
	}
	return nil
}
