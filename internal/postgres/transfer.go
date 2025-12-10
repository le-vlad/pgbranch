package postgres

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/le-vlad/pgbranch/pkg/config"
)

// DumpOptions configures pg_dump behavior
type DumpOptions struct {
	SchemaOnly    bool
	DataOnly      bool
	ExcludeTables []string
}

// DumpDatabase creates a pg_dump of the specified database and writes to the provided writer.
// Uses custom format (-Fc) which is compressed and supports parallel restore.
func (c *Client) DumpDatabase(ctx context.Context, dbName string, w io.Writer, opts *DumpOptions) error {
	args := c.buildDumpArgs(dbName, opts)

	cmd := exec.CommandContext(ctx, "pg_dump", args...)
	cmd.Stdout = w
	cmd.Env = c.buildEnv()

	var stderr strings.Builder
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_dump failed: %w\nstderr: %s", err, stderr.String())
	}

	return nil
}

func (c *Client) buildDumpArgs(dbName string, opts *DumpOptions) []string {
	args := []string{
		"-h", c.Config.Host,
		"-p", fmt.Sprintf("%d", c.Config.Port),
		"-U", c.Config.User,
		"-Fc",
		"--no-password",
		dbName,
	}

	if opts != nil {
		if opts.SchemaOnly {
			args = append(args, "--schema-only")
		}
		if opts.DataOnly {
			args = append(args, "--data-only")
		}
		for _, table := range opts.ExcludeTables {
			args = append(args, "--exclude-table", table)
		}
	}

	return args
}

// RestoreDatabase restores a pg_dump to the specified database from the provided reader.
// The database must already exist and be empty.
func (c *Client) RestoreDatabase(ctx context.Context, dbName string, r io.Reader) error {
	args := c.buildRestoreArgs(dbName)

	cmd := exec.CommandContext(ctx, "pg_restore", args...)
	cmd.Stdin = r
	cmd.Env = c.buildEnv()

	var stderr strings.Builder
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		stderrStr := stderr.String()
		// pg_restore returns non-zero even for warnings
		// Only fail on critical errors, not on SET parameter issues or warnings
		if isCriticalRestoreError(stderrStr) {
			return fmt.Errorf("pg_restore failed: %w\nstderr: %s", err, stderrStr)
		}
	}

	return nil
}

// isCriticalRestoreError checks if the pg_restore stderr indicates a critical failure
// vs recoverable issues like version-specific SET parameters
func isCriticalRestoreError(stderr string) bool {
	// If no ERROR at all, it's not critical
	if !strings.Contains(stderr, "ERROR") {
		return false
	}

	// These are non-critical errors that can be safely ignored:
	// - SET parameter errors (version compatibility issues)
	// - "errors ignored on restore" indicates pg_restore continued successfully
	nonCriticalPatterns := []string{
		"unrecognized configuration parameter",
		"errors ignored on restore",
	}

	for _, pattern := range nonCriticalPatterns {
		if strings.Contains(stderr, pattern) {
			// Check if there are other ERRORs besides the non-critical ones
			lines := strings.Split(stderr, "\n")
			criticalErrorCount := 0
			for _, line := range lines {
				if strings.Contains(line, "ERROR") {
					isCritical := true
					for _, np := range nonCriticalPatterns {
						if strings.Contains(line, np) {
							isCritical = false
							break
						}
					}
					// Also check the next line for context
					if isCritical && !strings.Contains(line, "SET ") {
						criticalErrorCount++
					}
				}
			}
			if criticalErrorCount == 0 {
				return false
			}
		}
	}

	return true
}

func (c *Client) buildRestoreArgs(dbName string) []string {
	return []string{
		"-h", c.Config.Host,
		"-p", fmt.Sprintf("%d", c.Config.Port),
		"-U", c.Config.User,
		"-d", dbName,
		"--no-password",
		"--no-owner",
		"--no-privileges",
	}
}

// buildEnv creates environment variables for pg_dump/pg_restore commands
// It inherits the current environment and adds PGPASSWORD if configured
func (c *Client) buildEnv() []string {
	env := os.Environ()
	if c.Config.Password != "" {
		env = append(env, fmt.Sprintf("PGPASSWORD=%s", c.Config.Password))
	}
	return env
}

func (c *Client) DumpSnapshotToWriter(ctx context.Context, snapshotDBName string, w io.Writer) error {
	return c.DumpDatabase(ctx, snapshotDBName, w, nil)
}

func (c *Client) RestoreSnapshotFromReader(ctx context.Context, snapshotDBName string, r io.Reader) error {
	if err := c.CreateEmptyDatabase(snapshotDBName); err != nil {
		return fmt.Errorf("failed to create database for restore: %w", err)
	}

	if err := c.RestoreDatabase(ctx, snapshotDBName, r); err != nil {
		c.DropDatabaseByName(snapshotDBName)
		return fmt.Errorf("failed to restore database: %w", err)
	}

	return nil
}

func (c *Client) CreateEmptyDatabase(dbName string) error {
	ctx := context.Background()
	conn, err := c.connectAdmin(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)

	query := fmt.Sprintf("CREATE DATABASE %s", sanitizeIdentifier(dbName))
	_, err = conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

func sanitizeIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

func GetPgDumpVersion() (string, error) {
	cmd := exec.Command("pg_dump", "--version")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get pg_dump version: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

func GetPgRestoreVersion() (string, error) {
	cmd := exec.Command("pg_restore", "--version")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get pg_restore version: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

func DumpDatabaseToWriter(cfg *config.Config, dbName string, w io.Writer) error {
	client := NewClient(cfg)
	return client.DumpSnapshotToWriter(context.Background(), dbName, w)
}

func RestoreDatabaseFromReader(cfg *config.Config, dbName string, r io.Reader) error {
	client := NewClient(cfg)
	return client.RestoreSnapshotFromReader(context.Background(), dbName, r)
}
