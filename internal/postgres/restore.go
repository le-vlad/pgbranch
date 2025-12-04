package postgres

import (
	"fmt"
	"os/exec"

	"github.com/le-vlad/pgbranch/pkg/config"
)

func (c *Client) Restore(inputPath string) error {
	cmd := exec.Command("pg_restore",
		"-h", c.Config.Host,
		"-p", fmt.Sprintf("%d", c.Config.Port),
		"-U", c.Config.User,
		"-d", c.Config.Database,
		"--no-owner",
		"--no-privileges",
		inputPath,
	)

	if c.Config.Password != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", c.Config.Password))
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		outputStr := string(output)
		if len(outputStr) > 0 {
			return fmt.Errorf("pg_restore completed with warnings/errors: %s", outputStr)
		}
	}

	return nil
}

func (c *Client) RestoreClean(inputPath string) error {
	c.TerminateConnections()

	if err := c.DropDatabase(); err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	if err := c.CreateDatabase(); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	if err := c.Restore(inputPath); err != nil {
		return fmt.Errorf("failed to restore: %w", err)
	}

	return nil
}

func RestoreFromPath(cfg *config.Config, inputPath string) error {
	client := NewClient(cfg)
	return client.RestoreClean(inputPath)
}
