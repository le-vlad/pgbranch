package postgres

import (
	"fmt"

	"github.com/le-vlad/pgbranch/pkg/config"
)

func (c *Client) RestoreFromSnapshot(snapshotDBName string) error {
	c.TerminateConnections()

	if err := c.DropDatabase(); err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	if err := c.CreateDatabaseFromTemplate(snapshotDBName, c.Config.Database); err != nil {
		return fmt.Errorf("failed to create database from snapshot: %w", err)
	}

	return nil
}

func RestoreFromSnapshotDB(cfg *config.Config, snapshotDBName string) error {
	client := NewClient(cfg)
	return client.RestoreFromSnapshot(snapshotDBName)
}

func (c *Client) DeleteSnapshot(snapshotDBName string) error {
	return c.DropDatabaseByName(snapshotDBName)
}

func DeleteSnapshotDB(cfg *config.Config, snapshotDBName string) error {
	client := NewClient(cfg)
	return client.DeleteSnapshot(snapshotDBName)
}
