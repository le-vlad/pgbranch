package postgres

import (
	"github.com/le-vlad/pgbranch/pkg/config"
)

func (c *Client) CreateSnapshot(snapshotDBName string) error {
	return c.CreateDatabaseFromTemplate(c.Config.Database, snapshotDBName)
}

func CreateSnapshotDB(cfg *config.Config, snapshotDBName string) error {
	client := NewClient(cfg)
	return client.CreateSnapshot(snapshotDBName)
}
