package postgres

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/le-vlad/pgbranch/pkg/config"
)

func (c *Client) Dump(outputPath string) error {
	cmd := exec.Command("pg_dump",
		"-h", c.Config.Host,
		"-p", fmt.Sprintf("%d", c.Config.Port),
		"-U", c.Config.User,
		"-d", c.Config.Database,
		"-Fc",
		"-f", outputPath,
	)

	if c.Config.Password != "" {
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", c.Config.Password))
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_dump failed: %s", string(output))
	}

	return nil
}

func DumpToPath(cfg *config.Config, outputPath string) error {
	client := NewClient(cfg)
	return client.Dump(outputPath)
}
