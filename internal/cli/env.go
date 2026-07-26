package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/le-vlad/pgbranch/internal/core"
)

var (
	envJSON    bool
	envURLOnly bool
	envWrite   bool
)

var envCmd = &cobra.Command{
	Use:   "env",
	Short: "Print the database connection for this worktree",
	Long: `Print the database this worktree should connect to.

In the main worktree this is the database from your config, unchanged. In a
linked worktree it is that worktree's own database, so several branches can be
live at once without touching your main development database.

Environment variables do not survive between shell invocations, so pgbranch
prints rather than exports. Use it directly, or write it to a file:

  eval $(pgbranch env)              # this shell
  pgbranch env --write              # writes .pgbranch/env
  pgbranch env --url                # just the URL, for scripts
  pgbranch env --json               # machine readable

With direnv, add this to .envrc:

  dotenv_if_exists .pgbranch/env`,
	RunE: runEnv,
}

func init() {
	envCmd.Flags().BoolVar(&envJSON, "json", false, "Print as JSON")
	envCmd.Flags().BoolVar(&envURLOnly, "url", false, "Print only the connection URL")
	envCmd.Flags().BoolVar(&envWrite, "write", false, "Write .pgbranch/env for this worktree")
}

func runEnv(cmd *cobra.Command, args []string) error {
	brancher, err := core.NewBrancher()
	if err != nil {
		return err
	}

	dbName, err := brancher.WorkingDB()
	if err != nil {
		return err
	}
	url := brancher.Config.ConnectionURLForDB(dbName)

	if envWrite {
		path, err := brancher.WriteEnvFile()
		if err != nil {
			return err
		}
		fmt.Printf("Wrote %s\n", relativeToCwd(path))
		return nil
	}

	switch {
	case envURLOnly:
		fmt.Println(url)
	case envJSON:
		state, err := brancher.Inspect()
		if err != nil {
			return err
		}
		out, err := json.MarshalIndent(map[string]any{
			"database":        dbName,
			"database_url":    url,
			"main_database":   brancher.Config.Database,
			"linked_worktree": state.IsLinked,
			"branch":          state.Branch,
			"worktree":        state.WorktreeAt,
		}, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	default:
		fmt.Printf("export DATABASE_URL=%q\n", url)
		fmt.Printf("export PGDATABASE=%q\n", dbName)
	}

	return nil
}
