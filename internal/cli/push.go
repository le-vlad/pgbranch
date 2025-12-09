package cli

import (
	"bytes"
	"context"
	"fmt"

	"github.com/le-vlad/pgbranch/internal/archive"
	"github.com/le-vlad/pgbranch/internal/core"
	"github.com/le-vlad/pgbranch/internal/remote"
	"github.com/spf13/cobra"
)

func newPushCmd() *cobra.Command {
	var (
		remoteName  string
		force       bool
		description string
	)

	cmd := &cobra.Command{
		Use:   "push <branch>",
		Short: "Push a branch to a remote",
		Long: `Push a local branch snapshot to a remote storage backend.

The branch must exist locally. The snapshot will be exported as a portable
archive and uploaded to the configured remote.

Examples:
  # Push to default remote
  pgbranch push main

  # Push to a specific remote
  pgbranch push main --remote origin

  # Force overwrite if branch exists on remote
  pgbranch push main --force

  # Add a description
  pgbranch push main --description "Initial schema with seed data"`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			branchName := args[0]

			brancher, err := core.NewBrancher()
			if err != nil {
				return err
			}

			branch, ok := brancher.Metadata.GetBranch(branchName)
			if !ok {
				return fmt.Errorf("branch '%s' does not exist locally", branchName)
			}

			remoteCfg, err := brancher.Config.GetRemote(remoteName)
			if err != nil {
				return err
			}

			remoteConfig := &remote.Config{
				Name:    remoteCfg.Name,
				Type:    remoteCfg.Type,
				URL:     remoteCfg.URL,
				Options: remoteCfg.Options,
			}

			r, err := remote.New(remoteConfig)
			if err != nil {
				return fmt.Errorf("failed to create remote: %w", err)
			}

			ctx := context.Background()

			exists, err := r.Exists(ctx, branchName)
			if err != nil {
				return fmt.Errorf("failed to check remote: %w", err)
			}

			if exists && !force {
				return fmt.Errorf("branch '%s' already exists on remote '%s'. Use --force to overwrite", branchName, remoteCfg.Name)
			}

			fmt.Printf("Creating archive for branch '%s'...\n", branchName)

			opts := &archive.CreateOptions{
				Description: description,
			}

			arch, err := archive.Create(ctx, brancher.Config, branchName, branch.Snapshot, opts)
			if err != nil {
				return fmt.Errorf("failed to create archive: %w", err)
			}

			fmt.Printf("Archive size: %s\n", formatSize(arch.Size()))

			var buf bytes.Buffer
			_, err = arch.WriteTo(&buf)
			if err != nil {
				return fmt.Errorf("failed to write archive: %w", err)
			}

			fmt.Printf("Pushing to remote '%s'...\n", remoteCfg.Name)

			err = r.Push(ctx, branchName, &buf, int64(buf.Len()))
			if err != nil {
				return fmt.Errorf("failed to push to remote: %w", err)
			}

			fmt.Printf("Successfully pushed '%s' to '%s'\n", branchName, remoteCfg.Name)

			return nil
		},
	}

	cmd.Flags().StringVarP(&remoteName, "remote", "r", "", "Remote name (default: use default remote)")
	cmd.Flags().BoolVarP(&force, "force", "f", false, "Force overwrite if branch exists on remote")
	cmd.Flags().StringVarP(&description, "description", "d", "", "Description for this snapshot")

	return cmd
}
