package cli

import (
	"context"
	"fmt"

	"github.com/le-vlad/pgbranch/internal/archive"
	"github.com/le-vlad/pgbranch/internal/core"
	"github.com/le-vlad/pgbranch/internal/remote"
	"github.com/le-vlad/pgbranch/internal/storage"
	"github.com/spf13/cobra"
)

func newPullCmd() *cobra.Command {
	var (
		remoteName string
		localName  string
		force      bool
	)

	cmd := &cobra.Command{
		Use:   "pull <branch>",
		Short: "Pull a branch from a remote",
		Long: `Pull a branch snapshot from a remote storage backend.

Downloads the snapshot archive from the remote, verifies its integrity,
and creates a local branch from it.

Examples:
  # Pull from default remote
  pgbranch pull main

  # Pull from a specific remote
  pgbranch pull main --remote origin

  # Pull with a different local name
  pgbranch pull main --as main-backup

  # Force overwrite if local branch exists
  pgbranch pull main --force`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			branchName := args[0]
			targetName := branchName
			if localName != "" {
				targetName = localName
			}

			brancher, err := core.NewBrancher()
			if err != nil {
				return err
			}

			if brancher.Metadata.BranchExists(targetName) && !force {
				return fmt.Errorf("branch '%s' already exists locally. Use --force to overwrite or --as to use a different name", targetName)
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

			if !exists {
				return fmt.Errorf("branch '%s' not found on remote '%s'", branchName, remoteCfg.Name)
			}

			fmt.Printf("Pulling '%s' from remote '%s'...\n", branchName, remoteCfg.Name)

			reader, size, err := r.Pull(ctx, branchName)
			if err != nil {
				return fmt.Errorf("failed to pull from remote: %w", err)
			}
			defer reader.Close()

			fmt.Printf("Downloaded %s, verifying...\n", formatSize(size))

			arch, err := archive.ReadFrom(reader)
			if err != nil {
				return fmt.Errorf("failed to read archive: %w", err)
			}

			fmt.Printf("Archive verified (checksum OK)\n")
			fmt.Printf("  Branch: %s\n", arch.Manifest.Branch)
			fmt.Printf("  Created: %s\n", arch.Manifest.CreatedAt.Format("2006-01-02 15:04:05"))
			if arch.Manifest.Description != "" {
				fmt.Printf("  Description: %s\n", arch.Manifest.Description)
			}
			if arch.Manifest.PgDumpVersion != "" {
				fmt.Printf("  pg_dump version: %s\n", arch.Manifest.PgDumpVersion)
			}

			if brancher.Metadata.BranchExists(targetName) && force {
				fmt.Printf("Removing existing local branch '%s'...\n", targetName)
				if err := brancher.DeleteBranch(targetName, true); err != nil {
					return fmt.Errorf("failed to delete existing branch: %w", err)
				}
			}

			snapshotDBName := storage.SnapshotDBName(brancher.Config.Database, targetName)

			fmt.Printf("Restoring to local snapshot...\n")

			if err := arch.Restore(ctx, brancher.Config, snapshotDBName); err != nil {
				return fmt.Errorf("failed to restore snapshot: %w", err)
			}

			brancher.Metadata.AddBranch(targetName, "", snapshotDBName)

			if err := brancher.Metadata.Save(); err != nil {
				brancher.Client.DeleteSnapshot(snapshotDBName)
				return fmt.Errorf("failed to save metadata: %w", err)
			}

			fmt.Printf("Successfully pulled '%s'", branchName)
			if targetName != branchName {
				fmt.Printf(" as '%s'", targetName)
			}
			fmt.Println()

			return nil
		},
	}

	cmd.Flags().StringVarP(&remoteName, "remote", "r", "", "Remote name (default: use default remote)")
	cmd.Flags().StringVar(&localName, "as", "", "Local branch name (default: same as remote branch)")
	cmd.Flags().BoolVarP(&force, "force", "f", false, "Force overwrite if local branch exists")

	return cmd
}
