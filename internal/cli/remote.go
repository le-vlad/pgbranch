package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/le-vlad/pgbranch/internal/remote"
	"github.com/le-vlad/pgbranch/pkg/config"
	"github.com/spf13/cobra"
)

func newRemoteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remote",
		Short: "Manage remote storage backends",
		Long: `Manage remote storage backends for sharing database snapshots.

Supported remote types:
  - Filesystem: /path/to/dir or file:///path/to/dir
  - S3/MinIO:   s3://bucket/prefix
  - GCS:        gs://bucket/prefix (coming soon)`,
	}

	cmd.AddCommand(
		newRemoteAddCmd(),
		newRemoteRemoveCmd(),
		newRemoteListCmd(),
		newRemoteLsRemoteCmd(),
		newRemoteSetDefaultCmd(),
		newRemoteDeleteBranchCmd(),
	)

	return cmd
}

func newRemoteAddCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add <name> <url>",
		Short: "Add a new remote",
		Long: `Add a new remote storage backend.

Examples:
  # Add a filesystem remote
  pgbranch remote add origin /shared/snapshots
  pgbranch remote add local ~/pgbranch-snapshots

  # Add an S3 remote
  pgbranch remote add origin s3://my-bucket/pgbranch

  # Add a MinIO remote (set AWS_ENDPOINT_URL environment variable)
  AWS_ENDPOINT_URL=http://localhost:9000 pgbranch remote add minio s3://my-bucket/pgbranch`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			url := args[1]

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			remoteCfg, err := remote.ParseURL(name, url)
			if err != nil {
				return fmt.Errorf("invalid remote URL: %w", err)
			}

			configRemote := &config.RemoteConfig{
				Name:    remoteCfg.Name,
				Type:    remoteCfg.Type,
				URL:     remoteCfg.URL,
				Options: remoteCfg.Options,
			}

			if err := cfg.AddRemote(configRemote); err != nil {
				return err
			}

			if err := cfg.Save(); err != nil {
				return fmt.Errorf("failed to save config: %w", err)
			}

			fmt.Printf("Added remote '%s' (%s)\n", name, remoteCfg.Type)
			if cfg.DefaultRemote == name {
				fmt.Printf("Set '%s' as default remote\n", name)
			}

			return nil
		},
	}

	return cmd
}

func newRemoteRemoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove <name>",
		Aliases: []string{"rm"},
		Short:   "Remove a remote",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			if err := cfg.RemoveRemote(name); err != nil {
				return err
			}

			if err := cfg.Save(); err != nil {
				return fmt.Errorf("failed to save config: %w", err)
			}

			fmt.Printf("Removed remote '%s'\n", name)
			return nil
		},
	}

	return cmd
}

func newRemoteListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List configured remotes",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			remotes := cfg.ListRemotes()
			if len(remotes) == 0 {
				fmt.Println("No remotes configured")
				fmt.Println("Use 'pgbranch remote add <name> <url>' to add one")
				return nil
			}

			sort.Slice(remotes, func(i, j int) bool {
				return remotes[i].Name < remotes[j].Name
			})

			for _, r := range remotes {
				defaultMarker := ""
				if r.Name == cfg.DefaultRemote {
					defaultMarker = " (default)"
				}
				fmt.Printf("%s\t%s\t%s%s\n", r.Name, r.Type, r.URL, defaultMarker)
			}

			return nil
		},
	}

	return cmd
}

func newRemoteLsRemoteCmd() *cobra.Command {
	var remoteName string

	cmd := &cobra.Command{
		Use:   "ls-remote",
		Short: "List branches on a remote",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			remoteCfg, err := cfg.GetRemote(remoteName)
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

			branches, err := r.List(context.Background())
			if err != nil {
				return fmt.Errorf("failed to list remote branches: %w", err)
			}

			if len(branches) == 0 {
				fmt.Printf("No branches on remote '%s'\n", remoteCfg.Name)
				return nil
			}

			sort.Slice(branches, func(i, j int) bool {
				return branches[i].Name < branches[j].Name
			})

			for _, b := range branches {
				sizeStr := formatSize(b.Size)
				fmt.Printf("%s\t%s\t%s\n", b.Name, sizeStr, b.ModTime.Format("2006-01-02 15:04"))
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&remoteName, "remote", "r", "", "Remote name (default: use default remote)")

	return cmd
}

func newRemoteSetDefaultCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-default <name>",
		Short: "Set the default remote",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			if err := cfg.SetDefaultRemote(name); err != nil {
				return err
			}

			if err := cfg.Save(); err != nil {
				return fmt.Errorf("failed to save config: %w", err)
			}

			fmt.Printf("Set '%s' as default remote\n", name)
			return nil
		},
	}

	return cmd
}

func formatSize(size int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case size >= GB:
		return fmt.Sprintf("%.1f GB", float64(size)/GB)
	case size >= MB:
		return fmt.Sprintf("%.1f MB", float64(size)/MB)
	case size >= KB:
		return fmt.Sprintf("%.1f KB", float64(size)/KB)
	default:
		return fmt.Sprintf("%d B", size)
	}
}

func newRemoteDeleteBranchCmd() *cobra.Command {
	var remoteName string

	cmd := &cobra.Command{
		Use:   "delete <branch>",
		Short: "Delete a branch from a remote",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			branchName := args[0]

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			remoteCfg, err := cfg.GetRemote(remoteName)
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

			if err := r.Delete(ctx, branchName); err != nil {
				return fmt.Errorf("failed to delete from remote: %w", err)
			}

			fmt.Printf("Deleted '%s' from remote '%s'\n", branchName, remoteCfg.Name)
			return nil
		},
	}

	cmd.Flags().StringVarP(&remoteName, "remote", "r", "", "Remote name (default: use default remote)")

	return cmd
}
