package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/le-vlad/pgbranch/internal/core"
	"github.com/le-vlad/pgbranch/internal/storage"
)

var logCmd = &cobra.Command{
	Use:   "log",
	Short: "Show branch history",
	Long: `Show all branches with their creation time and parent branch.

Example:
  pgbranch log`,
	RunE: runLog,
}

func runLog(cmd *cobra.Command, args []string) error {
	brancher, err := core.NewBrancher()
	if err != nil {
		return err
	}

	branches := brancher.ListBranches()

	if len(branches) == 0 {
		fmt.Println("No branches yet.")
		return nil
	}

	green := color.New(color.FgGreen).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()
	dim := color.New(color.Faint).SprintFunc()

	for _, info := range branches {
		var prefix string
		var name string
		if info.IsCurrent {
			prefix = "* "
			name = green(info.Name)
		} else {
			prefix = "  "
			name = info.Name
		}

		fmt.Printf("%s%s\n", prefix, name)

		fmt.Printf("    Created: %s\n", dim(info.Branch.CreatedAt.Format("2006-01-02 15:04:05")))

		if info.Branch.Parent != "" {
			fmt.Printf("    Parent:  %s\n", yellow(info.Branch.Parent))
		}

		// Snapshot size
		size, err := storage.GetSnapshotSize(info.Branch.Snapshot)
		if err == nil {
			fmt.Printf("    Size:    %s\n", dim(formatBytes(size)))
		}

		fmt.Println()
	}

	return nil
}

func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
