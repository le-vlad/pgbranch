package storage

import (
	"fmt"
	"strings"
)

// SnapshotDBName generates a database name for a snapshot.
// Format: {originalDB}_pgbranch_{branchName}
func SnapshotDBName(originalDB, branchName string) string {
	sanitized := strings.ReplaceAll(branchName, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, "/", "_")
	sanitized = strings.ReplaceAll(sanitized, ".", "_")
	return fmt.Sprintf("%s_pgbranch_%s", originalDB, sanitized)
}
