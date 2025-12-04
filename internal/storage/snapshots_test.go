package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshotDBName(t *testing.T) {
	tests := []struct {
		originalDB string
		branchName string
		expected   string
	}{
		{"mydb", "main", "mydb_pgbranch_main"},
		{"mydb", "feature-1", "mydb_pgbranch_feature_1"},
		{"mydb", "feature/login", "mydb_pgbranch_feature_login"},
		{"mydb", "release.1.0", "mydb_pgbranch_release_1_0"},
		{"testdb", "my-branch", "testdb_pgbranch_my_branch"},
	}

	for _, tt := range tests {
		t.Run(tt.branchName, func(t *testing.T) {
			result := SnapshotDBName(tt.originalDB, tt.branchName)
			assert.Equal(t, tt.expected, result)
		})
	}
}
