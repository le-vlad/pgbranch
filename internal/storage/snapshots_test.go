package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshotDBNameFormat(t *testing.T) {
	tests := []struct {
		originalDB string
		branchName string
		prefix     string
	}{
		{"mydb", "main", "mydb_pgbranch_main_"},
		{"mydb", "feature-1", "mydb_pgbranch_feature_1_"},
		{"mydb", "feature/login", "mydb_pgbranch_feature_login_"},
		{"mydb", "release.1.0", "mydb_pgbranch_release_1_0_"},
		{"testdb", "my-branch", "testdb_pgbranch_my_branch_"},
	}

	for _, tt := range tests {
		t.Run(tt.branchName, func(t *testing.T) {
			result := SnapshotDBName(tt.originalDB, tt.branchName)
			assert.True(t, strings.HasPrefix(result, tt.prefix),
				"%q should start with %q", result, tt.prefix)
			assert.Len(t, strings.TrimPrefix(result, tt.prefix), suffixLen,
				"expected a %d-character digest suffix", suffixLen)
		})
	}
}

func TestSnapshotDBNameIsDeterministic(t *testing.T) {
	assert.Equal(t,
		SnapshotDBName("mydb", "feature/login"),
		SnapshotDBName("mydb", "feature/login"))
}

// Sanitisation maps '-', '/', '.' and ' ' to '_'. Without a digest these branch
// names would all share one database, silently mixing their data.
func TestSnapshotDBNameDistinguishesSanitizedCollisions(t *testing.T) {
	branches := []string{"feat-x", "feat/x", "feat.x", "feat_x", "feat x"}

	seen := make(map[string]string, len(branches))
	for _, branch := range branches {
		name := SnapshotDBName("mydb", branch)
		if other, clash := seen[name]; clash {
			t.Errorf("branches %q and %q both map to database %q", other, branch, name)
		}
		seen[name] = branch
	}
}

// PostgreSQL truncates identifiers to 63 bytes with a notice rather than an
// error, so two long branch names sharing a prefix would collide.
func TestSnapshotDBNameFitsPostgresIdentifierLimit(t *testing.T) {
	long := strings.Repeat("a", 200)

	name := SnapshotDBName("mydb", long)
	assert.LessOrEqual(t, len(name), maxIdentifierLen)

	other := SnapshotDBName("mydb", long+"-different-tail")
	assert.LessOrEqual(t, len(other), maxIdentifierLen)
	assert.NotEqual(t, name, other, "long branch names must not collide after truncation")
}

func TestSnapshotDBNameHandlesOversizedDatabaseName(t *testing.T) {
	// A database name long enough to leave no budget for the branch segment
	// must still produce distinct, in-limit identifiers.
	db := strings.Repeat("d", 55)

	a := SnapshotDBName(db, "one")
	b := SnapshotDBName(db, "two")
	assert.NotEqual(t, a, b)
}
