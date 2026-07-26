package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// maxIdentifierLen is PostgreSQL's NAMEDATALEN-1. Longer identifiers are
// truncated with a NOTICE rather than rejected, so two branches sharing a
// 63-byte prefix would silently share one database.
const maxIdentifierLen = 63

// suffixLen is the length of the hex digest appended to every snapshot name.
const suffixLen = 6

var sanitizer = strings.NewReplacer("-", "_", "/", "_", ".", "_", " ", "_")

// SnapshotDBName generates the database name backing a branch.
//
// Format: {originalDB}_pgbranch_{sanitized}_{digest}
//
// The digest disambiguates names that sanitisation would otherwise merge:
// "feat-x", "feat/x", "feat.x" and "feat_x" all sanitise to "feat_x", and long
// branch names collide once PostgreSQL truncates them to 63 bytes. Either case
// points two branches at one database, which loses data.
//
// Existing branches keep their names: the database backing a branch is recorded
// in metadata at creation time and read back from there, never recomputed.
func SnapshotDBName(originalDB, branchName string) string {
	digest := sha256.Sum256([]byte(branchName))
	suffix := hex.EncodeToString(digest[:])[:suffixLen]

	prefix := fmt.Sprintf("%s_pgbranch_", originalDB)
	sanitized := sanitizer.Replace(branchName)

	// Reserve room for the prefix, the digest, and the underscore joining them.
	budget := maxIdentifierLen - len(prefix) - suffixLen - 1
	if budget < 0 {
		budget = 0
	}
	if len(sanitized) > budget {
		sanitized = sanitized[:budget]
	}

	return prefix + sanitized + "_" + suffix
}
