package cli

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parseSettings(t *testing.T, raw string) map[string]any {
	t.Helper()
	settings := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(raw), &settings))
	return settings
}

func TestAddSessionStartHookToEmptySettings(t *testing.T) {
	settings := map[string]any{}

	require.True(t, addSessionStartHook(settings))

	hooks := settings["hooks"].(map[string]any)
	entries := hooks["SessionStart"].([]any)
	require.Len(t, entries, 1)
	assert.True(t, containsHookCommand(entries[0]))
}

func TestAddSessionStartHookIsIdempotent(t *testing.T) {
	settings := map[string]any{}

	require.True(t, addSessionStartHook(settings))
	assert.False(t, addSessionStartHook(settings), "second install should be a no-op")

	hooks := settings["hooks"].(map[string]any)
	assert.Len(t, hooks["SessionStart"].([]any), 1)
}

// Installing must not disturb settings the user already has, including their
// own SessionStart hooks and unrelated top-level keys.
func TestAddSessionStartHookPreservesExistingSettings(t *testing.T) {
	settings := parseSettings(t, `{
	  "permissions": {"allow": ["Bash(go build:*)"]},
	  "hooks": {
	    "SessionStart": [{"hooks": [{"type": "command", "command": "echo mine"}]}],
	    "PreToolUse": [{"matcher": "Bash", "hooks": [{"type": "command", "command": "guard"}]}]
	  }
	}`)

	require.True(t, addSessionStartHook(settings))

	assert.Contains(t, settings, "permissions", "unrelated keys must survive")

	hooks := settings["hooks"].(map[string]any)
	assert.Contains(t, hooks, "PreToolUse", "unrelated hook events must survive")

	entries := hooks["SessionStart"].([]any)
	require.Len(t, entries, 2, "user's own SessionStart hook must survive")
	assert.False(t, containsHookCommand(entries[0]))
	assert.True(t, containsHookCommand(entries[1]))
}

func TestRemoveSessionStartHookLeavesUserHooks(t *testing.T) {
	settings := parseSettings(t, `{
	  "hooks": {
	    "SessionStart": [{"hooks": [{"type": "command", "command": "echo mine"}]}]
	  }
	}`)
	require.True(t, addSessionStartHook(settings))

	require.True(t, removeSessionStartHook(settings))

	hooks := settings["hooks"].(map[string]any)
	entries := hooks["SessionStart"].([]any)
	require.Len(t, entries, 1)
	assert.False(t, containsHookCommand(entries[0]), "only pgbranch's hook should be removed")
}

// Uninstalling from settings we fully own should leave no empty scaffolding.
func TestRemoveSessionStartHookCleansUpEmptyContainers(t *testing.T) {
	settings := map[string]any{}
	require.True(t, addSessionStartHook(settings))

	require.True(t, removeSessionStartHook(settings))

	assert.NotContains(t, settings, "hooks")
}

func TestRemoveSessionStartHookOnUntouchedSettings(t *testing.T) {
	settings := parseSettings(t, `{"permissions": {"allow": []}}`)

	assert.False(t, removeSessionStartHook(settings))
	assert.Contains(t, settings, "permissions")
}

func TestRemoveSessionStartHookPreservesOtherEvents(t *testing.T) {
	settings := parseSettings(t, `{
	  "hooks": {"PreToolUse": [{"hooks": [{"type": "command", "command": "guard"}]}]}
	}`)
	require.True(t, addSessionStartHook(settings))

	require.True(t, removeSessionStartHook(settings))

	hooks := settings["hooks"].(map[string]any)
	assert.Contains(t, hooks, "PreToolUse")
	assert.NotContains(t, hooks, "SessionStart")
}

// The installed hook must not override worktree creation. WorktreeCreate is an
// override hook: Claude Code delegates creation to it and fails when it does
// not print a path. pgbranch provisions from the git post-checkout hook instead.
func TestInstallerNeverRegistersWorktreeCreate(t *testing.T) {
	settings := map[string]any{}
	require.True(t, addSessionStartHook(settings))

	hooks := settings["hooks"].(map[string]any)
	assert.NotContains(t, hooks, "WorktreeCreate")
	assert.NotContains(t, hooks, "WorktreeRemove")
}
