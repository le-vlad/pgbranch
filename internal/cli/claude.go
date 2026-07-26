package cli

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/le-vlad/pgbranch/internal/core"
	"github.com/le-vlad/pgbranch/internal/gitrepo"
	"github.com/le-vlad/pgbranch/pkg/config"
)

//go:embed assets/claude_skill.md
var claudeSkill string

// hookCommand is the command Claude Code runs at session start. Matching on
// this string lets uninstall find our hook among the user's own.
const hookCommand = "pgbranch claude session-start"

var claudeCmd = &cobra.Command{
	Use:   "claude",
	Short: "Claude Code integration",
	Long: `Integrate pgbranch with Claude Code, so that agent worktrees get their
own database instead of sharing your development one.

Without this, 'claude --worktree' produces a clean checkout with no .env file,
and the application inside it falls back to the default connection string —
which is your main database. An agent running migrations there is editing the
database your own dev server is connected to.

'pgbranch claude install' adds a SessionStart hook telling Claude which database
the worktree owns, plus a skill so it can rediscover that later in a long
session. Provisioning itself is done by the git post-checkout hook, which
Claude's worktree creation already triggers.`,
}

var claudeInstallCmd = &cobra.Command{
	Use:   "install",
	Short: "Install the Claude Code integration",
	RunE:  runClaudeInstall,
}

var claudeUninstallCmd = &cobra.Command{
	Use:   "uninstall",
	Short: "Remove the Claude Code integration",
	RunE:  runClaudeUninstall,
}

// claudeSessionStartCmd is invoked by the SessionStart hook. Its stdout is one
// of the few hook outputs Claude Code injects into the model's context.
var claudeSessionStartCmd = &cobra.Command{
	Use:    "session-start",
	Short:  "Internal: report this worktree's database to Claude",
	Hidden: true,
	RunE:   runClaudeSessionStart,
}

func init() {
	claudeCmd.AddCommand(claudeInstallCmd)
	claudeCmd.AddCommand(claudeUninstallCmd)
	claudeCmd.AddCommand(claudeSessionStartCmd)
}

// claudeDir returns the repository's .claude directory. It lives in the main
// worktree, which is where Claude Code reads project settings from.
func claudeDir() (string, error) {
	ctx, err := gitrepo.Discover()
	if err != nil {
		return "", fmt.Errorf("not a git repository")
	}
	return filepath.Join(ctx.MainRoot, ".claude"), nil
}

func runClaudeInstall(cmd *cobra.Command, args []string) error {
	dir, err := claudeDir()
	if err != nil {
		return err
	}

	settingsPath := filepath.Join(dir, "settings.json")
	settings, err := readSettings(settingsPath)
	if err != nil {
		return err
	}
	if addSessionStartHook(settings) {
		if err := writeJSON(settingsPath, settings); err != nil {
			return err
		}
	}

	skillPath := filepath.Join(dir, "skills", "pgbranch", "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(skillPath), 0755); err != nil {
		return fmt.Errorf("failed to create skill directory: %w", err)
	}
	if err := os.WriteFile(skillPath, []byte(claudeSkill), 0644); err != nil {
		return fmt.Errorf("failed to write skill: %w", err)
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("%s Claude Code integration installed\n", green("✓"))
	fmt.Printf("    %s   SessionStart hook\n", relativeToCwd(settingsPath))
	fmt.Printf("    %s   pgbranch skill\n", relativeToCwd(skillPath))
	fmt.Println()
	fmt.Println("Claude worktrees now get their own database. Check with:")
	fmt.Println("    claude --worktree try")
	fmt.Println()
	fmt.Println("This needs the git hook too, if you have not installed it:")
	fmt.Println("    pgbranch hook install")

	return nil
}

func runClaudeUninstall(cmd *cobra.Command, args []string) error {
	dir, err := claudeDir()
	if err != nil {
		return err
	}

	settingsPath := filepath.Join(dir, "settings.json")
	settings, err := readSettings(settingsPath)
	if err != nil {
		return err
	}
	if removeSessionStartHook(settings) {
		if err := writeJSON(settingsPath, settings); err != nil {
			return err
		}
	}

	skillDir := filepath.Join(dir, "skills", "pgbranch")
	if err := os.RemoveAll(skillDir); err != nil {
		return fmt.Errorf("failed to remove skill: %w", err)
	}

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("%s Claude Code integration removed\n", green("✓"))
	return nil
}

// runClaudeSessionStart emits the JSON that Claude Code understands:
// systemMessage is shown to the user, hookSpecificOutput.additionalContext is
// injected into the model's context.
//
// It must always exit zero. A hook failure at session start is a broken editor.
func runClaudeSessionStart(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	// The hook payload arrives on stdin. We do not need it, but leaving it
	// unread can hand Claude Code a broken pipe.
	if cmd.InOrStdin() != nil {
		_, _ = io.Copy(io.Discard, cmd.InOrStdin())
	}

	if !config.IsInitialized() {
		return nil
	}
	brancher, err := core.NewBrancher()
	if err != nil {
		return nil
	}

	state, err := brancher.EnsureWorktreeDatabase("")
	if err != nil {
		return nil
	}
	if !state.IsLinked {
		// The main worktree already uses the configured database; saying so
		// every session would be noise.
		return nil
	}

	envPath, err := brancher.WriteEnvFile()
	if err != nil {
		return nil
	}

	url := brancher.Config.ConnectionURLForDB(state.Database)
	context := fmt.Sprintf(
		"pgbranch: this git worktree has its own PostgreSQL database, isolated from the developer's main database.\n"+
			"  DATABASE_URL=%s\n"+
			"  This worktree's database: %s (git branch %q)\n"+
			"  The developer's main database %q is NOT connected to this worktree and must not be modified.\n"+
			"  Point the application at the URL above (it is also written to %s) before running migrations,\n"+
			"  seeds, or tests. Run 'pgbranch env' at any time to recover this connection string.",
		url, state.Database, state.Branch, state.MainDB, envPath)

	verb := "is using"
	if state.Created {
		verb = "created"
	}
	message := fmt.Sprintf("pgbranch %s database %q for this worktree. Your main database %q is untouched.",
		verb, state.Database, state.MainDB)

	out, err := json.Marshal(map[string]any{
		"systemMessage": message,
		"hookSpecificOutput": map[string]string{
			"hookEventName":     "SessionStart",
			"additionalContext": context,
		},
	})
	if err != nil {
		return nil
	}
	fmt.Println(string(out))
	return nil
}

func readSettings(path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]any{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", path, err)
	}
	settings := map[string]any{}
	if err := json.Unmarshal(data, &settings); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}
	return settings, nil
}

func writeJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create %s: %w", filepath.Dir(path), err)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

// addSessionStartHook merges our hook into whatever the user already has,
// preserving every other key and every other hook. Returns false when the hook
// is already present.
func addSessionStartHook(settings map[string]any) bool {
	hooks, _ := settings["hooks"].(map[string]any)
	if hooks == nil {
		hooks = map[string]any{}
	}
	entries, _ := hooks["SessionStart"].([]any)

	for _, entry := range entries {
		if containsHookCommand(entry) {
			return false
		}
	}

	hooks["SessionStart"] = append(entries, map[string]any{
		"hooks": []any{
			map[string]any{"type": "command", "command": hookCommand},
		},
	})
	settings["hooks"] = hooks
	return true
}

func removeSessionStartHook(settings map[string]any) bool {
	hooks, _ := settings["hooks"].(map[string]any)
	if hooks == nil {
		return false
	}
	entries, _ := hooks["SessionStart"].([]any)

	kept := make([]any, 0, len(entries))
	for _, entry := range entries {
		if !containsHookCommand(entry) {
			kept = append(kept, entry)
		}
	}
	if len(kept) == len(entries) {
		return false
	}

	if len(kept) == 0 {
		delete(hooks, "SessionStart")
	} else {
		hooks["SessionStart"] = kept
	}
	if len(hooks) == 0 {
		delete(settings, "hooks")
	} else {
		settings["hooks"] = hooks
	}
	return true
}

// containsHookCommand reports whether a SessionStart matcher entry runs our command.
func containsHookCommand(entry any) bool {
	matcher, ok := entry.(map[string]any)
	if !ok {
		return false
	}
	inner, _ := matcher["hooks"].([]any)
	for _, h := range inner {
		hook, ok := h.(map[string]any)
		if !ok {
			continue
		}
		if cmd, _ := hook["command"].(string); cmd == hookCommand {
			return true
		}
	}
	return false
}
