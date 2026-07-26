# pgbranch

[![Build Status](https://github.com/le-vlad/pgbranch/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/le-vlad/pgbranch/actions/workflows/go.yml)
[![Latest Release](https://img.shields.io/github/v/release/le-vlad/pgbranch)](https://github.com/le-vlad/pgbranch/releases/latest)
[![Go Report Card](https://goreportcard.com/badge/github.com/le-vlad/pgbranch)](https://goreportcard.com/report/github.com/le-vlad/pgbranch)
[![Go Reference](https://pkg.go.dev/badge/github.com/le-vlad/pgbranch.svg)](https://pkg.go.dev/github.com/le-vlad/pgbranch)
[![codecov](https://codecov.io/gh/le-vlad/pgbranch/branch/main/graph/badge.svg)](https://codecov.io/gh/le-vlad/pgbranch)

<p align="center">
  <img src="img/thumb.png" alt="pgbranch" width="400">
</p>

Git branching for your PostgreSQL database.

## Table of Contents

- [The Problem](#the-problem)
- [The Solution](#the-solution)
- [How It Works](#how-it-works)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Commands](#commands)
- [Schema Diff](#schema-diff)
- [Schema Merge](#schema-merge) *(Beta)*
- [Continuous Migration](#continuous-migration)
- [Automatic Branch Switching](#automatic-branch-switching)
- [Git Worktrees](#git-worktrees)
- [Claude Code](#claude-code)
- [Remotes](#remotes)
- [Caveats](#caveats)

## The Problem

You're working on a feature branch. You run migrations. Life is good.

Then you switch back to `main`.

Your database is now in a broken state. The migrations from your feature branch are still applied. Your schema doesn't match your code. Nothing works.

Your options:
1. Drop the database and re-seed (slow, painful)
2. Manually roll back migrations (error-prone, annoying)
3. Maintain multiple databases and remember to switch connection strings (who has time for that)

None of these are good.

## The Solution

```bash
pgbranch init -d myapp_dev
pgbranch branch main           # snapshot your clean main state
pgbranch checkout main         # switch to the main branch

pgbranch branch feature-x      # create a new branch (snapshots current state)
pgbranch checkout feature-x    # switch to it
# ... run migrations, break things ...

pgbranch checkout main         # instantly back to clean state
```

That's it. Your database now has branches. Just like git.

## How It Works

PostgreSQL has a feature called template databases. When you create a database from a template, it does a file-level copy. It's fast.

pgbranch uses this to create instant snapshots of your database. When you "checkout" a branch, it drops your working database and recreates it from the snapshot.

No pg_dump. No restore. No waiting.

## Installation

```bash
go install github.com/le-vlad/pgbranch/cmd/pgbranch@latest
```

## Quick Start

```bash
# Initialize for your dev database
pgbranch init -d myapp_dev

# Create a branch from current state and switch to it
pgbranch branch main
pgbranch checkout main

# Create a new feature branch and switch to it
pgbranch branch feature-auth
pgbranch checkout feature-auth

# Do some work, run migrations, whatever

# Go back to main
pgbranch checkout main

# Your database is now exactly as it was before
```

## Commands

```
pgbranch init -d <database>    Initialize pgbranch
pgbranch branch                List all branches
pgbranch branch <name>         Create a branch from current state
pgbranch checkout <name>       Switch to a branch
pgbranch delete <name>         Delete a branch
pgbranch status                Show current branch and info
pgbranch log                   Show all branches with details
pgbranch env                   Print this worktree's database connection
pgbranch prune                 Reclaim stale and departed-worktree databases
pgbranch hook install          Install git hook for auto-switching
pgbranch hook uninstall        Remove the git hook
pgbranch claude install        Install the Claude Code integration
pgbranch claude uninstall      Remove the Claude Code integration
pgbranch diff <branch1> [branch2]  Compare schemas between branches
pgbranch merge <source> <target>   Merge schema changes (Beta)
pgbranch migrate -c <config.yaml>  Migrate database via logical replication
```

### Init Options

```
-d, --database   Database name (required)
-H, --host       PostgreSQL host (default: localhost)
-p, --port       PostgreSQL port (default: 5432)
-U, --user       PostgreSQL user (default: postgres)
-W, --password   PostgreSQL password
```

## Schema Diff

Compare the schema between two database branches to see what changed.

```bash
# Compare two branches
pgbranch diff main feature-auth

# Compare a branch against current working database
pgbranch diff main

# Show summary statistics only
pgbranch diff main feature-auth --stat

# Show SQL statements needed to migrate
pgbranch diff main feature-auth --sql
```

### What It Detects

- **Tables**: Created, dropped
- **Columns**: Added, removed, type changes, nullability, defaults
- **Indexes**: Created, dropped, modified
- **Constraints**: Primary keys, foreign keys, unique, check constraints
- **Enums**: Created, dropped, new values added
- **Functions**: Created, dropped, body changes

### Output Format

```
Comparing 'main' → 'feature-auth'

+ TABLE public.sessions
    id uuid NOT NULL
    user_id uuid NOT NULL
    expires_at timestamp with time zone NOT NULL

~ TABLE public.users
  + COLUMN last_login timestamp with time zone
  ~ COLUMN email: type varchar(100) → varchar(255)

+ INDEX idx_sessions_user_id on public.sessions(user_id)

Summary:
  + 5 addition(s)
  ~ 1 modification(s)
```

Changes are color-coded: `+` green (additions), `-` red (deletions), `~` yellow (modifications). Destructive changes are flagged with a warning indicator.

## Schema Merge

> **Beta**: This feature is in beta. Use with caution and always backup important data.

Merge schema changes from one branch into another. This applies the schema diff as actual DDL statements to the target branch.

```bash
# Merge feature branch into main
pgbranch merge feature-auth main

# Preview changes without applying (dry run)
pgbranch merge feature-auth main --dry-run

# Generate a migration file instead of applying directly
pgbranch merge feature-auth main --migration-file

# Specify custom migration directory
pgbranch merge feature-auth main --migration-file --migration-dir ./db/migrations

# Force merge without confirmation prompts
pgbranch merge feature-auth main --force
```

### Safety Features

- **Dry run mode**: Preview all SQL statements before applying
- **Destructive change warnings**: Explicitly warns about `DROP TABLE`, `DROP COLUMN`, and other data-loss operations
- **Confirmation prompts**: Requires explicit confirmation for destructive changes
- **Validation**: Checks for potential issues before applying

### Migration File Generation

Instead of applying changes directly, generate a timestamped SQL migration file:

```bash
pgbranch merge feature-auth main --migration-file
# Creates: migrations/20240115143022_merge_feature_auth.sql
```

The generated file includes:
- Header with source/target branch info
- All DDL statements in correct dependency order
- Comments for destructive operations

## Continuous Migration

Continuously migrate a PostgreSQL database to another instance using logical replication. Copies schema, performs an initial data snapshot, then streams live changes -- all with table-by-table progress.

```bash
# Full migration (schema + snapshot + live streaming)
pgbranch migrate -c migration.yaml

# Copy schema only
pgbranch migrate -c migration.yaml --schema-only

# Copy schema + initial data, then stop (no live streaming)
pgbranch migrate -c migration.yaml --snapshot-only

# Keep replication slot on exit for later resume
pgbranch migrate -c migration.yaml --keep
```

### Configuration

Create a YAML file describing the source and target:

```yaml
source:
  host: source-db.example.com
  port: 5432
  database: myapp
  user: replicator
  password: secret
  sslmode: require          # disable, allow, prefer (default), require, verify-ca, verify-full

target:
  host: target-db.example.com
  port: 5432
  database: myapp
  user: admin
  password: secret
  sslmode: require

# Tables to migrate (use ["*"] for all user tables)
tables:
  - public.users
  - public.orders
  - public.products

# Optional settings
slot_name: migrate_slot            # replication slot name (default: migrate_slot)
publication_name: migrate_pub      # publication name (default: migrate_pub)
batch_size: 10000                  # rows per batch during snapshot (default: 10000)
```

### How It Works

1. **Validate** -- Checks source has `wal_level=logical`, verifies connectivity, resolves table list
2. **Schema Copy** -- Extracts DDL from source (tables, columns, indexes, constraints, enums, functions) and applies to target
3. **Setup Replication** -- Creates a publication and replication slot on source, exports a consistent snapshot
4. **Initial Snapshot** -- Copies all table data using PostgreSQL's COPY protocol at the consistent snapshot point
5. **WAL Streaming** -- Streams INSERT/UPDATE/DELETE changes in real-time via logical decoding (pgoutput)
6. **Shutdown** -- On Ctrl+C: flushes checkpoint, drops slot + publication (unless `--keep` is set)

### Requirements

- Source PostgreSQL must have `wal_level=logical` (set with `ALTER SYSTEM SET wal_level = 'logical'` + restart)
- Source user must have `REPLICATION` privilege
- Tables should have a `PRIMARY KEY` for UPDATE/DELETE replication
- At least one free replication slot on source

### Resume Support

If interrupted, a checkpoint file is saved alongside your config. Re-running the same command skips completed tables and resumes WAL streaming from the last confirmed position. Use `--keep` to preserve the replication slot across runs.

### Progress Display

Shows a rich TUI with per-table progress bars during the snapshot phase and live counters (inserts, updates, deletes, ops/sec) during streaming. Falls back to periodic log lines in non-TTY environments.

## Requirements

- PostgreSQL (with `psql`, `createdb`, `dropdb` in PATH)
- Go 1.21+ (for installation)

## What It Actually Creates

When you run `pgbranch branch feature-x` on a database called `myapp_dev`, it creates a new database called `myapp_dev_pgbranch_feature_x_c791eb`. That's your snapshot. The trailing digest keeps `feature-x`, `feature/x` and `feature.x` apart, and keeps long branch names from colliding once PostgreSQL truncates identifiers at 63 bytes.

Your working database stays as `myapp_dev`. When you checkout, it gets replaced with a copy of the snapshot.

In a [linked worktree](#git-worktrees) there is no copy: the worktree uses `myapp_dev_pgbranch_feature_x_c791eb` directly, so your main database is never disturbed.

## Automatic Branch Switching

Tired of manually running `pgbranch checkout` every time you switch git branches? Install the git hook:

```bash
pgbranch hook install
```

Now whenever you run `git checkout feature-x`, pgbranch will automatically switch your database to the `feature-x` branch (if it exists).

To remove the hook:

```bash
pgbranch hook uninstall
```

The hook is installed where git actually reads hooks from, so it is shared by
every worktree of the repository and it respects `core.hooksPath` if you use
husky or lefthook.

## Git Worktrees

Worktrees exist so you can have several branches checked out at once. A single
working database cannot represent that, so pgbranch splits the difference:

- **The main worktree** keeps using your configured database, exactly as before.
  Your `DATABASE_URL` never changes.
- **Each linked worktree** uses its branch's own database directly. Creating a
  worktree never touches your main database, and a migration run in one worktree
  is invisible to the others.

```bash
pgbranch hook install
git worktree add ../feature-x -b feature-x
# ✓ pgbranch: created database 'myapp_pgbranch_feature_x_c791eb' for this
#   worktree (main database 'myapp' untouched)
```

This is safe because git already refuses to check one branch out in two
worktrees, so a branch's database always has exactly one writer. pgbranch
enforces the same rule for its own commands: `checkout`, `delete`, and `update`
refuse to touch a branch another worktree is holding.

### Pointing your app at the right database

Ask pgbranch, don't guess:

```bash
eval $(pgbranch env)                          # export into this shell
export DATABASE_URL=$(pgbranch env --url)     # just the URL
pgbranch env --json                           # machine readable
```

pgbranch also writes `.pgbranch/env` inside each worktree. It never edits your
`.env` — a fresh worktree does not have one, since `.env` is gitignored, and
rewriting a DSN in place risks mangling credentials and query parameters.

With direnv, add to `.envrc`:

```bash
dotenv_if_exists .pgbranch/env
```

### Cleaning up

Removing a worktree leaks nothing: its database is simply that branch's
database, and it is still there when you come back. Databases created
automatically for agent worktrees are disposable and reclaimed by:

```bash
pgbranch prune
```

## Claude Code

Running `claude --worktree` on a Postgres project is more dangerous than it
looks. The worktree is a clean checkout, so there is no `.env` in it, and the
app falls back to its default connection string — which is your main
development database. The agent runs a migration and your dev server is looking
at the wreckage.

```bash
pgbranch hook install
pgbranch claude install
```

Now every Claude worktree gets its own database, and Claude is told about it at
session start:

```
pgbranch created database "myapp_pgbranch_worktree_agent1_508c60" for this
worktree. Your main database "myapp" is untouched.
```

Claude can run destructive migrations, drop tables, and test rollbacks without
touching your database. If the work is worth keeping it lives on that branch's
database and survives the worktree being removed.

`pgbranch claude install` writes two things into your repo, and merges rather
than overwrites, so your existing settings and hooks survive:

- a `SessionStart` hook in `.claude/settings.json`, which tells Claude which
  database it owns
- a `pgbranch` skill in `.claude/skills/`, so Claude can rediscover the
  connection string later in a long session

Provisioning is done by the git `post-checkout` hook, which Claude's worktree
creation already triggers. pgbranch deliberately does **not** register a
`WorktreeCreate` hook: that hook *replaces* Claude Code's worktree creation
rather than observing it.

## Remotes

Share database snapshots across machines or with your team using remote storage backends.

### Supported Backends

| Backend | URL Format | Status |
|---------|------------|--------|
| Filesystem | `/path/to/dir` or `file:///path/to/dir` | Supported |
| AWS S3 | `s3://bucket/prefix` | Supported |
| MinIO | `s3://bucket/prefix` (S3-compatible) | Supported |
| Cloudflare R2 | `r2://account-id/bucket/prefix` | Supported |
| Google Cloud Storage | `gs://bucket/prefix` | Supported |
| Azure Blob Storage | - | Planned |

### Remote Commands

```
pgbranch remote add <name> <url>     Add a remote
pgbranch remote list                 List configured remotes
pgbranch remote remove <name>        Remove a remote
pgbranch remote set-default <name>   Set default remote
pgbranch remote ls-remote            List branches on remote
pgbranch remote delete <branch>      Delete branch from remote
pgbranch push <branch>               Push branch to remote
pgbranch pull <branch>               Pull branch from remote
```

### Setting Up a Remote

```bash
# Add a filesystem remote (local network share, mounted drive, etc.)
pgbranch remote add origin /shared/snapshots

# Add an S3 remote (will prompt for credentials)
pgbranch remote add origin s3://my-bucket/pgbranch

# Add a Cloudflare R2 remote
pgbranch remote add origin r2://account-id/my-bucket/pgbranch

# Skip credential prompts and use environment variables instead
pgbranch remote add origin s3://my-bucket/pgbranch --no-credentials
```

### Push and Pull

```bash
# Push a local branch to the remote
pgbranch push main

# Push with a description
pgbranch push main --description "Clean schema with seed data"

# Force overwrite if branch exists on remote
pgbranch push main --force

# Pull a branch from remote
pgbranch pull main

# Pull with a different local name
pgbranch pull main --as main-backup

# Force overwrite if local branch exists
pgbranch pull main --force
```

### Credentials

For S3 and R2 remotes, pgbranch will prompt for your access key and secret key. These credentials are encrypted and stored in your project's `.pgbranch.json` config.

To use environment variables instead (useful for CI/CD), add the remote with `--no-credentials`:

```bash
pgbranch remote add origin s3://bucket/prefix --no-credentials
```

Then set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in your environment.

## Caveats

- This is for **local development only**. Don't use this in production.
- Checkout will **drop your working database**. Uncommitted changes are gone.
- Snapshots are full database copies. They take disk space.
- Active connections to the database will be terminated on checkout.
- `pgbranch checkout` does not work inside a linked worktree, by design: git already pins it to one branch, and that branch owns its database. Use `git checkout`.
- PostgreSQL cannot copy a database that has open connections, so `pgbranch branch <name>` — which copies your live working database — closes them first. Your dev server will reconnect. Creating a *worktree* does not have this problem: it copies the parent branch's database, which nothing is connected to.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=le-vlad/pgbranch&type=date&legend=bottom-right)](https://www.star-history.com/#le-vlad/pgbranch&type=date&legend=bottom-right)

## License

MIT
