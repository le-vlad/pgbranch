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
- [Automatic Branch Switching](#automatic-branch-switching)
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
# ... switch to feature branch, run migrations, break things ...
pgbranch branch feature-x      # save this state too
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

# Create a branch from current state
pgbranch branch main

# Do some work, run migrations, whatever
# Then save that state
pgbranch branch feature-auth

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
pgbranch hook install          Install git hook for auto-switching
pgbranch hook uninstall        Remove the git hook
pgbranch diff <branch1> [branch2]  Compare schemas between branches
pgbranch merge <source> <target>   Merge schema changes (Beta)
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

## Requirements

- PostgreSQL (with `psql`, `createdb`, `dropdb` in PATH)
- Go 1.21+ (for installation)

## What It Actually Creates

When you run `pgbranch branch feature-x` on a database called `myapp_dev`, it creates a new database called `myapp_dev_pgbranch_feature_x`. That's your snapshot.

Your working database stays as `myapp_dev`. When you checkout, it gets replaced with a copy of the snapshot.

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

## Remotes

Share database snapshots across machines or with your team using remote storage backends.

### Supported Backends

| Backend | URL Format | Status |
|---------|------------|--------|
| Filesystem | `/path/to/dir` or `file:///path/to/dir` | Supported |
| AWS S3 | `s3://bucket/prefix` | Supported |
| MinIO | `s3://bucket/prefix` (S3-compatible) | Supported |
| Cloudflare R2 | `r2://account-id/bucket/prefix` | Supported |
| Google Cloud Storage | `gs://bucket/prefix` | Planned |
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

## License

MIT
