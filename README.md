# pgbranch

Git branching for your PostgreSQL database.

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
```

### Init Options

```
-d, --database   Database name (required)
-H, --host       PostgreSQL host (default: localhost)
-p, --port       PostgreSQL port (default: 5432)
-U, --user       PostgreSQL user (default: postgres)
-W, --password   PostgreSQL password
```

## Requirements

- PostgreSQL (with `psql`, `createdb`, `dropdb` in PATH)
- Go 1.21+ (for installation)

## What It Actually Creates

When you run `pgbranch branch feature-x` on a database called `myapp_dev`, it creates a new database called `myapp_dev_pgbranch_feature_x`. That's your snapshot.

Your working database stays as `myapp_dev`. When you checkout, it gets replaced with a copy of the snapshot.

## Caveats

- This is for **local development only**. Don't use this in production.
- Checkout will **drop your working database**. Uncommitted changes are gone.
- Snapshots are full database copies. They take disk space.
- Active connections to the database will be terminated on checkout.

## License

MIT
