---
name: pgbranch
description: Find, inspect, or switch the PostgreSQL database this git worktree uses. Use before running migrations, seeds, tests, or any SQL, and whenever a database connection fails, DATABASE_URL is unset, or you are unsure which database you are connected to.
---

# pgbranch

This repository uses pgbranch to give each git worktree its own PostgreSQL
database. Work in a worktree never touches the developer's main database.

## Find the database you should use

```sh
pgbranch env            # shell exports
pgbranch env --url      # just the connection URL
pgbranch env --json     # machine readable
```

`pgbranch env` is always correct for the directory you are in. Prefer it over
reading `.env`, which is gitignored and therefore absent from a fresh worktree.

Environment variables do not persist between commands here, so export in the
same command that needs it:

```sh
export DATABASE_URL=$(pgbranch env --url) && npm run migrate
```

The same values are written to `.pgbranch/env` in the worktree root, for
processes that read a dotenv file.

## Rules

**Never connect to the main database from a worktree.** `pgbranch env --json`
reports it as `main_database`; it is the developer's, and their dev server is
probably attached to it. Your database is the `database` field.

**Do not edit `.env`.** pgbranch owns `.pgbranch/env`. Rewriting a DSN in place
risks corrupting credentials and query parameters.

**`pgbranch checkout` does not work in a worktree, by design.** Git already pins
the worktree to one branch, and that branch owns its database. Use `git checkout`
if you want a different branch; the database follows automatically.

## Destructive work is safe here

Because the worktree has its own database, you may freely run migrations, drop
tables, test rollbacks, and reseed. The developer's database is unaffected. If
the work is worth keeping, it lives on this branch's database and survives the
worktree being removed.

## Other commands

```sh
pgbranch status         # current branch and database
pgbranch branch         # list database branches
pgbranch prune          # reclaim databases from deleted worktrees
```
