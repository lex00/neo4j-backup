# Glossary

Alias
: The name applications connect to (`CREATE ALIAS orders FOR DATABASE orders-20261116t0200`).
  A restore points it at a new database, so applications never change their connection.

Group
: A set of aliases (or databases) in the policy that are backed up on one schedule and restored
  to one instant. Usually one customer, or one customer's related databases.

Member
: One alias (or, in `by-name` mode, one database) in a group. A Neo4j machine is a "server",
  never a member.

Physical database
: The real database behind an alias. Its name is the alias's slug plus a timestamp, and a
  restore creates a new one.

Slug
: The alias made safe for database names and bucket paths (`acme.orders` becomes
  `acme-orders`). It names the member's folder in the bucket.

Tier
: A named schedule in the policy: when fulls, differentials, verifies and prunes run. Every
  group has one.

Full and differential backup
: A full is a complete copy of a database. A differential holds only the changes since the
  previous backup, so it is small and quick.

Chain
: A full and the differentials taken after it. A restore replays the chain up to the point
  you ask for; prune deletes whole chains only.

Lane
: Full backups and differential backups run with separate concurrency limits, called the
  full lane and the diff lane, so a long full doesn't hold up the hourly diffs.

Runner
: Where `neo4j-admin` runs: a process on the worker (`RUNNER_MODE=subprocess`) or a Kubernetes
  pod per command (`RUNNER_MODE=k8s`).

Seed
: How a restore creates a database: `CREATE DATABASE … OPTIONS {seedURI: …}` tells the Neo4j
  servers to fetch a backup from the bucket and start the database from it.

Cutover
: The moment a restore points the alias at the new database. Before it, nothing changes for
  applications; after it, `rollback` points the alias back.

Baseline
: A full backup taken right after a restore or load, so the next differentials have a full of
  the new database to build on.

RPO, `rpo_minutes`
: Recovery point objective: how old the newest backup may get before the database counts as
  stale and the freshness check fails.

RTO, `rto_minutes`
: Recovery time objective: how long a restore waits for a seed to come online before it gives up.

`by_design`
: The freshness state of a member that is not backed up on purpose (a read-only group rebuilt
  from source), so it is never reported stale.

Fence
: A marker in the bucket that a restore of a group is in progress. Backups of that group wait
  for it, so they don't back up a database that is about to be replaced.

Sidecar
: A small JSON file next to each backup in the bucket (`.backup.json`) with its kind,
  database, store id and time, so the tool can build chains without opening backups.

Lock
: `_locks/<group>.lock` in the bucket. A group's backup, verify, aggregate and load take it, so
  they never run at the same time whoever started them.

Profile
: A named set of group settings in the policy (`app-live`, `data-interval`) that groups use
  and override.

Tenant
: A label on groups for selecting a customer's groups at once (`--tenant acme`). It is not
  access control.
