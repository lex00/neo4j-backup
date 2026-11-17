# Glossary

Alias
: The name applications connect to. A restore points it at a new database.

Group
: Aliases in the policy backed up on one schedule and restored to one instant.

Member
: One alias, or one database in `by-name` mode. A Neo4j machine is a "server", never a member.

Physical database
: The real database behind an alias, named slug plus timestamp. Each restore creates a new one.

Slug
: The alias made safe for database names and bucket paths (`acme.orders` becomes `acme-orders`).

Tier
: A named schedule in the policy: when fulls, differentials, verifies and prunes run.

Full and differential backup
: A full is a complete copy. A differential holds the changes since the previous backup.

Chain
: A full and the differentials after it. Prune deletes whole chains only.

Lane
: Fulls and differentials have separate concurrency limits (the full lane and the diff lane).

Runner
: Where `neo4j-admin` runs: a local process (`RUNNER_MODE=subprocess`) or a pod per command (`RUNNER_MODE=k8s`).

Seed
: How a restore creates a database: the servers fetch a backup from the bucket (`seedURI`).

Cutover
: The moment a restore points the alias at the new database. `rollback` points it back.

Baseline
: A full backup right after a restore or load.

RPO, `rpo_minutes`
: How old the newest backup may get before the member is stale.

RTO, `rto_minutes`
: How long a restore waits for the new database to come online.

`by_design`
: A member not backed up on purpose, such as a read-only rebuilt group. It is never stale.

Fence
: A bucket marker for a restore in progress. The group's backups wait for it.

Sidecar
: The `.backup.json` file next to each backup, describing it.

Lock
: `_locks/<group>.lock`. It stops a group's backup, verify, aggregate and load from overlapping.

Profile
: A named set of group settings that groups use and override.

Tenant
: A label for selecting a customer's groups (`--tenant acme`). It is not access control.
