# Customer guide

For teams whose Neo4j databases are run by their organization's platform team. The platform team
backs up your databases and restores them when you ask. This page explains what you get, how to ask
for a restore, and what to expect while it happens.

Your platform team may have changed some of the defaults described here; their word wins.

## Connect through your alias

Your database has an **alias**, a stable name such as `orders`, and a database behind it with a
name like `orders-20261115t204747`. Always connect with the alias.

A restore creates a new database from a backup and moves your alias to it, so the database name
changes every time. Anything that connects with the database name (an application, a BI tool, a
script) stops working after a restore. The alias keeps working.

## What your backups give you

Each set of your databases belongs to a **group**, and each group has a **tier** that sets the
backup schedule. The example tiers:

| Tier | Full backup | Differential backup | Most you can lose in a restore |
|---|---|---|---|
| gold | daily | hourly | about an hour |
| silver | weekly | every 6 hours | about 6 hours |
| bronze | weekly | daily | about a day |

You can ask for your databases back as they were at the latest backup, or at any point in time
covered by backups that are still kept (your group's retention, 7 days unless your platform team
set otherwise). A point in time needs a differential backup after it; between backups, the
restore lands on the exact time you ask for, as long as a later differential backup exists.

All databases in a group are restored together, to the same instant, so data that refers across
them stays consistent. If you need only one of them back, say so: the platform team can restore
just that one, and it will then no longer line up in time with the others.

## Ask for a restore

Send your platform team:

- the alias (or group) to restore;
- the point in time, in UTC (for example `2026-11-15T09:00:00Z`), or "latest";
- whether every database in the group may go back to that time;
- why, and any ticket number your organization uses.

They will tell you what will be lost and when the switch will happen, then confirm when it's done
with the time restored to and the new database names.

## During and after a restore

- Your data goes back to the chosen time. Anything written after it is gone. If you need some
  of those writes, export them before the restore.
- Running transactions are cancelled at the switch. Queries and writes in flight when the
  alias moves are rolled back. Make sure your applications retry; on a cluster, servers may briefly
  disagree about where the alias points.
- Your users keep their access. Privileges granted on your database (directly or through the
  alias) are copied to the restored database before the switch. If your users get "Access denied"
  through the alias after a restore, tell the platform team.
- The old database is kept for a while, so a restore can be undone by moving the alias back.
  Ask quickly if the restore wasn't what you needed.

## Scheduled rebuilds

If your graph is rebuilt on a schedule, the platform team can have the tool warn you before each
rebuild (a webhook or a message): when the switch will happen and what will be lost (your writes
since the previous load, unless they're kept). Transactions running at the switch are cancelled,
as for a restore.

## If you have credentials on the cluster

Some teams can run administration commands on the cluster. Please don't:

- move, drop or recreate your alias;
- create databases behind your alias, or drop the ones the platform team manages;
- change a database's access mode.

Backups follow your alias. If it is moved to a database the platform team doesn't know about,
backups of it can fail or protect the wrong data. Ask the platform team instead.

## Not available yet

A page where you can see your own backup status without asking. Until then, ask the platform
team: they can tell you when each database was last backed up and how far back you can restore.
