# Customer guide

Your platform team backs up your Neo4j databases and restores them on request.
Their settings override the defaults on this page.

## Connect through your alias

Your database has an alias, a stable name such as `orders`. Always connect with the alias.
A restore points it at a new database, so the name behind it (`orders-20261115t204747`) changes.

## Your backups

Your databases belong to a group, and each group has a tier that sets the backup schedule:

| Tier | Most you can lose in a restore |
|---|---|
| gold | about an hour |
| silver | about 6 hours |
| bronze | about a day |

You can restore to the latest backup or to any moment within your retention period.
The default period is 7 days, and your platform team may set another.

All databases in a group go back to the same moment, so data linking them stays consistent.
To restore only one database, say so. It will then sit at a different point in time from the others.

## Ask for a restore

Send your platform team:

1. the alias or group to restore
2. the time in UTC (such as `2026-11-15T09:00:00Z`), or "latest"
3. whether every database in the group may go back to that time
4. the reason, and any ticket number

They will tell you what will be lost and when, then confirm when the restore is done.

## During and after a restore

- Anything written after the chosen time is gone. Export what you need first.
- Work in progress at the switch is cancelled, so make sure your applications retry.
- Your users keep their access. If they get "Access denied" through the alias, tell your platform team.
- The old database is kept for a while, so a restore can be undone. Ask quickly.

## Scheduled rebuilds

If your data is rebuilt on a schedule, your platform team can warn you before each rebuild.

## Leave alias changes to your platform team

Backups follow your alias. Your platform team handles these changes, even if you can run admin commands on the cluster:

- moving, dropping or recreating your alias
- creating or dropping databases behind it
- changing their access mode

Ask your platform team when your databases were last backed up.
