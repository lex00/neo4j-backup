"""`neo4j-backup` — a scheduler-agnostic CLI over `neo4j_backup_core` (#58 P1).

A third thin adapter beside Dagster/Airflow, for teams on CI/cron with no orchestrator. Subprocess
execution only (the CLI runs on a runner/CI host); every subcommand honours the agent-drivable
contract in `neo4j_backup_core.cli_contract` (#60) — see CLI-CONTRACT.md.
"""
