"""`neo4j-backup-mcp` — an operator-assist MCP server over `neo4j_backup_core` (#58 P5).

Schedulers (Dagster/Airflow/CLI-in-CI) own the cadence; this server owns the *exceptions* — an
operator drives DR and status through an agent: "what's the freshest backup for orders?", "restore
orders to just before 14:32". Read-only by default; mutations are gated behind a read-write scope
*and* a per-call `confirm`, run verify-before-drop for destructive restores, and return the blast
radius first. It is thin over the same core the CLI uses (`tools.py` is pure and dependency-free;
`server.py` is the FastMCP wiring behind the optional `[mcp]` extra).
"""
