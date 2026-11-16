"""Policy schema + loader. Mirrors policies/*.yaml (DESIGN.md §2, §11).

The db_group is the policy + PITR-alignment unit; aliases are the app-facing names
(validated against the full alias spec via the naming authority).
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from . import cron, naming

_log = logging.getLogger(__name__)


class Encryption(BaseModel):
    mode: Literal["sse-kms", "client-side", "none"] = "sse-kms"
    kms_key_ref: str | None = None


class Topology(BaseModel):
    """Cluster shape for a seeded database: `CREATE DATABASE … TOPOLOGY n PRIMARIES m
    SECONDARIES` (DESIGN.md §3). Applied at seed time (restore or bulk import), so a
    restored physical keeps its redundancy instead of the DBMS default. Omitted entirely
    when a group declares no topology — required for standalone/single-instance DBMS,
    where the clause is illegal."""

    primaries: int = Field(default=1, ge=1)
    secondaries: int = Field(default=0, ge=0)


class LoadSource(BaseModel):
    """Where a rebuild's input files live (#103): a named store (#105) and a prefix. Default:
    the group's own store under `<group>/_source/`."""
    model_config = ConfigDict(extra="forbid")
    storage: str = "default"
    prefix: str | None = None


class LoadBuild(BaseModel):
    """How a load is built. `import`: `neo4j-admin database import full` with `args` passed
    through (the tool puts the database name first, as `import_command` does). `command`: a
    command or container run in the runner (#109)."""
    model_config = ConfigDict(extra="forbid")
    kind: Literal["import", "command"] = "import"
    input_type: Literal["csv", "parquet"] | None = None
    args: list[str] = Field(default_factory=list)
    command: list[str] = Field(default_factory=list)
    image: str | None = None
    env: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _kind_fields(self) -> "LoadBuild":
        if self.kind == "import" and self.input_type is None:
            raise ValueError("build.kind 'import' needs input_type (csv or parquet)")
        if self.kind == "command" and not (self.command or self.image):
            raise ValueError("build.kind 'command' needs a command or an image")
        return self


class Load(BaseModel):
    """How a group's data arrives and what is kept (#103). Schema only here; loads, rebuilds and
    write protection are #107, #108 and #112.

    | customer                                  | mode    | writes     | protect_writes |
    | live application                          | live    | read-write | true           |
    | bulk loads on an interval only            | rebuild | read-only  | n/a            |
    | bulk loads, writes allowed but disposable | rebuild | read-write | false          |
    | bulk loads, writes kept within a cycle    | rebuild | read-write | true           |
    | bulk load once, then live                 | once    | read-write | true           |
    | static dataset                            | once    | read-only  | n/a            |
    """
    model_config = ConfigDict(extra="forbid")
    mode: Literal["live", "once", "rebuild"] = "live"
    writes: Literal["read-only", "read-write"] = "read-write"
    protect_writes: bool | None = None  # default: true for read-write; n/a for read-only
    keep_loads: int = 14
    baseline: bool = True  # full backup right after a load or restore (#97)
    source: LoadSource | None = None
    build: LoadBuild | None = None

    @model_validator(mode="after")
    def _combinations(self) -> "Load":
        explicit = "protect_writes" in self.model_fields_set
        if self.keep_loads < 1:
            raise ValueError("load.keep_loads must be >= 1")
        if self.mode == "live" and self.writes == "read-only":
            raise ValueError("load.mode 'live' can't be read-only (nothing would write to it)")
        if self.mode == "live" and explicit and self.protect_writes is False:
            raise ValueError("load.mode 'live' always backs up its writes; protect_writes: false "
                             "is only for rebuild groups")
        if self.writes == "read-only":
            if explicit and self.protect_writes is True:
                raise ValueError("load.protect_writes: true needs writes: read-write")
            self.protect_writes = None  # n/a
        elif self.protect_writes is None:
            self.protect_writes = True
        if self.mode == "rebuild" and self.build is None:
            raise ValueError("load.mode 'rebuild' needs a build (how each cycle's data is made)")
        return self

    @property
    def backs_up_writes(self) -> bool:
        """Whether the group's databases get scheduled full/diff backups."""
        return self.writes == "read-write" and bool(self.protect_writes)


class DbGroup(BaseModel):
    id: str
    owner: str | None = None
    # A group is alias-swap (apps connect via aliases; restore seeds a new physical + swaps the
    # alias — the default, non-destructive) OR by-name (#48: no alias; restore targets the
    # database by its own name — create-if-absent, or destructive DROP+CREATE with replace).
    restore_mode: Literal["alias-swap", "by-name"] = "alias-swap"
    aliases: list[str] = Field(default_factory=list)      # alias-swap mode
    databases: list[str] = Field(default_factory=list)    # by-name mode (database names)
    tier: str
    s3_prefix: str
    retention_days: int = 7
    rpo_minutes: int = 60
    rto_minutes: int = 120
    encryption: Encryption = Field(default_factory=Encryption)
    topology: Topology | None = None
    overrides: dict[str, dict] = Field(default_factory=dict)
    load: Load = Field(default_factory=Load)  # absent: mode live, as before (#103)

    @model_validator(mode="before")
    @classmethod
    def _load_names_the_group(cls, data):
        # validate `load:` here so its errors name the group (#103)
        if isinstance(data, dict) and isinstance(data.get("load"), dict):
            try:
                data = {**data, "load": Load.model_validate(data["load"])}
            except ValidationError as e:
                msgs = "; ".join(f"{'.'.join(str(x) for x in err['loc']) or 'load'}: "
                                 f"{err['msg'].removeprefix('Value error, ')}"
                                 for err in e.errors())
                raise ValueError(f"group {data.get('id')!r}: load: {msgs}") from None
        return data

    @model_validator(mode="after")
    def _validate_members(self) -> "DbGroup":
        if self.restore_mode == "by-name":
            if self.aliases:
                raise ValueError(f"group {self.id!r}: use 'databases' (not 'aliases') in by-name mode")
            if not self.databases:
                raise ValueError(f"group {self.id!r}: by-name mode needs 'databases'")
            for d in self.databases:
                naming.validate_db(d)  # stricter than alias: a legal DATABASE name
        else:
            if self.databases:
                raise ValueError(f"group {self.id!r}: 'databases' is only valid in by-name mode")
            if not self.aliases:
                raise ValueError(f"group {self.id!r}: alias-swap mode needs 'aliases'")
            for a in self.aliases:
                naming.validate_alias(a)
        return self

    @property
    def names(self) -> list[str]:
        """The group's members — aliases or database names per `restore_mode`. Backup, schedules,
        prune, and the partition set are mode-agnostic over this."""
        return self.databases if self.restore_mode == "by-name" else self.aliases

    def topology_for(self, alias: str) -> Topology | None:
        """Seed topology for an alias: a per-alias `overrides[alias].topology` wins over
        the group's, else the group's (or None -> no TOPOLOGY clause)."""
        ov = self.overrides.get(alias, {}).get("topology")
        if ov is not None:
            return Topology.model_validate(ov)
        return self.topology


class Tier(BaseModel):
    # Each optional (#103): a tier for load-only groups (read-only, or protect_writes: false)
    # may have only rebuild_cron. Groups that back up their writes need full_cron and diff_cron.
    full_cron: str | None = None
    diff_cron: str | None = None
    rebuild_cron: str | None = None

    @model_validator(mode="after")
    def _some_cron(self) -> "Tier":
        if not (self.full_cron or self.diff_cron or self.rebuild_cron):
            raise ValueError("a tier needs at least one of full_cron, diff_cron, rebuild_cron")
        return self


class Policy(BaseModel):
    db_groups: list[DbGroup]
    tiers: dict[str, Tier]

    @model_validator(mode="after")
    def _tiers_resolve(self) -> "Policy":
        for g in self.db_groups:
            if g.tier not in self.tiers:
                raise ValueError(f"group {g.id!r} references unknown tier {g.tier!r}")
            t = self.tiers[g.tier]
            if g.load.mode == "rebuild" and not t.rebuild_cron:
                raise ValueError(f"group {g.id!r}: load.mode 'rebuild' needs tier {g.tier!r} "
                                 f"to have a rebuild_cron")
            if g.load.backs_up_writes and not (t.full_cron and t.diff_cron):
                raise ValueError(f"group {g.id!r} backs up its writes, so tier {g.tier!r} needs "
                                 f"full_cron and diff_cron")
        for w in self.warnings():
            _log.warning("policy: %s", w)
        return self

    def warnings(self) -> list[str]:
        """Settings that load but probably aren't what the team wants (#73): a tier whose full
        backups are further apart than a group's retention_days. Prune keeps the chain covering
        the start of the window, so nothing breaks, but backups are kept longer than
        `retention_days` (up to the full interval) and the policy reads as shorter than it is."""
        out = []
        for g in self.db_groups:
            if not self.tiers[g.tier].full_cron:
                continue
            try:
                gap = cron.max_gap(self.tiers[g.tier].full_cron)
            except ValueError:
                continue  # a form the schedulers accept but cron.py doesn't read (@daily, MON)
            if gap is not None and gap.total_seconds() > g.retention_days * 86400:
                out.append(
                    f"group {g.id!r}: tier {g.tier!r} takes a full backup up to every "
                    f"{_days(gap)} (full_cron {self.tiers[g.tier].full_cron!r}), longer than "
                    f"retention_days {g.retention_days}; prune keeps each chain until a newer "
                    f"full covers the window, so backups stay up to {_days(gap)} plus "
                    f"{g.retention_days} days")
        return out

    def group(self, gid: str) -> DbGroup:
        for g in self.db_groups:
            if g.id == gid:
                return g
        raise KeyError(gid)

    def partition_keys(self) -> list[str]:
        """One work unit per (group, member), encoded as 'group/name' (alias or database)."""
        return [f"{g.id}/{n}" for g in self.db_groups for n in g.names]

    def groups_for_tier(self, tier: str) -> list[DbGroup]:
        return [g for g in self.db_groups if g.tier == tier]


def _days(gap) -> str:
    d = gap.total_seconds() / 86400
    return f"{d:g} days" if d != 1 else "1 day"


def parse_partition_key(key: str) -> tuple[str, str]:
    group_id, alias = key.split("/", 1)
    return group_id, alias


# In-process cache keyed by source; also the last-known-good store. Dagster's daemon and run
# workers are separate processes, so each keeps its own cache — expected.
_cache: dict[str, tuple[Policy, float]] = {}


def _read_source(source: str) -> str:
    """Read the raw policy YAML from a local path or an s3:// URI (#43), or via a team-supplied
    fetcher for authenticated/custom delivery (#46).

    `POLICY_LOADER=module.callable` names an importable `(source: str) -> str` that returns the
    raw YAML — the override for an authenticated endpoint / Vault / config API (it does its own
    auth; this repo ships none). Selected exactly like PATH_LAYOUT/SECRET_PROVIDER; the SDK is
    the loader's own lazy import. `load_policy`'s validation, cache, and last-known-good wrap it
    unchanged, so a fetch error folds into last-known-good."""
    spec = os.environ.get("POLICY_LOADER")
    if spec:
        import importlib

        module, _, attr = spec.rpartition(".")
        if not module:
            raise RuntimeError(f"POLICY_LOADER must be 'module.callable', got {spec!r}")
        return getattr(importlib.import_module(module), attr)(source)
    if source.startswith("s3://"):
        import boto3  # lazy — only when an s3 source is used

        bucket, _, key = source[len("s3://"):].partition("/")
        client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3") or None,  # MinIO/local override
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )
        return client.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    path = source[len("file://"):] if source.startswith("file://") else source
    return Path(path).read_text()


def _transient(e: Exception) -> bool:
    """A fetch error worth retrying (network, S3 throttling), not a policy that's wrong."""
    if isinstance(e, (yaml.YAMLError, ValueError, FileNotFoundError, KeyError, TypeError)):
        return False  # bad YAML, schema/validation error (pydantic raises ValueError), no file
    return True


def load_policy_at_startup(source: str | Path, *, configured: bool, attempts: int = 4,
                           delay: float = 1.0, sleep=time.sleep) -> Policy | None:
    """Load the policy when an orchestrator builds its schedules or DAGs (#75).

    - No policy configured (`configured` is False and the local default file doesn't exist):
      returns None and logs it, so the caller can build nothing on purpose.
    - A policy that doesn't parse or validate: raises, so the code location (Dagster) or the
      DAG file (Airflow) fails to load with the error, instead of silently having no schedules.
    - A transient fetch error (S3 at cold start, with no last-known-good): retried with
      exponential backoff (`delay`, 2*`delay`, …) for `attempts` tries, then raised.
    """
    src = str(source)
    if not configured and not src.startswith(("s3://", "file://")) and not Path(src).exists() \
            and not os.environ.get("POLICY_LOADER"):
        _log.info("no policy configured (NEO4J_BACKUP_POLICY unset, %s absent); "
                  "building no schedules", src)
        return None
    for i in range(attempts):
        try:
            return load_policy(src)
        except Exception as e:  # noqa: BLE001
            if not _transient(e) or i == attempts - 1:
                _log.error("policy from %s failed to load: %s", src, e)
                raise
            wait = delay * (2 ** i)
            _log.warning("policy fetch from %s failed (%s); retrying in %.0fs", src, e, wait)
            sleep(wait)
    return None  # unreachable


def load_policy(source: str | Path, *, force: bool = False) -> Policy:
    """Load + validate the policy from a local path or `s3://bucket/key.yaml` (#43).

    Cached for `POLICY_CACHE_TTL` seconds (default 60; `0` = always fetch); `force=True` bypasses
    the cache (the reconcile sensor uses it, since it gates whether a new database gets a
    partition). On a fetch/parse failure the last successfully-loaded policy is returned
    (last-known-good) with a warning; a cold start with nothing cached re-raises.
    """
    source = str(source)
    ttl = float(os.environ.get("POLICY_CACHE_TTL", "60"))
    hit = _cache.get(source)
    if hit and not force and (time.monotonic() - hit[1]) < ttl:
        return hit[0]
    try:
        policy = Policy.model_validate(yaml.safe_load(_read_source(source)))
        _cache[source] = (policy, time.monotonic())
        return policy
    except Exception as e:  # noqa: BLE001 — last-known-good fallback, or re-raise on cold start
        if hit:
            _log.warning("policy reload from %s failed (%s); using last known good", source, e)
            return hit[0]
        raise
