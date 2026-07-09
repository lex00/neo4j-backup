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
from pydantic import BaseModel, Field, field_validator, model_validator

from . import naming

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


class DbGroup(BaseModel):
    id: str
    owner: str | None = None
    aliases: list[str]
    tier: str
    s3_prefix: str
    retention_days: int = 7
    rpo_minutes: int = 60
    rto_minutes: int = 120
    encryption: Encryption = Field(default_factory=Encryption)
    topology: Topology | None = None
    overrides: dict[str, dict] = Field(default_factory=dict)

    @field_validator("aliases")
    @classmethod
    def _validate_aliases(cls, v: list[str]) -> list[str]:
        for a in v:
            naming.validate_alias(a)  # raises on illegal alias
        return v

    def topology_for(self, alias: str) -> Topology | None:
        """Seed topology for an alias: a per-alias `overrides[alias].topology` wins over
        the group's, else the group's (or None -> no TOPOLOGY clause)."""
        ov = self.overrides.get(alias, {}).get("topology")
        if ov is not None:
            return Topology.model_validate(ov)
        return self.topology


class Tier(BaseModel):
    full_cron: str
    diff_cron: str


class Policy(BaseModel):
    db_groups: list[DbGroup]
    tiers: dict[str, Tier]

    @model_validator(mode="after")
    def _tiers_resolve(self) -> "Policy":
        for g in self.db_groups:
            if g.tier not in self.tiers:
                raise ValueError(f"group {g.id!r} references unknown tier {g.tier!r}")
        return self

    def group(self, gid: str) -> DbGroup:
        for g in self.db_groups:
            if g.id == gid:
                return g
        raise KeyError(gid)

    def partition_keys(self) -> list[str]:
        """One work unit per (group, alias), encoded as 'group/alias'."""
        return [f"{g.id}/{a}" for g in self.db_groups for a in g.aliases]

    def groups_for_tier(self, tier: str) -> list[DbGroup]:
        return [g for g in self.db_groups if g.tier == tier]


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
