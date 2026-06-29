"""Policy schema + loader. Mirrors policies/*.yaml (DESIGN.md §2, §11).

The db_group is the policy + PITR-alignment unit; aliases are the app-facing names
(validated against the full alias spec via the naming authority).
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from . import naming


class Encryption(BaseModel):
    mode: Literal["sse-kms", "client-side", "none"] = "sse-kms"
    kms_key_ref: str | None = None


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
    overrides: dict[str, dict] = Field(default_factory=dict)

    @field_validator("aliases")
    @classmethod
    def _validate_aliases(cls, v: list[str]) -> list[str]:
        for a in v:
            naming.validate_alias(a)  # raises on illegal alias
        return v


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


def load_policy(path: str | Path) -> Policy:
    data = yaml.safe_load(Path(path).read_text())
    return Policy.model_validate(data)
