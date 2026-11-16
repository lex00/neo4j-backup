"""Policy schema + loader. Mirrors policies/*.yaml (DESIGN.md §2, §11).

The db_group is the policy + PITR-alignment unit; aliases are the app-facing names
(validated against the full alias spec via the naming authority).
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, timedelta
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


class StoreConfig(BaseModel):
    """A named object store (#105). `url` is `s3://<bucket>`, `gs://<bucket>` or
    `azb://<account>/<container>`; the rest are optional per-store settings. `credentials_ref` is
    a secret reference (resolved by the secret provider; wired with the runner env in #105's
    second part)."""
    model_config = ConfigDict(extra="forbid")
    url: str
    region: str | None = None
    endpoint: str | None = None
    # the endpoint as neo4j-admin on the runner sees it, when it differs (e.g. a local stack where
    # the tool runs on the host and the runner in the compose network); default: `endpoint`
    runner_endpoint: str | None = None
    sse: str | None = None
    sse_kms_key_id: str | None = None
    write_args: dict = Field(default_factory=dict)
    credentials_ref: str | None = None

    @model_validator(mode="after")
    def _url(self) -> "StoreConfig":
        scheme, _, rest = self.url.partition("://")
        parts = [p for p in rest.split("/") if p]
        if scheme not in ("s3", "gs", "azb") or not parts:
            raise ValueError(f"store url {self.url!r}: expected s3://<bucket>, gs://<bucket> "
                             f"or azb://<account>/<container>")
        if (scheme == "azb" and len(parts) != 2) or (scheme != "azb" and len(parts) != 1):
            raise ValueError(f"store url {self.url!r}: name the bucket (or account/container) "
                             f"only; key prefixes come from the path layout")
        return self

    @property
    def cloud(self) -> str:
        return {"s3": "aws", "gs": "gcp", "azb": "azure"}[self.url.split("://")[0]]

    @property
    def bucket(self) -> str:
        """The bucket (S3/GCS) or container (Azure)."""
        return self.url.split("://", 1)[1].rstrip("/").split("/")[-1]

    @property
    def account(self) -> str | None:
        return self.url.split("://", 1)[1].split("/")[0] if self.cloud == "azure" else None


class Cutover(BaseModel):
    model_config = ConfigDict(extra="forbid")
    strategy: Literal["alias-swap", "external"] = "alias-swap"
    hook: str | None = None


class Cluster(BaseModel):
    """One Neo4j DBMS the policy covers (#106). `default` comes from the environment
    (NEO4J_BOLT_URI, NEO4J_USER, NEO4J_PASSWORD_REF, NEO4J_BACKUP_SOURCE, …) unless defined."""
    model_config = ConfigDict(extra="forbid")
    bolt_uri: str
    user: str = "neo4j"
    password_ref: str | None = None  # secret reference (#105); None: SECRET_PROVIDER's default
    backup_source: str | list[str] = "neo4j:6362"
    storage: str | None = None       # the cluster's store; a group's `storage` wins (#105)
    topology: Topology | None = None  # default for the cluster's groups
    cypher_version: str | None = None
    cutover: Cutover | None = None
    backup_upload: Literal["admin", "pipeline"] | None = None

    @property
    def backup_sources(self) -> str:
        """`--from` value: one address, or several comma-separated."""
        return (",".join(self.backup_source) if isinstance(self.backup_source, list)
                else self.backup_source)


class Check(BaseModel):
    """A check on a new database before its swap (#111): Cypher returning one number."""
    model_config = ConfigDict(extra="forbid")
    query: str
    expect: str
    name: str | None = None

    @model_validator(mode="after")
    def _expect(self) -> "Check":
        from .checks import parse_expect
        parse_expect(self.expect)
        return self


class Notice(BaseModel):
    """Warn a customer before a load or rebuild replaces their database (#126)."""
    model_config = ConfigDict(extra="forbid")
    hook: str                 # an http(s) URL (POST JSON) or a command (NOTICE_* env vars)
    before: str = "0m"        # wait this long after the notice before building (e.g. 15m)
    required: bool = False    # a failed hook stops the load (default: logged, load goes on)

    @property
    def before_seconds(self) -> int:
        n, unit = self.before[:-1], self.before[-1]
        mult = {"s": 1, "m": 60, "h": 3600}.get(unit)
        if mult is None or not n.isdigit():
            raise ValueError(f"notice.before {self.before!r}: use e.g. 15m, 1h or 900s")
        return int(n) * mult

    @model_validator(mode="after")
    def _check(self) -> "Notice":
        self.before_seconds
        return self


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
    env: dict[str, str] = Field(default_factory=dict)  # values `secret:<ref>` are resolved (#105)
    # kind: command (#109): what the command leaves for the tool
    output: Literal["import", "dump"] = "import"
    import_args: list[str] = Field(default_factory=list)  # output: import, paths under the output
    timeout: str = "2h"                                    # e.g. 90m, 2h, 3600s

    @property
    def timeout_seconds(self) -> int:
        n, unit = self.timeout[:-1], self.timeout[-1]
        mult = {"s": 1, "m": 60, "h": 3600}.get(unit)
        if mult is None or not n.isdigit():
            raise ValueError(f"build.timeout {self.timeout!r}: use e.g. 90m, 2h or 3600s")
        return int(n) * mult

    @model_validator(mode="after")
    def _kind_fields(self) -> "LoadBuild":
        if self.kind == "import" and self.input_type is None:
            raise ValueError("build.kind 'import' needs input_type (csv or parquet)")
        if self.kind == "command" and not (self.command or self.image):
            raise ValueError("build.kind 'command' needs a command or an image")
        if self.kind == "command" and self.output == "import" and not self.import_args:
            raise ValueError("build.kind 'command' with output 'import' needs import_args "
                             "(the --nodes/--relationships files the command writes)")
        self.timeout_seconds  # validate
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
    checks: list[Check] = Field(default_factory=list)  # #111, after the default checks
    notice: Notice | None = None  # #126, before the build

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
    profile: str | None = None  # the profile its settings came from (#101), for reporting
    # A label for selecting a customer's groups (#102). NOT an access boundary: the deployment
    # has full access to every database in its clusters. `owner` is the old name.
    tenant: str | None = None
    owner: str | None = None  # deprecated: read as `tenant`
    # A group is alias-swap (apps connect via aliases; restore seeds a new physical + swaps the
    # alias — the default, non-destructive) OR by-name (#48: no alias; restore targets the
    # database by its own name — create-if-absent, or destructive DROP+CREATE with replace).
    restore_mode: Literal["alias-swap", "by-name"] = "alias-swap"
    aliases: list[str] = Field(default_factory=list)      # alias-swap mode
    databases: list[str] = Field(default_factory=list)    # by-name mode (database names)
    tier: str
    storage: str | None = None  # a name under `storage:` (#105); None: the cluster's, else default
    cluster: str | None = None  # a name under `clusters:` (#106); None: `default`
    s3_prefix: str | None = None  # deprecated (#105): never read; the store comes from `storage`
    retention_days: int = 7
    rpo_minutes: int = 60
    rto_minutes: int = 120
    encryption: Encryption = Field(default_factory=Encryption)
    topology: Topology | None = None
    overrides: dict[str, dict] = Field(default_factory=dict)
    load: Load = Field(default_factory=Load)  # absent: mode live, as before (#103)
    # offboarding (#124): a retired group has no schedules; its artifacts stay restorable until
    # `retired_on` + `retire_after_days` (default: retention_days), then prune deletes them
    state: Literal["active", "retired"] = "active"
    retired_on: date | None = None
    retire_after_days: int | None = None

    @property
    def retired(self) -> bool:
        return self.state == "retired"

    def deletes_on(self) -> date | None:
        """The day prune deletes a retired group's artifacts."""
        if not self.retired or self.retired_on is None:
            return None
        return self.retired_on + timedelta(days=self.retire_after_days
                                           if self.retire_after_days is not None
                                           else self.retention_days)

    @model_validator(mode="after")
    def _retirement(self) -> "DbGroup":
        if self.retired and self.retired_on is None:
            raise ValueError(f"group {self.id!r}: state: retired needs retired_on (YYYY-MM-DD), "
                             f"the day the retention period starts")
        if not self.retired and (self.retired_on or self.retire_after_days is not None):
            raise ValueError(f"group {self.id!r}: retired_on and retire_after_days need "
                             f"state: retired")
        if self.retire_after_days is not None and self.retire_after_days < 0:
            raise ValueError(f"group {self.id!r}: retire_after_days must be >= 0")
        return self

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
    def _s3_prefix_deprecated(self) -> "DbGroup":
        if self.s3_prefix is not None:
            _log.warning("policy: group %r sets 's3_prefix', which is ignored; name a store with "
                         "'storage:' instead (#105)", self.id)
        return self

    @model_validator(mode="after")
    def _owner_is_tenant(self) -> "DbGroup":
        if self.owner is not None:
            if self.tenant is None:
                self.tenant = self.owner
            _log.warning("policy: group %r uses 'owner', which is now 'tenant' (#102); "
                         "rename it", self.id)
        return self

    @model_validator(mode="before")
    @classmethod
    def _lowercase_databases(cls, data):
        # Neo4j stores database names lowercase (#85): accept `Orders`, use `orders`
        if isinstance(data, dict) and isinstance(data.get("databases"), list):
            data = {**data, "databases": [d.lower() if isinstance(d, str) else d
                                          for d in data["databases"]]}
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
    # Maintenance (#104). On a tier that takes backups (has full_cron) an unset value means
    # today's fixed schedule, so behaviour is unchanged; a rebuild-only tier gets none unless set.
    verify_cron: str | None = None
    aggregate_cron: str | None = None
    prune_cron: str | None = None
    cleanup_cron: str | None = None  # for cleanup of superseded databases (#93)

    @model_validator(mode="after")
    def _some_cron(self) -> "Tier":
        if not (self.full_cron or self.diff_cron or self.rebuild_cron):
            raise ValueError("a tier needs at least one of full_cron, diff_cron, rebuild_cron")
        if self.full_cron:
            for name, default in MAINTENANCE_DEFAULTS.items():
                if getattr(self, name) is None:
                    setattr(self, name, default)
        return self

    def crons(self) -> dict[str, str]:
        """Every job this tier schedules: lane name -> cron (only those set)."""
        names = ("full", "diff", "rebuild", "verify", "aggregate", "prune", "cleanup")
        return {n: getattr(self, f"{n}_cron") for n in names if getattr(self, f"{n}_cron")}


# Today's fixed Airflow schedules (avp_dags.py before #104): the defaults, so nothing moves.
MAINTENANCE_DEFAULTS = {"verify_cron": "0 6 * * *", "aggregate_cron": "0 5 * * 0",
                        "prune_cron": "0 7 * * 0"}


class Dbms(BaseModel):
    """DBMS-wide jobs (#104): the metadata export and the `system` database backup, and how many
    of each to keep. Per cluster once clusters exist (#106); one `default` until then."""
    model_config = ConfigDict(extra="forbid")
    metadata_cron: str = "0 2 * * *"
    system_backup_cron: str = "0 1 * * *"
    keep_metadata: int = Field(default=14, ge=1)
    keep_system: int = Field(default=14, ge=1)


class SelectionError(ValueError):
    """A command was given no group, both a group and a tenant, or an unknown one (#102)."""


PROFILE_FORBIDDEN = ("id", "aliases", "databases", "overrides", "profile")
_MERGED_PER_KEY = ("load",)  # dict fields merged key by key; everything else is replaced


def resolve_profiles(raw: dict) -> tuple[dict, dict[str, dict[str, str]]]:
    """Apply `profiles:` to the raw policy dict (#101), before validation, so a profile can supply
    required fields. Returns (resolved raw dict, provenance): provenance maps group id -> field
    -> "group" | "profile:<name>" (`load.<key>` for the per-key merged `load`). A group's own
    values win; `load` merges per key; lists (args, checks) and other values are replaced."""
    if not isinstance(raw, dict):
        return raw, {}
    profiles = raw.get("profiles") or {}
    if not isinstance(profiles, dict):
        raise ValueError("profiles must be a map of name -> group settings")
    for name, prof in profiles.items():
        if not isinstance(prof, dict):
            raise ValueError(f"profile {name!r} must be a map of group settings")
        bad = [k for k in prof if k in PROFILE_FORBIDDEN]
        if bad:
            raise ValueError(f"profile {name!r} can't set {', '.join(bad)} (per group only)")
        unknown = [k for k in prof if k not in DbGroup.model_fields]
        if unknown:
            raise ValueError(f"profile {name!r}: unknown group field(s) {', '.join(unknown)}")
    groups, prov = [], {}
    for g in raw.get("db_groups") or []:
        if not isinstance(g, dict):
            groups.append(g)
            continue
        src = {k: "group" for k in g if k != "profile"}
        name = g.get("profile")
        if name is not None:
            if name not in profiles:
                known = ", ".join(sorted(profiles)) or "none defined"
                raise ValueError(f"group {g.get('id')!r} uses unknown profile {name!r} "
                                 f"(profiles: {known})")
            merged = {}
            for k, v in profiles[name].items():
                merged[k] = v
                src[k] = f"profile:{name}"
            for k, v in g.items():
                if k in _MERGED_PER_KEY and isinstance(v, dict) and isinstance(merged.get(k), dict):
                    for kk in merged[k]:
                        src[f"{k}.{kk}"] = f"profile:{name}"
                    for kk in v:
                        src[f"{k}.{kk}"] = "group"
                    src[k] = "merged"
                    merged[k] = {**merged[k], **v}
                else:
                    merged[k] = v
                    src[k] = "group" if k != "profile" else src.get(k)
            g = merged
        groups.append(g)
        prov[str(g.get("id"))] = {k: v for k, v in src.items() if v}
    return {**raw, "db_groups": groups}, prov


class Policy(BaseModel):
    db_groups: list[DbGroup]
    tiers: dict[str, Tier]
    profiles: dict[str, dict] = Field(default_factory=dict)  # #101; resolved before validation
    dbms: Dbms = Field(default_factory=Dbms)  # #104
    # Named stores (#105). Without a `default` entry, the environment (BACKUP_BUCKET, CLOUD, …)
    # defines the `default` store, as before.
    storage: dict[str, StoreConfig] = Field(default_factory=dict)
    # Neo4j clusters (#106). Without a `default` entry, the environment defines it, as before.
    clusters: dict[str, Cluster] = Field(default_factory=dict)
    # initial status of the Dagster schedules generated from this policy (#104)
    schedules: Literal["stopped", "running"] = "stopped"

    @model_validator(mode="before")
    @classmethod
    def _apply_profiles(cls, data):
        return resolve_profiles(data)[0] if isinstance(data, dict) else data

    @model_validator(mode="after")
    def _tiers_resolve(self) -> "Policy":
        for name in self.clusters:
            if name == "system" or name.startswith("metadata"):
                raise ValueError(f"cluster name {name!r} is reserved ('system', 'metadata…': "
                                 f"they would collide with the default cluster's _dbms/ keys)")
        for name, c in self.clusters.items():
            if c.storage and c.storage != "default" and c.storage not in self.storage:
                raise ValueError(f"cluster {name!r}: storage {c.storage!r} is not a store")
        ids: set[str] = set()
        members: dict[tuple[str, str], str] = {}
        for g in self.db_groups:
            if g.id in ids:
                raise ValueError(f"group id {g.id!r} is used twice (ids must be unique, #106)")
            ids.add(g.id)
            cl = g.cluster or "default"
            if g.cluster and g.cluster != "default" and g.cluster not in self.clusters:
                known = ", ".join(sorted({"default", *self.clusters}))
                raise ValueError(f"group {g.id!r}: cluster {g.cluster!r} is not defined "
                                 f"(clusters: {known})")
            for n in g.names:
                if (cl, n) in members:
                    raise ValueError(f"{n!r} on cluster {cl!r} is in groups {members[(cl, n)]!r} "
                                     f"and {g.id!r}; a database or alias belongs to one group")
                members[(cl, n)] = g.id
            if g.tier not in self.tiers:
                raise ValueError(f"group {g.id!r} references unknown tier {g.tier!r}")
            for what, name in (("storage", g.storage),
                               ("load.source.storage", g.load.source.storage
                                if g.load.source else None)):
                if name and name != "default" and name not in self.storage:
                    known = ", ".join(sorted({"default", *self.storage}))
                    raise ValueError(f"group {g.id!r}: {what} {name!r} is not a store "
                                     f"(stores: {known})")
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
        out += self._rpo_warnings()
        return out

    def _rpo_warnings(self) -> list[str]:
        """A group whose backups (or, for a group not backed up by design, rebuilds) come further
        apart than its `rpo_minutes` can't meet it (#123)."""
        out = []
        for g in self.db_groups:
            if g.retired:  # no schedules (#124)
                continue
            t = self.tiers[g.tier]
            load = g.load
            by_design = load.writes == "read-only" or (load.writes == "read-write"
                                                       and load.protect_writes is False)
            name, expr = (("rebuild_cron", t.rebuild_cron) if by_design
                          else ("diff_cron", t.diff_cron) if t.diff_cron
                          else ("full_cron", t.full_cron))
            if not expr:
                continue
            try:
                gap = cron.max_gap(expr)
            except ValueError:
                continue
            if gap is not None and gap.total_seconds() > g.rpo_minutes * 60:
                out.append(f"group {g.id!r}: tier {g.tier!r} runs {name} {expr!r} up to "
                           f"{int(gap.total_seconds() // 60)} minutes apart, longer than "
                           f"rpo_minutes {g.rpo_minutes}; `status` will report it stale")
        return out

    def group(self, gid: str) -> DbGroup:
        for g in self.db_groups:
            if g.id == gid:
                return g
        raise KeyError(gid)

    def store_name(self, group_id: str) -> str:
        """The store a group's backups use (#105): the group's `storage`, else its cluster's
        (#106), else `default`."""
        g = self.group(group_id)
        c = self.clusters.get(g.cluster or "default")
        return g.storage or (c.storage if c else None) or "default"

    def cluster_store_name(self, cluster: str) -> str:
        """The store a cluster's DBMS objects (metadata exports, system backups) go to."""
        c = self.clusters.get(cluster)
        return (c.storage if c else None) or "default"

    def cluster_name(self, group_id: str) -> str:
        return self.group(group_id).cluster or "default"

    def cluster_names(self) -> list[str]:
        return sorted({"default", *self.clusters, *(g.cluster for g in self.db_groups
                                                     if g.cluster)})

    def tenants(self) -> list[str]:
        return sorted({g.tenant for g in self.db_groups if g.tenant})

    def select(self, group: str | None = None, tenant: str | None = None) -> list[DbGroup]:
        """The groups a command acts on: one by id, or every group with a tenant label (#102).
        Raises SelectionError for both, neither, or an unknown group or tenant."""
        if group and tenant:
            raise SelectionError("give a group or --tenant, not both")
        if tenant:
            out = [g for g in self.db_groups if g.tenant == tenant]
            if not out:
                known = ", ".join(self.tenants()) or "none"
                raise SelectionError(f"unknown tenant {tenant!r} (tenants: {known})")
            return out
        if not group:
            raise SelectionError("give a group or --tenant")
        try:
            return [self.group(group)]
        except KeyError:
            known = ", ".join(g.id for g in self.db_groups)
            raise SelectionError(f"unknown group {group!r} (groups: {known})") from None

    def partition_keys(self) -> list[str]:
        """One work unit per (group, member), encoded as 'group/name' (alias or database)."""
        return [f"{g.id}/{n}" for g in self.db_groups for n in g.names]

    def groups_for_tier(self, tier: str, *, include_retired: bool = False) -> list[DbGroup]:
        """The tier's groups for its scheduled lanes; retired groups (#124) are left out, except
        for prune (`include_retired`), which deletes their artifacts when the period ends."""
        return [g for g in self.db_groups if g.tier == tier
                and (include_retired or not g.retired)]


def _days(gap) -> str:
    d = gap.total_seconds() / 86400
    return f"{d:g} days" if d != 1 else "1 day"


def explain(source: str | Path, group_id: str) -> dict:
    """A group's effective settings and where each came from (#101): `group` (the group's
    resolved fields) and `from` (field -> "group" | "profile:<name>" | "default"; `load` fields
    as `load.<key>`). Reads and validates the whole policy; touches no cluster or store."""
    raw = yaml.safe_load(_read_source(str(source)))
    resolved, prov = resolve_profiles(raw)
    policy = Policy.model_validate(raw)
    group = policy.group(group_id)
    src = prov.get(group_id, {})
    dumped = group.model_dump(mode="json")
    origin = {}
    for k, v in dumped.items():
        if k == "load":
            for kk in v:
                origin[f"load.{kk}"] = src.get(f"load.{kk}") or (
                    src["load"] if src.get("load", "").startswith(("group", "profile"))
                    else "default")
        else:
            origin[k] = src.get(k, "default")
    return {"group": dumped, "from": origin}


def validate_file(source: str | Path) -> list[str]:
    """Every problem with a policy file, as messages (empty = valid). Reads only the file (#101):
    no cluster, no store, so it can run as a pull-request check on the policy repo."""
    try:
        raw = yaml.safe_load(Path(str(source)).read_text())
    except FileNotFoundError:
        return [f"{source}: no such file"]
    except yaml.YAMLError as e:
        return [f"{source}: not valid YAML: {e}"]
    try:
        Policy.model_validate(raw)
    except ValidationError as e:
        return [f"{'.'.join(str(x) for x in err['loc']) or 'policy'}: "
                f"{err['msg'].removeprefix('Value error, ')}" for err in e.errors()]
    except ValueError as e:
        return [str(e)]
    return []


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
