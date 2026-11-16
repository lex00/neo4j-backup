"""Pluggable secret resolution for the Neo4j credential (#18).

The credential is resolved **lazily, per connection** (see `Neo4jClient._driver`), not baked
in at construction — so a rotated secret is picked up on the next connect without a redeploy,
and pairs with the auth-expired retry in `retry.py` (#19).

`SECRET_PROVIDER` selects the backend (default `env`); `NEO4J_PASSWORD_REF` is the
provider-specific reference. Each provider lazy-imports its SDK inside `resolve`, so only the
selected provider pulls a dependency.
"""

from __future__ import annotations

import os
from typing import Protocol, runtime_checkable


@runtime_checkable
class SecretProvider(Protocol):
    def resolve(self, ref: str | None) -> str: ...


class EnvSecretProvider:
    """Default — read the credential from an environment variable (`ref`, or `NEO4J_PASSWORD`).
    Never stored in run config; read from the process environment at connect time."""

    def resolve(self, ref: str | None = None) -> str:
        name = ref or "NEO4J_PASSWORD"
        try:
            return os.environ[name]
        except KeyError:
            raise RuntimeError(f"secret env var {name!r} is not set") from None


class AwsSmSecretProvider:
    """AWS Secrets Manager. `ref` = secret id/ARN, with an optional `#json_key` suffix to pull
    one field out of a JSON secret. Region/endpoint come from the standard AWS_* env (boto3 is
    already a dependency)."""

    def resolve(self, ref: str | None) -> str:
        if not ref:
            raise RuntimeError("aws-sm provider needs NEO4J_PASSWORD_REF (secret id/ARN)")
        import json

        import boto3

        secret_id, _, key = ref.partition("#")
        value = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)["SecretString"]
        return json.loads(value)[key] if key else value


# To add Vault / credstash: implement a provider whose `resolve` lazy-imports its SDK, then
# register it here. `ref` carries the path/key; connection config comes from that SDK's env.
_PROVIDERS: dict[str, type] = {
    "env": EnvSecretProvider,
    "aws-sm": AwsSmSecretProvider,
}


def build(name: str) -> SecretProvider:
    try:
        return _PROVIDERS[name]()
    except KeyError:
        known = ", ".join(sorted(_PROVIDERS))
        raise RuntimeError(f"unknown SECRET_PROVIDER {name!r}; known: {known}") from None


def from_env() -> SecretProvider:
    return build(os.environ.get("SECRET_PROVIDER", "env"))


def resolve_ref(ref: str) -> str:
    """The one secret-reference syntax used in the policy (#105): `credentials_ref`, cluster
    `password_ref` (#106) and build `env` values (#109). `<provider>:<ref>` names the provider
    (`env:FINANCE_S3`, `aws-sm:arn:aws:secretsmanager:…#key`); a bare `<ref>` uses the configured
    SECRET_PROVIDER."""
    if not ref:
        raise RuntimeError("empty secret reference")
    name, sep, rest = ref.partition(":")
    if sep and name in _PROVIDERS:
        return build(name).resolve(rest)
    return from_env().resolve(ref)


def key_pair(ref: str) -> dict[str, str]:
    """An S3 key pair from a secret reference: the secret is JSON with `access_key_id`,
    `secret_access_key` and optionally `session_token`."""
    import json

    try:
        value = json.loads(resolve_ref(ref))
        out = {"access_key_id": value["access_key_id"],
               "secret_access_key": value["secret_access_key"]}
    except (ValueError, KeyError, TypeError) as e:
        raise RuntimeError(f"credentials {ref!r} must be JSON with access_key_id and "
                           f"secret_access_key") from e
    if value.get("session_token"):
        out["session_token"] = value["session_token"]
    return out
