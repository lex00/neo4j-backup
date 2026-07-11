"""Regression: adapter resources must delegate every public method of the core client they
wrap, so an asset calling e.g. `store.list_text_keys(...)` doesn't AttributeError at runtime
(the Dagster `prune` asset hit exactly this via `metadata.prune`)."""

import pytest

pytest.importorskip("dagster")

from neo4j_backup_core.clients import (
    AzureObjectStore,
    Neo4jClient,
    ObjectStore,
    S3ObjectStore,
)
from neo4j_backup_dagster.resources import Neo4jResource, ObjectStoreResource


def _public_methods(cls) -> set:
    return {n for n, v in vars(cls).items() if not n.startswith("_") and callable(v)}


def test_objectstore_resource_delegates_the_interface():
    # the resource must forward every ObjectStore (protocol) method callers use
    missing = _public_methods(ObjectStore) - set(dir(ObjectStoreResource))
    assert not missing, f"ObjectStoreResource missing delegations: {sorted(missing)}"


def test_backends_implement_the_interface():
    # every cloud backend must implement the whole interface (dir() = own + base composites)
    for backend in (S3ObjectStore, AzureObjectStore):
        missing = _public_methods(ObjectStore) - set(dir(backend))
        assert not missing, f"{backend.__name__} missing interface methods: {sorted(missing)}"


def test_neo4j_resource_delegates_all_core_methods():
    missing = _public_methods(Neo4jClient) - set(dir(Neo4jResource))
    assert not missing, f"Neo4jResource missing delegations: {sorted(missing)}"
