"""#21 pluggable path layout — the default must stay byte-identical to the shipped scheme."""

import pytest

from neo4j_backup_core import paths
from neo4j_backup_core.paths import DefaultPathLayout, PathLayout


def test_default_layout_byte_identical():
    layout = DefaultPathLayout()
    assert layout.alias_prefix("demo", "acme-orders") == "demo/acme-orders/"
    assert (layout.physical_prefix("demo", "acme-orders", "acme-orders-20260629t120000")
            == "demo/acme-orders/acme-orders-20260629t120000/")
    key = "demo/acme-orders/acme-orders-20260629t120000/full.backup"
    assert layout.physical_of_key("demo", "acme-orders", key) == "acme-orders-20260629t120000"
    assert layout.metadata_prefix() == "_dbms/"
    assert layout.metadata_key("20260629t120000") == "_dbms/metadata-20260629t120000.cypher"
    assert layout.system_prefix() == "_dbms/system/"


def test_module_shims_delegate_to_default():
    layout = DefaultPathLayout()
    assert paths.alias_prefix("g", "a") == layout.alias_prefix("g", "a")
    assert paths.physical_prefix("g", "a", "p") == layout.physical_prefix("g", "a", "p")
    round_trip = paths.physical_of_key("g", "a", paths.physical_prefix("g", "a", "p") + "x.backup")
    assert round_trip == "p"
    assert paths.metadata_key("t") == layout.metadata_key("t")
    assert paths.system_prefix() == layout.system_prefix()


def test_get_layout_default():
    got = paths.get_layout()
    assert isinstance(got, DefaultPathLayout)
    assert isinstance(got, PathLayout)  # runtime_checkable protocol


def test_get_layout_selects_named_class(monkeypatch):
    monkeypatch.setenv("PATH_LAYOUT", "neo4j_backup_core.paths.DefaultPathLayout")
    assert isinstance(paths.get_layout(), DefaultPathLayout)


def test_get_layout_rejects_bare_name(monkeypatch):
    monkeypatch.setenv("PATH_LAYOUT", "NoDotHere")
    with pytest.raises(RuntimeError, match="module.Class"):
        paths.get_layout()
