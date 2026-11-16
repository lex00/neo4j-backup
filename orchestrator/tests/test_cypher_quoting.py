"""#77(b): one quoting helper for every DDL statement; the seed URI is checked before it is
written into Cypher."""

import pytest

from neo4j_backup_core import cypher, metadata, privileges
from neo4j_backup_core.clients import Neo4jClient


class Recorder(Neo4jClient):
    def __init__(self):
        super().__init__()
        self.cypher = []

    def run_system(self, cypher, **params):
        if cypher.startswith("SHOW DATABASE"):  # the seed's wait for every server (#221)
            return [{"currentStatus": "online"}]
        self.cypher.append(cypher)
        return []


def test_quote_doubles_backticks():
    assert cypher.quote("a`b") == "`a``b`"
    assert privileges.quote is cypher.quote and metadata._bt("a`b") == "`a``b`"


def test_alias_with_a_backtick_gives_valid_quoted_ddl():
    neo = Recorder()
    neo.alter_alias("orders`; DROP DATABASE x; //", "orders-1")
    neo.create_alias("we`ird", "orders-1")
    neo.stop_database("db`x")
    neo.drop_database("db`x")
    neo.seed_database("db`x", "s3://b/k.backup")
    assert neo.cypher == [
        "ALTER ALIAS `orders``; DROP DATABASE x; //` SET DATABASE TARGET `orders-1`",
        "CREATE ALIAS `we``ird` IF NOT EXISTS FOR DATABASE `orders-1`",
        "STOP DATABASE `db``x` WAIT",
        "DROP DATABASE `db``x` IF EXISTS WAIT",
        "CREATE DATABASE `db``x` OPTIONS { seedURI: 's3://b/k.backup' } WAIT 3600 SECONDS",
    ]


@pytest.mark.parametrize("uri", ["s3://b/k'}) WAIT; DROP DATABASE x //", "http://evil/x",
                                 "s3://b/k\\x", "s3://b/k\n"])
def test_bad_seed_uris_are_refused(uri):
    neo = Recorder()
    with pytest.raises(ValueError):
        neo.seed_database("db", uri)
    assert neo.cypher == []


def test_seed_uri_must_be_in_the_store():
    neo = Recorder()
    with pytest.raises(ValueError, match="not in the backup store"):
        neo.seed_database("db", "s3://other-bucket/k.backup", store_base="s3://neo4j-backups/")
    neo.seed_database("db", "s3://neo4j-backups/g/k.backup", store_base="s3://neo4j-backups/")
    neo.seed_database("db", "file:/seed/x.backup")  # FileSeedProvider, no store check asked
    assert len(neo.cypher) == 2
