"""#117: which privileges belong to a database, and how they move to a new physical."""

from neo4j_backup_core import privileges as P

CMDS = [
    "GRANT ACCESS ON DATABASE `orders-a` TO `r`",
    "GRANT MATCH {*} ON GRAPH `orders-a` NODE * TO `r`",
    "DENY WRITE ON GRAPH `orders-a` TO `r`",
    "GRANT SET PROPERTY {x} ON GRAPH `orders-a` NODE Foo TO `r`",
    "GRANT ACCESS ON DATABASE `orders-ab` TO `r`",          # a different database
    "GRANT MATCH {*} ON GRAPH * NODE * TO `reader`",         # all graphs: untouched
    "GRANT ACCESS ON HOME DATABASE TO `r`",                  # home: untouched
    "GRANT ACCESS ON DATABASE `x` TO `orders-a`",            # role named like the database
    "GRANT ACCESS ON DATABASE `orders-a``b` TO `r`",         # quoted name with a backtick
]


def test_quote_doubles_backticks():
    assert P.quote("a`b") == "`a``b`"


def test_scoped_to_matches_only_the_on_clause():
    assert P.scoped_to(CMDS, "orders-a") == CMDS[:4]
    assert P.scoped_to(CMDS, "orders-a`b") == [CMDS[8]]


def test_rewrite_moves_the_on_clause_and_keeps_roles():
    assert P.rewrite(CMDS[1], "orders-a", "orders-b") == \
        "GRANT MATCH {*} ON GRAPH `orders-b` NODE * TO `r`"
    assert P.rewrite(CMDS[7], "orders-a", "orders-b") == CMDS[7]  # role name untouched
    assert P.rewrite("DENY WRITE ON GRAPH `a` TO `a`", "a", "b") == "DENY WRITE ON GRAPH `b` TO `a`"
