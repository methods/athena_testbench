import pytest
import itertools

from utils.connections import  presto_transaction, pg_transaction


def kill_running_presto_queries():
        all_queries = presto_transaction("SELECT * FROM system.runtime.queries")
        running_query_ids = (query[1] for query in all_queries if 'RUNNING' in query)

        for query_id in running_query_ids:
            presto_transaction(
                f"CALL system.runtime.kill_query(query_id => '{query_id}')"
            )



def test_pg_transaction_connects_to_postgres():
    results = pg_transaction("SELECT * FROM information_schema.tables")

    assert len(results) > 0


def test_pg_transaction_can_create_and_drop_tables():
    pg_transaction("CREATE TABLE IF NOT EXISTS test_table (id SERIAL, content TEXT)")

    all_tables = itertools.chain(*pg_transaction("SELECT * FROM information_schema.tables"))

    assert "test_table" in all_tables

    pg_transaction("DROP TABLE IF EXISTS test_table")

    all_tables = itertools.chain(*pg_transaction("SELECT * FROM information_schema.tables"))

    assert "test_table" not in all_tables


def test_pg_transaction_reads_from_tables():
    pg_transaction("CREATE TABLE IF NOT EXISTS  book (id SERIAL, title TEXT)")
    pg_transaction("TRUNCATE book")

    expected_content = ["Ender's Game", "The Magus"]

    for title in expected_content:
        pg_transaction(f"INSERT INTO book (title) VALUES (:title)", {"title": title})


    results = pg_transaction("SELECT * FROM book")

    pg_transaction("DROP TABLE IF EXISTS book")

    assert expected_content[0] in results[0]
    assert expected_content[1] in results[1]


def test_presto_transaction_connects_to_presto():
    results = presto_transaction("SELECT * FROM system.runtime.nodes")

    assert len(results) > 0


def test_presto_transaction_reads_from_postgres_tables():
    pg_transaction("CREATE TABLE IF NOT EXISTS book (id SERIAL, title TEXT)")
    pg_transaction("TRUNCATE book")

    expected_content = ["Ender's Game", "The Magus"]

    for title in expected_content:
        pg_transaction(f"INSERT INTO book (title) VALUES (:title)", {"title": title})


    results_pg = pg_transaction("SELECT * FROM book")
    results_presto = presto_transaction("SELECT * FROM book")

    kill_running_presto_queries()
    pg_transaction("DROP TABLE IF EXISTS book")

    assert list(results_pg) == list(results_presto)
