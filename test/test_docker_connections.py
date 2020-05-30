from utils.connections import presto_connect, pg_connect, presto_transaction, \
    pg_transaction


def test_python_can_connect_to_presto():
    results = presto_transaction('SELECT * FROM system.runtime.nodes')

    assert len(results) > 0


def test_python_can_connect_to_postgres():
    results = []
    try:
        pg_transaction("CREATE TABLE IF NOT EXISTS  book (id SERIAL, title TEXT)")
        pg_transaction("TRUNCATE book")
        for title in ("Ender's Game", "The Magus"):
            pg_transaction(f"INSERT INTO book (title) VALUES (%s)", [title])
        results = pg_transaction("SELECT * FROM book")

    except Exception as e:
        print(f"EXCEPTION: {repr(e)}")
    finally:
        pg_transaction("DROP TABLE IF EXISTS book")

    assert len(results) > 0


def test_round_trip():
    results = []
    try:
        pg_transaction("CREATE TABLE IF NOT EXISTS  book (id SERIAL, title TEXT)")
        pg_transaction("TRUNCATE book")
        for title in ("Ender's Game", "The Magus"):
            pg_transaction(f"INSERT INTO book (title) VALUES (%s)", [title])
        results = presto_transaction('SELECT * FROM book')

    except Exception as e:
        print(f"EXCEPTION: {repr(e)}")
    finally:
        pg_transaction("DROP TABLE IF EXISTS book")

    assert len(results) > 0
