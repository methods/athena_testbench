from utils.connections import presto_connect, pg_connect


def test_python_can_connect_to_presto():
    con = presto_connect()
    cur = con.cursor()
    cur.execute('SELECT * FROM system.runtime.nodes')
    rows = cur.fetchall()
    con.commit()
    assert len(rows) > 0


def test_python_can_connect_to_postgres():

    with pg_connect() as con:
        con.run("CREATE TABLE IF NOT EXISTS  book (id SERIAL, title TEXT)")
        con.run("TRUNCATE book")
        for title in ("Ender's Game", "The Magus"):
            con.run("INSERT INTO book (title) VALUES (:title)", title=title)
        results = con.run("SELECT * FROM book")

    assert len(results) > 0


def test_round_trip():
    with pg_connect() as psql_con:
        psql_con.run("CREATE TABLE IF NOT EXISTS book (id SERIAL, title TEXT)")
        psql_con.run("TRUNCATE book")
        for title in ("Ender's Game", "The Magus"):
            psql_con.run("INSERT INTO book (title) VALUES (:title)", title=title)

    presto_con = presto_connect()
    cur = presto_con.cursor()
    cur.execute('SELECT * FROM book')
    rows = cur.fetchall()
    presto_con.commit()

    assert len(rows) > 0
