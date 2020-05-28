import pytest
import prestodb
import pg8000 as pg


def pg_connect():
    return pg.connect(
        user="presto",
        database="postgres",
        password="9bee2c3975024292eff5829526722ac2")

def presto_connect():
    return prestodb.dbapi.connect(
        host='localhost',
        port=8080,
        user='presto',
        catalog='postgresql',
        schema='public',
    )

def test_python_can_connect_to_presto():
    conn = presto_connect()
    cur = conn.cursor()
    cur.execute('SELECT * FROM system.runtime.nodes')
    rows = cur.fetchall()
    assert len(rows) > 0


def test_python_can_connect_to_postgres():

    con = pg_connect()
    con.run("CREATE TABLE IF NOT EXISTS  book (id SERIAL, title TEXT)")
    con.run("TRUNCATE book")
    for title in ("Ender's Game", "The Magus"):
        con.run("INSERT INTO book (title) VALUES (:title)", title=title)
    results = con.run("SELECT * FROM book")
    con.commit()

    assert len(results) > 0


def test_round_trip():
    psql_con = pg_connect()


    psql_con.run("CREATE TABLE IF NOT EXISTS book (id SERIAL, title TEXT)")
    psql_con.run("TRUNCATE book")
    for title in ("Ender's Game", "The Magus"):
        psql_con.run("INSERT INTO book (title) VALUES (:title)", title=title)
    psql_con.commit()

    presto_con = presto_connect()
    cur = presto_con.cursor()
    cur.execute('SELECT * FROM book')
    rows = cur.fetchall()

    assert len(rows) > 0
