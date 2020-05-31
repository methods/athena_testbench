import prestodb
import pg8000 as pg

from typing import Sequence


def pg_connect():
    return pg.connect(
        user="presto",
        database="postgres",
        password="9bee2c3975024292eff5829526722ac2",
        port=5433
    )


def presto_connect():
    return prestodb.dbapi.connect(
        host='localhost',
        port=8080,
        user='presto',
        catalog='postgresql',
        schema='public',
    )


def pg_transaction(sql_str: str, values: dict = {}) -> list:
    with pg_connect() as con:
        results = con.run(sql_str, **values)
        con.commit()

        return results


def presto_transaction(sql_str: str) -> list:
    con = presto_connect()
    cur = con.cursor()
    cur.execute(sql_str)
    results = cur.fetchall()
    # con.commit()
    return results
