import prestodb
import pg8000 as pg


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
