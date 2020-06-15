# athena_testbench

Testing / Development environment for AWS Athena queries

## Dockerised presto and postgres

This repo comes with default config to spin up:

- presto
- postgresql and its connector

So you only have to `bin/start-server.sh` and use the [Presto CLI](https://prestodb.io/docs/current/installation/cli.html) to access. s

NB `bin/start-server.sh` runs the server as a background process logging to `server_logs.log`. To see stdout and stderror, put a watch on that file.

To stop the server, please use `bin/stop-server.sh`. This will safely bring down the presto network (keyboard interrupted the server does not!)

```
./presto --server localhost:8080 --catalog postgresql --schema public
```

The postgres database is exposing post 5433 so this can be used to populate the database. 5433 was chosen rather than the default (5432) in order to avoid clashing with local postgresql servers. NB - inside the docker container, the postgres server is still on 5432! In spite of what presto might tell you it exists with the user presto and default schema 'public'

### Useful docker commands

The docs

https://docs.docker.com/engine/reference/commandline/docker/

List docker containers

```
docker ps
```

Go to command line in postgres container

```
docker exec -it athena_testbench_postgres_1 /bin/bash
```

Go to command line in presto container

```
docker exec -it athena_testbench_presto_1 /bin/bash
```

## References

- [Presto Docs](https://prestodb.io/docs/current/)
- Python presto library - https://github.com/prestodb/presto-python-client
- Python postgresql library - https://github.com/tlocke/pg8000
- https://github.com/Jimexist/docker-presto
