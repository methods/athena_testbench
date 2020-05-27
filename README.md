# athena_testbench
Testing / Development environment for AWS Athena queries


## Dockerised presto and postgres

This repo comes with default config to spin up:

- presto
- postgresql and its connector

So you only have to `docker-compose up` and use the [Presto CLI](https://prestodb.io/docs/current/installation/cli.html) to access
```
./presto --server localhost:8080 --catalog postgresql --schema public
```

The postgres database is exposing it's default port (5432) so this can be used to populate the database. In spite of what presto might tell you it exists with the user presto and default schema 'public'


### Useful docker commands

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

* https://github.com/Jimexist/docker-presto