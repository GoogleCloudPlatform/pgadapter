Build the different psql Docker images like this:

```shell
docker build . -f build/DockerfilePsql11 -t psql11
```

Run the different psql Docker images like this:

```shell
docker run --network="host" -it psql11 -h localhost -p 5432 -d my-test-db
```
