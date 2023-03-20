# pgx sample

This sample application shows how to connect to Cloud Spanner through PGAdapter using
the standard `pgx` PostgreSQL driver. PGAdapter is automatically started in a Docker
container by the sample application.

Run the sample with the following command:

```shell
go run pgx_sample.go \
    -project=my-project \
    -instance=my-instance \
    -database=my-database
```

