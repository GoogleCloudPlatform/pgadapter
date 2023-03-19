# psycopg3 sample

This sample application shows how to connect to Cloud Spanner through PGAdapter using
the standard `psycopg3` PostgreSQL driver. PGAdapter is automatically started in a Docker
container by the sample application.

Run the sample with the following command:

```shell
python psycopg3_sample-py \
    -p my-project \
    -i my-instance \
    -i my-database
```
