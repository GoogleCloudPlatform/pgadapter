# JDBC sample

This sample application shows how to connect to Cloud Spanner through PGAdapter using the standard
`JDBC` PostgreSQL driver. PGAdapter is automatically started in-process together with the sample
application.

Run the sample with the following command:

```shell
mvn exec:java \
  -Dexec.args=" \
    -p my-project \
    -i my-instance \
    -d my-database"
```
