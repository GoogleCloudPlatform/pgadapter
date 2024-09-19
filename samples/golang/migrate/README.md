# Golang Migrate

This sample application shows how to use [Golang Migrate](https://github.com/golang-migrate/migrate)
with PGAdapter. The sample application automatically starts PGAdapter and the Spanner emulator in a
Docker container and connects to this. It then executes a small set of migration scripts.

__NOTE__: Golang Migrate only works with PGAdapter if you add `fallback_application_name=golang-migrate`
to the connection string. See [sample.go](./sample.go) for a full example.

```go
connString := "postgres://localhost:5432/my-database?sslmode=disable&fallback_application_name=golang-migrate"
m, err := migrate.New("file://migrations", connString)
```

If you fail to add the application name to the connection string, you will get an error that
the function `pg_advisory_lock` is not supported.
