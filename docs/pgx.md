# Google Cloud Spanner PostgreSQL Adapter - pgx Support

PGAdapter has limited support for the [Go pgx driver](https://github.com/jackc/pgx) version 4.15.0
and higher. 

## Usage

First start PGAdapter:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull us-west1-docker.pkg.dev/cloud-spanner-pg-adapter/pgadapter-docker-images/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  us-west1-docker.pkg.dev/cloud-spanner-pg-adapter/pgadapter-docker-images/pgadapter \
  -p my-project -i my-instance -d my-database \
  -x
```

Then connect to PGAdapter like this:

```go
// pwd:uid is not used by PGAdapter, but it is required in the connection string.
// Replace localhost and 5432 with the host and port number where PGAdapter is running.
// statement_cache_capacity=0 disables the use of prepared statements in pgx.
// sslmode=disable instructs pgx to try plain text mode directly. Otherwise, pgx will try two times
// with SSL enabled before trying plain text.
connString := "postgres://uid:pwd@localhost:5432/?statement_cache_capacity=0&sslmode=disable"
ctx := context.Background()
conn, err := pgx.Connect(ctx, connString)
if err != nil {
    return err
}
defer conn.Close(ctx)

var greeting string
err = conn.QueryRow(ctx, "select 'Hello world!' as hello").Scan(&greeting)
if err != nil {
    return err
}
fmt.Printf("Greeting from Cloud Spanner PostgreSQL: %v\n", greeting)
```
