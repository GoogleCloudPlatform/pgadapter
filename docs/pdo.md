# PGAdapter - PHP PDO Connection Options

PGAdapter has experimental support for the [PHP PDO_PGSQL driver](https://www.php.net/manual/en/ref.pdo-pgsql.php).

## Sample Application

See this [sample application using PHP PDO](../samples/php/pdo) for a PHP sample application that
embeds and starts PGAdapter and the Spanner emulator automatically, and then connects to PGAdapter
using `PDO`.

## Usage

First start PGAdapter:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -x
```

Then connect to PGAdapter using TCP like this:

```php
// Connect to PGAdapter using the PostgreSQL PDO driver.
$dsn = "pgsql:host=localhost;port=5432;dbname=test";
$connection = new PDO($dsn);

// Execute a query on Spanner through PGAdapter.
$statement = $connection->query("SELECT 'Hello World!' as hello");
$rows = $statement->fetchAll();

echo sprintf("Greeting from Cloud Spanner PostgreSQL: %s\n", $rows[0][0]);
```

You can also connect to PGAdapter using Unix Domain Sockets if PGAdapter is running on the same host
as the client application, or the `/tmp` directory in the Docker container has been mapped to a
directory on the local machine:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v /tmp:/tmp
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -x
```

```php
// Connect to PGAdapter using the PostgreSQL PDO driver.
// Connect to host '/tmp' to use Unix Domain Sockets.
$dsn = "pgsql:host=/tmp;port=5432;dbname=test";
$connection = new PDO($dsn);

// Execute a query on Spanner through PGAdapter.
$statement = $connection->query("SELECT 'Hello World!' as hello");
$rows = $statement->fetchAll();

echo sprintf("Greeting from Cloud Spanner PostgreSQL: %s\n", $rows[0][0]);
```

## Running PGAdapter

This example uses the pre-built Docker image to run PGAdapter.
See [README](../README.md) for more possibilities on how to run PGAdapter.


## Performance Considerations

The following will give you the best possible performance when using PHP PDO with PGAdapter.

### Parameterized Queries
Use parameterized queries to reduce the number of times that Spanner has to parse the query. Spanner
caches the query execution plan based on the SQL string. Using parameterized queries allows Spanner
to re-use the query execution plan for different query parameter values, as the SQL string remains
the same.

Example:

```php
$connection = new PDO($dsn);
$statement = $connection->prepare("SELECT * FROM my_table WHERE my_col=:param_name");
$statement->execute(["param_name" => "my-value"]);
$rows = $statement->fetchAll();
```

### Unix Domain Sockets
Use Unix Domain Socket connections for the lowest possible latency when PGAdapter and the client
application are running on the same host.

### Batching
Use the batching options that are available as SQL commands in PGAdapter to batch DDL or DML
statements. PGAdapter will combine DML and DDL statements that are executed in a batch into a single
request on Cloud Spanner. This can significantly reduce the overhead of executing multiple DML or
DDL statements.

Use the `START BATCH DML`, `START BATCH DDL`, and `RUN BATCH` SQL statements to create batches.

Example for DML statements:

```php
$connection->exec("START BATCH DML");
$statement = $connection->prepare("insert into my_table (id, value) values (:id, :value)");
$statement->execute(["id" => 1, "value" => "One"]);
$statement->execute(["id" => 2, "value" => "Two"]);
$connection->exec("RUN BATCH");
```

Example for DDL statements:

```php
$connection->exec("START BATCH DDL");
$connection->exec("CREATE TABLE my_table (id bigint primary key, value varchar)");
$connection->exec("CREATE INDEX my_index ON my_table (value)");
$connection->exec("RUN BATCH");
```
