# PGAdapter - npgsql Connection Options

PGAdapter has experimental support for the [.NET npgsql driver](https://www.npgsql.org/index.html). 

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

```csharp
// Replace localhost and 5432 with the host and port number where PGAdapter is running.
// SSL Mode=Disable instructs npgsql to try plain text mode directly instead of first trying SSL.

var connectionString = "Host=localhost;Port=5432;Database=my-database;SSL Mode=Disable";
using var connection = new NpgsqlConnection(connectionString);
connection.Open();

using var cmd = new NpgsqlCommand("select 'Hello world!' as hello", connection);
using (var reader = cmd.ExecuteReader())
{
    while (reader.Read())
    {
        var greeting = reader.GetString(0);
        Console.WriteLine($"Greeting from Cloud Spanner PostgreSQL: {greeting}");
    }
}
```

You can also connect to PGAdapter using Unix Domain Sockets if PGAdapter is running on the same host
as the client application:

```csharp
// '/tmp' is the default domain socket directory for PGAdapter. This can be changed using the -dir
// command line argument. 5432 is the default port number used by PGAdapter. Change this in the
// connection string if PGAdapter is running on a custom port.

var connectionString = "Host=/tmp;Port=5432;Database=my-database";
using var connection = new NpgsqlConnection(connectionString);
connection.Open();

using var cmd = new NpgsqlCommand("select 'Hello world!' as hello", connection);
using (var reader = cmd.ExecuteReader())
{
    while (reader.Read())
    {
        var greeting = reader.GetString(0);
        Console.WriteLine($"Greeting from Cloud Spanner PostgreSQL: {greeting}");
    }
}
```


## Running PGAdapter

This example uses the pre-built Docker image to run PGAdapter.
See [README](../README.md) for more possibilities on how to run PGAdapter.

## Limitations

### Transactions with Default Isolation Level
Starting a transaction in npgsql without specifying an isolation level will cause the following error:

```
P0001: Unknown statement: BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED
```

npgsql always starts a transaction with isolation level `read committed` if no isolation level is
specified when calling `connection.BeginTransaction()`. Include `IsolationLevel.Serializable` to
work around this problem:

```csharp
using var connection = new NpgsqlConnection(ConnectionString);
connection.Open();
// The serialization level *MUST* be specified in npgsql. Otherwise,
// npgsql will default to read-committed.
var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
```

## Performance Considerations

The following will give you the best possible performance when using npgsql with PGAdapter.

### Unix Domain Sockets
Use Unix Domain Socket connections for the lowest possible latency when PGAdapter and the client
application are running on the same host. See https://www.npgsql.org/doc/connection-string-parameters.html
for more information on connection options for npgsql.

### Autocommit or Read-Only Transactions
Use auto-commit or read-only transactions for workloads that only read data. Reading in a read/write
transaction takes unnecessary locks if you are not planning on executing any write operations.

#### Autocommit
Not assigning a transaction to a command will make it use autocommit.

```csharp
using var cmd = new NpgsqlCommand("SELECT 1", connection);
using (var reader = cmd.ExecuteReader())
{
   ...
}
```

#### Read-Only Transaction
Execute `SET TRANSACTION READ ONLY` to use a read-only transaction. A read-only transaction will
not take any locks. See the Cloud Spanner [read-only transaction documentation](https://cloud.google.com/spanner/docs/transactions#read-only_transactions)
for more information on read-only transactions.

NOTE: You must commit or rollback the read-only transaction in npgsql to end the transaction. There
is no semantic or performance difference between committing or rolling back a read-only transaction.

```csharp
// The serialization level *MUST* be specified in npgsql. Otherwise,
// npgsql will default to read-committed.
var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
// npgsql and ADO.NET do not expose any native API creating a read-only transaction.
new NpgsqlCommand("set transaction read only", connection, transaction).ExecuteNonQuery();
using var cmd = new NpgsqlCommand("SELECT 1", connection, transaction);
using (var reader = cmd.ExecuteReader())
{
   ...
}
```

### Batching / Pipelining
Use [batching / pipelining](https://www.npgsql.org/doc/performance.html#batchingpipelining) for
optimal performance when executing multiple statements. This both saves round-trips between your
application and PGAdapter and between PGAdapter and Cloud Spanner.

You can batch any type of statement. A batch can also contain a mix of different types of statements,
such as both queries and DML statements. It is also possible (and recommended!) to batch DDL
statements, but it is not recommended to mix DDL statements with other types of statements in one
batch.

#### DML Batch Example

```csharp
var sql = "INSERT INTO my_table (key, value) values ($1, $2)";
var batchSize = 10;
using var batch = new NpgsqlBatch(connection);
for (var i = 0; i < batchSize; i++)
{
    batch.BatchCommands.Add(new NpgsqlBatchCommand(sql)
    {
        Parameters =
        {
            new () {Value = "key" + i},
            new () {Value = "value" + i},
        }
    });
}
var updateCount = batch.ExecuteNonQuery();
```

#### Mixed Batch Example

```csharp
var sql = "INSERT INTO my_table (key, value) values ($1, $2)";
var batchSize = 5;
using var batch = new NpgsqlBatch(connection);
for (var i = 0; i < batchSize; i++)
{
    batch.BatchCommands.Add(new NpgsqlBatchCommand(sql)
    {
        Parameters =
        {
            new () {Value = "key" + i},
            new () {Value = "value" + i},
        }
    });
}
batch.BatchCommands.Add(new NpgsqlBatchCommand("select count(*) from my_table where key=$1")
{
    Parameters = { new () {Value = "key1"} }
});
batch.BatchCommands.Add(new NpgsqlBatchCommand("update my_table set value="updated_value" where key=$1")
{
    Parameters = { new () {Value = "key1"} }
});
using (var reader = batch.ExecuteReader())
{
    Console.WriteLine($"Inserted {reader.RecordsAffected} rows");
    while (reader.Read())
    {
        Console.WriteLine($"Found {reader.GetInt64(0)} rows");
    }
    // Move to the next result (the update statement).
    reader.NextResult();
    // npgsql returns the total number of rows affected for the entire batch until here.
    Console.WriteLine($"Inserted and updated {reader.RecordsAffected} rows");
}
```

#### DDL Batch Example

```csharp
using var command = new NpgsqlBatch(connection);
command.BatchCommands.Add(new NpgsqlBatchCommand(@"
    create table singers (
        id         varchar not null primary key,
        first_name varchar,
        last_name  varchar not null,
        full_name  varchar generated always as (coalesce(concat(first_name, ' '::varchar, last_name), last_name)) stored,
        active     boolean,
        created_at timestamptz,
        updated_at timestamptz
    )"));
command.BatchCommands.Add(new NpgsqlBatchCommand(@"
    create table albums (
        id               varchar not null primary key,
        title            varchar not null,
        marketing_budget numeric,
        release_date     date,
        cover_picture    bytea,
        singer_id        varchar not null,
        created_at       timestamptz,
        updated_at       timestamptz,
        constraint fk_albums_singers foreign key (singer_id) references singers (id)
    )"));
command.ExecuteNonQuery();
```