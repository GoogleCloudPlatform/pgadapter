# PGAdapter and SQLAlchemy

PGAdapter has experimental support for [SQLAlchemy 1.4](https://docs.sqlalchemy.org/en/14/index.html)
with the `psycopg2` driver. This document shows how to use this sample application, and lists the
limitations when working with `SQLAlchemy` with PGAdapter.

The [sample.py](sample.py) file contains a sample application using `SQLAlchemy` with PGAdapter. Use
this as a reference for features of `SQLAlchemy` that are supported with PGAdapter. This sample
assumes that the reader is familiar with `SQLAlchemy`, and it is not intended as a tutorial for how
to use `SQLAlchemy` in general.

See [Limitations](#limitations) for a full list of known limitations when working with `SQLAlchemy`.

## Start PGAdapter
You must start PGAdapter before you can run the sample. The following command shows how to start PGAdapter using the
pre-built Docker image. See [Running PGAdapter](../../../README.md#usage) for more information on other options for how
to run PGAdapter.

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  -v /tmp:/tmp \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -x
```

## Creating the Sample Data Model
The sample data model contains example tables that cover all supported data types the Cloud Spanner
PostgreSQL dialect. It also includes an example for how [interleaved tables](https://cloud.google.com/spanner/docs/reference/postgresql/data-definition-language#extensions_to)
can be used with SQLAlchemy. Interleaved tables is a Cloud Spanner extension of the standard
PostgreSQL dialect.

The corresponding SQLAlchemy model is defined in [model.py](model.py).

Run the following command in this directory. Replace the host, port and database name with the actual
host, port and database name for your PGAdapter and database setup.

```shell
psql -h localhost -p 5432 -d my-database -f create_data_model.sql
```

You can also drop an existing data model using the `drop_data_model.sql` script:

```shell
psql -h localhost -p 5432 -d my-database -f drop_data_model.sql
```

## Data Types
Cloud Spanner supports the following data types in combination with `SQLAlchemy`.

| PostgreSQL Type                         | SQLAlchemy type         |
|-----------------------------------------|-------------------------|
| boolean                                 | Boolean                 |
| bigint / int8                           | Integer, BigInteger     |
| varchar                                 | String                  |
| text                                    | String                  |
| float8 / double precision               | Float                   |
| numeric                                 | Numeric                 |
| timestamptz / timestamp with time zone  | DateTime(timezone=True) |
| date                                    | Date                    |
| bytea                                   | LargeBinary             |


## Limitations
The following limitations are currently known:

| Limitation                   | Workaround                                                                                                                                                                                                                                                         |
|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Creating and Dropping Tables | Cloud Spanner does not support the full PostgreSQL DDL dialect. Automated creation of tables using `SQLAlchemy` is therefore not supported.                                                                                                                        |
| Generated primary keys       | Manually assign a value to the primary key column in your code. The recommended primary key type is a random UUID. Sequences / SERIAL / IDENTITY columns are currently not supported.                                                                              |
| INSERT ... ON CONFLICT       | INSERT ... ON CONFLICT is not supported                                                                                                                                                                                                                            |
| SAVEPOINT                    | Nested transactions and savepoints are not supported.                                                                                                                                                                                                              |
| Server side cursors          | Server side cursors are currently not supported.                                                                                                                                                                                                                   |
| Transaction isolation level  | Only SERIALIZABLE and AUTOCOMMIT are supported. `postgresql_readonly=True` is also supported.                                                                                                                                                                      |
| Large CreateInBatches        | PGAdapter can handle at most 50 parameters in a prepared statement. A large number of rows in a `CreateInBatches` call can exceed this limit. Limit the batch size to a smaller number to prevent `gorm` from generating a statement with more than 50 parameters. |

### Migrations
Migrations are not supported as Cloud Spanner does not support the full PostgreSQL DDL dialect. It is recommended to
create the schema manually. Note that PGAdapter does support `create table if not exists` / `drop table if exists`.
See [create_data_model.sql](create_data_model.sql) for the data model for this example.

### Generated Primary Keys
Generated primary keys are not supported and should be replaced with primary key definitions that
are manually assigned. See https://cloud.google.com/spanner/docs/schema-design#primary-key-prevent-hotspots
for more information on choosing a good primary key. This sample uses UUIDs that are generated by the client for primary
keys.

```go
type User struct {
	// Prevent gorm from using an auto-generated key.
	ID           int64 `gorm:"primaryKey;autoIncrement:false"`
	Name         string
}
```

### Generated Columns
Generated columns can be used, but Cloud Spanner does not support the `RETURNING` keyword. This means that `gorm` is not
able to get the value of the generated column directly after it has been updated.

```go
// FullName is generated by the database. The '->' marks this a read-only field. Preferably this field should also
// include a `default:(-)` annotation, as that would make gorm read the value back using a RETURNING clause. That is
// however currently not supported.
FullName string `gorm:"->;type:GENERATED ALWAYS AS (coalesce(concat(first_name,' '::varchar,last_name))) STORED;"`
```

### OnConflict Clauses
`OnConflict` clauses are not supported by Cloud Spanner and should not be used. The following will
therefore not work.

```go
user := User{
    ID:   1,
    Name: "User Name",
}
// OnConflict is not supported and this will return an error.
db.Clauses(clause.OnConflict{DoNothing: true}).Create(&user)
```

### Auto-save Associations
Auto-saving associations will automatically use an `OnConflict` clause in gorm. These are not
supported. Instead, the parent entity of the association must be created before the child entity is
created.

```go
blog := Blog{
    ID:     1,
    Name:   "",
    UserID: 1,
    User: User{
        ID:   1,
        Name: "User Name",
    },
}
// This will fail, as the insert statement for User will use an OnConflict clause.
db.Create(&blog).Error
```

Instead, do the following:

```go
user := User{
    ID:   1,
    Name: "User Name",
    Age:  20,
}
blog := Blog{
    ID:     1,
    Name:   "",
    UserID: 1,
}
db.Create(&user)
db.Create(&blog)
```

### Nested Transactions
`gorm` uses savepoints for nested transactions. Savepoints are currently not supported by Cloud Spanner. Nested
transactions can therefore not be used with PGAdapter. It is recommended to set the configuration option
`DisableNestedTransactions: true` to be sure that `gorm` does not try to use a nested transaction.

```go
db, err := gorm.Open(postgres.Open(connectionString), &gorm.Config{
    DisableNestedTransaction: true,
})
```

### Locking
Locking clauses, like `clause.Locking{Strength: "UPDATE"}`, are not supported. These are generally speaking also not
required, as Cloud Spanner uses isolation level `serializable` for read/write transactions.
