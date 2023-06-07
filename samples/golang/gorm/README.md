# PGAdapter and gorm

PGAdapter has Pilot Support for [gorm](https://gorm.io/) with the `pgx` driver. This document shows
how to use this sample application, and lists the limitations when working with `gorm` with PGAdapter.

The [sample.go](sample.go) file contains a sample application using `gorm` with PGAdapter. Use this as a reference for
features of `gorm` that are supported with PGAdapter. This sample assumes that the reader is familiar with `gorm`, and
it is not intended as a tutorial for how to use `gorm` in general.

## Pilot Support
Pilot Support means that `gorm` can be used with Cloud Spanner PostgreSQL databases, but with limitations.
Applications that have been developed with `gorm` for PostgreSQL will probably require modifications
before they can be used with Cloud Spanner PostgreSQL databases. It is possible to develop new
applications using `gorm` with Cloud Spanner PostgreSQL databases. These applications will also work
with PostgreSQL without modifications.

See [Limitations](#limitations) for a full list of limitations when working with `gorm`.

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
Run the following command in this directory. Replace the host, port and database name with the actual host, port and
database name for your PGAdapter and database setup.

```shell
psql -h localhost -p 5432 -d my-database -f create_data_model.sql
```

You can also drop an existing data model using the `drop_data_model.sql` script:

```shell
psql -h localhost -p 5432 -d my-database -f drop_data_model.sql
```

## Data Types
Cloud Spanner supports the following data types in combination with `gorm`.

| PostgreSQL Type                         | gorm / go type               |
|-----------------------------------------|------------------------------|
| boolean                                 | bool, sql.NullBool           |
| bigint / int8                           | int64, sql.NullInt64         |
| varchar                                 | string, sql.NullString       |
| text                                    | string, sql.NullString       |
| float8 / double precision               | float64, sql.NullFloat64     |
| numeric                                 | decimal.NullDecimal          |
| timestamptz / timestamp with time zone  | time.Time, sql.NullTime      |
| date                                    | datatypes.Date               |
| bytea                                   | []byte                       |


## Limitations
The following limitations are currently known:

| Limitation             | Workaround                                                                                                                                                                                                                                                         |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Migrations             | Cloud Spanner does not support the full PostgreSQL DDL dialect. Automated migrations using `gorm` are therefore not supported.                                                                                                                                     |
| Generated primary keys | Disable auto increment for primary key columns by adding the annotation `gorm:"primaryKey;autoIncrement:false"` to the primary key property.                                                                                                                       |
| OnConflict             | OnConflict clauses are not supported                                                                                                                                                                                                                               |
| Nested transactions    | Nested transactions and savepoints are not supported. It is therefore recommended to set the configuration option `DisableNestedTransaction: true,`                                                                                                                |
| Locking                | Lock clauses (e.g. `clause.Locking{Strength: "UPDATE"}`) are not supported. These are generally speaking also not required, as the default isolation level that is used by Cloud Spanner is serializable.                                                          |
| Auto-save associations | Auto saved associations are not supported, as these will automatically use an OnConflict clause                                                                                                                                                                    |
| Large CreateInBatches  | PGAdapter can handle at most 50 parameters in a prepared statement. A large number of rows in a `CreateInBatches` call can exceed this limit. Limit the batch size to a smaller number to prevent `gorm` from generating a statement with more than 50 parameters. |

### Migrations
Migrations are not supported as Cloud Spanner does not support the full PostgreSQL DDL dialect.
It is recommended to create the schema manually.
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
