# PGAdapter and gorm

PGAdapter supports [gorm](https://gorm.io/) with the `pgx` driver. This document shows
how to use this sample application, and lists the limitations when working with `gorm` with PGAdapter.

The [sample.go](sample.go) file contains a sample application using `gorm` with PGAdapter. Use this as a reference for
features of `gorm` that are supported with PGAdapter. This sample assumes that the reader is familiar with `gorm`, and
it is not intended as a tutorial for how to use `gorm` in general.

The sample is by default executed using the Cloud Spanner emulator. You can run the sample on the emulator with this
command:

```shell
go run sample.go
```

You can also run the sample application on a real Cloud Spanner PostgreSQL database with this command:

```shell
go run sample.go -project my-project -instance my-instance -database my-database
```

Replace the project, instance, and database with your Cloud Spanner PostgreSQL database. The sample will automatically
create the required tables for this sample.

## Support Level
`gorm` can be used with Cloud Spanner PostgreSQL databases, but with limitations.
Applications that have been developed with `gorm` for PostgreSQL will probably require modifications
before they can be used with Cloud Spanner PostgreSQL databases. It is possible to develop new
applications using `gorm` with Cloud Spanner PostgreSQL databases. These applications will also work
with PostgreSQL without modifications.

See [Limitations](#limitations) for a full list of limitations when working with `gorm`.

## PGAdapter Docker
PGAdapter is started in a Docker test container by the sample application. Docker is therefore required to be installed
on your system to run this sample.

## Open Source PostgreSQL
This sample can also be executed on open-source PostgreSQL. PostgreSQL is started in a Docker test container by the
sample application. Use this command to run the sample application on open-source PostgreSQL:

```shell
go run sample.go -postgres=true
```

## Data Types
Cloud Spanner supports the following data types in combination with `gorm`.

| PostgreSQL Type                            | gorm / go type                      |
|--------------------------------------------|-------------------------------------|
| boolean                                    | bool, sql.NullBool                  |
| bigint / int8                              | int64, sql.NullInt64                |
| varchar                                    | string, sql.NullString              |
| text                                       | string, sql.NullString              |
| float8 / double precision                  | float64, sql.NullFloat64            |
| numeric                                    | decimal.NullDecimal                 |
| timestamptz / timestamp with time zone     | time.Time, sql.NullTime             |
| date                                       | datatypes.Date                      |
| bytea                                      | []byte                              |
| jsonb                                      | string                              |
| bool[]                                     | pq.BoolArray, pgtype.BoolArray      |
| bigint[]                                   | pq.Int64Array, pgtype.Int8Array     |
| varchar[] / text[]                         | pq.StringArray, pgtype.TextArray    |
| float8[] / double precision[]              | pq.Float64Array, pgtype.Float8Array |
| numeric[]                                  | pgtype.NumericArray                 |
| timestamptz[] / timestamp with time zone[] | pgtype.TimestamptzArray             |
| date[]                                     | pgtype.DateArray                    |
| bytea[]                                    | pgtype.ByteaArray                   |
| jsonb[]                                    | pgtype.JSONBArray                   |


## Limitations
The following limitations are currently known:

| Limitation             | Workaround                                                                                                                                                                                                                                                         |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Migrations             | Cloud Spanner does not support the full PostgreSQL DDL dialect. Automated migrations using `gorm` are therefore not supported.                                                                                                                                     |
| OnConflict             | OnConflict clauses are not supported                                                                                                                                                                                                                               |
| Nested transactions    | Nested transactions and savepoints are not supported. It is therefore recommended to set the configuration option `DisableNestedTransaction: true,`                                                                                                                |
| Locking                | Lock clauses (e.g. `clause.Locking{Strength: "UPDATE"}`) are not supported. These are generally speaking also not required, as the default isolation level that is used by Cloud Spanner is serializable.                                                          |
| Auto-save associations | Auto saved associations are not supported, as these will automatically use an OnConflict clause                                                                                                                                                                    |

### Migrations
Migrations are not supported as Cloud Spanner does not support the full PostgreSQL DDL dialect.
It is recommended to create the schema manually.
See [create_data_model.sql](create_data_model.sql) for the data model for this example.

### Generated Primary Keys
Cloud Spanner supports bit-reversed sequences. These work as regular sequences, except that the value is bit-reversed
before being returned to the user. That makes these values safe for use as a primary key in Cloud Spanner, and these
will not cause hot-spotting. You can use the standard `gorm.Model` in combination with bit-reversed sequences.

Example model definition:

```go
type TicketSale struct {
	gorm.Model
	Concert      Concert
	ConcertId    string
	CustomerName string
	Price        decimal.Decimal
	Seats        pq.StringArray `gorm:"type:text[]"`
}
```

Corresponding table and sequence definition:

```sql
create sequence if not exists ticket_sale_seq
    bit_reversed_positive
    skip range 1 1000
    start counter with 50000
;

create table if not exists ticket_sales (
    id bigint not null primary key default nextval('ticket_sale_seq'),
    concert_id       varchar not null,
    customer_name    varchar not null,
    price            decimal not null,
    seats            text[],
    created_at       timestamptz,
    updated_at       timestamptz,
    deleted_at       timestamptz,
    constraint fk_ticket_sales_concerts foreign key (concert_id) references concerts (id)
);
```

See also https://cloud.google.com/spanner/docs/reference/postgresql/data-definition-language#create_sequence

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
