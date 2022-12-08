# PGAdapter - gorm Connection Options

PGAdapter has Pilot Support for [gorm](https://gorm.io) version v1.23.8 and higher.

## Limitations
Pilot Support means that it is possible to use `gorm` with Cloud Spanner PostgreSQL databases, but
with limitations. This means that porting an existing application from PostgreSQL to Cloud Spanner
will probably require code changes. See [Limitations](../samples/golang/gorm/README.md#limitations)
in the `gorm` sample directory for a full list of limitations.

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

Then connect to PGAdapter and use `gorm` like this:

```go
db, err := gorm.Open(postgres.Open("host=localhost port=5432 database=gorm-sample"), &gorm.Config{
    // DisableNestedTransaction will turn off the use of Savepoints if gorm
    // detects a nested transaction. Cloud Spanner does not support Savepoints,
    // so it is recommended to set this configuration option to true.
    DisableNestedTransaction: true,
    Logger: logger.Default.LogMode(logger.Error),
})
if err != nil {
  fmt.Printf("Failed to open gorm connection: %v\n", err)
}
tx := db.Begin()
user := User{
    ID:        1,
    Name:      "User Name",
    Age:       20,
}
res := tx.Create(&user)
```

## Full Sample and Limitations
[This directory](../samples/golang/gorm) contains a full sample of how to work with `gorm` with
Cloud Spanner and PGAdapter. The sample readme file also lists the [current limitations](../samples/golang/gorm)
when working with `gorm`.
