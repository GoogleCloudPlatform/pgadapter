# PGAdapter and gorm

PGAdapter has not yet been extensively tested with `gorm` and is therefore currently not supported.

## Creating the Sample Data Model
Run the following command in this directory. Replace the host, port and database name with the actual host, port and
database name for your PGAdapter and database setup.

```shell
psql -h localhost -p 5432 -d my-database -f create_data_model.sql
```

## Limitations
The following limitations are currently known:

| Limitation             | Workaround                                                                                                                                   |
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| Migrations             | Cloud Spanner does not support the full PostgreSQL DDL dialect. Automated migrations using `gorm` are therefore not supported.               |
| Generated primary keys | Disable auto increment for primary key columns by adding the annotation `gorm:"primaryKey;autoIncrement:false"` to the primary key property. |
| Generated columns      | Generated columns require support for `RETURNING` clauses. That is currently not supported by Cloud Spanner.                                 |
| OnConflict             | OnConflict clauses are not supported                                                                                                         |
| Auto-save associations | Auto saved associations are not supported, as these will automatically use an OnConflict clause                                              |

### Generated Primary Keys
Generated primary keys are not supported and should be replaced with primary key definitions that
are manually assigned. See https://cloud.google.com/spanner/docs/schema-design#primary-key-prevent-hotspots
for more information on choosing a good primary key.

```go
type User struct {
	// Prevent gorm from using an auto-generated key.
	ID           int64 `gorm:"primaryKey;autoIncrement:false"`
	Name         string
}
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
