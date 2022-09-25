# Frequently Asked Questions

The following are common questions when trying to use Liquibase with PGAdapter.

#### Why am I getting the following error: `ERROR: Type <timestamp> is not supported.`?
Liquibase requires a `databasechangelog` and `databasechangeloglock` table to work. These tables are
used by Liquibase to keep track of which changes have already been applied to the database.
Liquibase will automatically try to create these tables if they are not already present in your
database. The auto-generated table definitions will use the `timestamp [without time zone]` data
type that is not supported by Cloud Spanner. You therefore need to manually create these tables
before you can use Liquibase. The DDL statements for these tables can be found in
[create_database_change_log.sql](create_database_change_log.sql).

#### Why am I getting the following error: `ERROR: DDL statements are only allowed outside explicit transactions.`?
Liquibase will use one DDL transaction for each change set that is defined in your dbchangelog.xml
file. Cloud Spanner does not support DDL transactions. Instead, PGAdapter can automatically convert
DDL transactions to DDL batches. This option can be enabled by adding the following option to your
JDBC connection string: `options=-c%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction`.

An example of a full JDBC connection string that can be used for Liquibase when connecting to
PGAdapter is therefore:

```
jdbc:postgresql://localhost:5432/my-database?options=-c%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction
```

#### Why am I getting the following error: `ERROR: Setting a name of a <PRIMARY KEY> constraint is not supported.`
Liquibase will generate a primary key name if you do not specifically set one in your `createTable`
change definition. Cloud Spanner does not support primary keys with arbitrary names. Instead, the
name of a primary key will always be `pk_<table_name>`. You __must__ therefore always include a
primary key name in your change definition, and the name of the primary key constraint __must__ be
equal to `pk_<table_name>`. Example:

```xml
<createTable  tableName="singers">
  <column  name="singer_id"  type="varchar(36)">
    <!-- Setting the primary key name to 'pk_<table_name>' is required! -->
    <constraints
      primaryKey="true"
      primaryKeyName="pk_singers"
      nullable="false"/>
  </column>
  <column  name="first_name"  type="varchar(200)" />
  <column  name="last_name"  type="varchar(200)" />
</createTable>
```
