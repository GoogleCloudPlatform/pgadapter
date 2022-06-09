# DDL Options for PGAdapter

Cloud Spanner does not support DDL statements in transactions. It does however support DDL batches
that are parsed and verified as a single unit before being executed. The execution of the batch is
not guaranteed to be atomic. See [this reference](https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.admin.database.v1#updatedatabaseddlrequest)
for more details.

PGAdapter supports the `-ddl` command line argument to specify the behavior that PGAdapter should
use for DDL statements. This argument has the following possible values:

- [Single](#single): Disables all DDL batching and DDL statements in transactions.
  Only single DDL statements outside batches and transactions are allowed.
- [Batch](#batch-default): This is the default. Allows batches that contain only DDL statements outside
  explicit transactions. Does not allow mixing of DML, DQL and DDL statements in one batch.
- [AutocommitImplicitTransaction](#autocommit-implicit-transaction): DDL statements automatically
  commit the active implicit transaction. Allows batches that contain mixes of DML, DQL and DDL
  statements. Fails if a DDL statement is executed in an explicit transaction.
- [AutocommitExplicitTransaction](#autocommit-explicit-transaction): DDL statements automatically
  commit the active explicit or implicit transaction. Allows batches that contain mixes of DML, DQL
  and DDL statements. Explicit transaction blocks that contain DDL statements will not be atomic.

## Single
This is the safest but also most limiting option. It will ensure that PGAdapter will only allow
execution of single DDL statements outside of transactions. Each DDL statement will itself be atomic.
All attempts to execute a batch of DDL statements will return an error.

Example:

```shell
java -jar pgadapter.jar -p my-project -i my-instance -d my-database \
     -ddl=Single
```

## Batch (Default)
The default behavior of PGAdapter is to allow batches of DDL outside explicit transactions. The DDL
batch is not guaranteed to be atomic, and if a statement in the batch fails, earlier statements in
the same batch may have been applied to the database. DDL statements inside explicit transactions or
in batches that mix DDL statements with queries or update statements will return an error.

### Examples

The following batch is allowed, but is not guaranteed to be atomic.

```sql
create table singers (singer_id bigint primary key, name text);
create table albums (album_id bigint primary key, title text, singer_id bigint);
create index idx_singers_name on singers (name);
```

The following batches will return an error:

```sql
begin; -- DDL statements are not allowed in explicit transactions.
create table singers (singer_id bigint primary key, name text);
create table albums (album_id bigint primary key, title text, singer_id bigint);
create index idx_singers_name on singers (name);
commit;
```

```sql
create table singers (singer_id bigint primary key, name text);
insert into singers values (1, 'Some Name'); -- Mixing DDL and DML in a batch is not allowed
```

## Autocommit Implicit Transaction
This option instructs PGAdapter to allow DDL statements inside implicit transactions. This means
that a batch of statements that contain both DDL and other statements will be allowed. PGAdapter
will automatically commit the implicit transaction when it encounters a DDL statement.

PGAdapter will return an error if it encounters a DDL statement inside an explicit transaction block.

### Examples

Start PGAdapter with the following options:

```shell
java -jar pgadapter.jar -p my-project -i my-instance -d my-database \
     -ddl=AutocommitImplicitTransaction
```

The following batches are allowed, but are not guaranteed to be atomic.

```sql
create table singers (singer_id bigint primary key, name text);
create index idx_singers_name on singers (name);
```

```sql
create table singers (singer_id bigint primary key, name text);
-- The following statement starts a new implicit transaction.
insert into singers values (1, 'Name1');
insert into singers values (2, 'Name2');

-- The implicit transaction is automatically committed when the next DDL statement is executed.
-- The above insert statements will have been applied to the database, even if the following
-- DDL statement fails.
create table albums (album_id bigint primary key, title text, singer_id bigint);
```

The following batch will return an error:

```sql
begin; -- DDL statements are not allowed in explicit transactions.
create table singers (singer_id bigint primary key, name text);
create table albums (album_id bigint primary key, title text, singer_id bigint);
create index idx_singers_name on singers (name);
commit;
```

## Autocommit Explicit Transaction
This option instructs PGAdapter to allow DDL statements inside both implicit and explicit transactions.
This means that any combination of DDL statements and other statements will be allowed. Any DDL
statement will be treated as if it were pre-fixed with a `COMMIT` statement.

### Examples

Start PGAdapter with the following options:

```shell
java -jar pgadapter.jar -p my-project -i my-instance -d my-database \
     -ddl=AutocommitExplicitTransaction
```

The following batches are allowed, but are not guaranteed to be atomic.

```sql
create table singers (singer_id bigint primary key, name text);
create index idx_singers_name on singers (name);
```

```sql
create table singers (singer_id bigint primary key, name text);
-- The following statement starts a new implicit transaction.
insert into singers values (1, 'Name1');
insert into singers values (2, 'Name2');

-- The implicit transaction is automatically committed when the next DDL statement is executed.
-- The above insert statements will have been applied to the database, even if the following
-- DDL statement fails.
create table albums (album_id bigint primary key, title text, singer_id bigint);
```

```sql
begin; -- This starts an explicit transaction.
insert into singers values (1, 'Name1');
insert into singers values (2, 'Name2');

-- The DDL statement will automatically commit the explicit transaction.
create table albums (album_id bigint primary key, title text, singer_id bigint);
create index idx_singers_name on singers (name);
commit; -- This is a no-op, as there is no active transaction any more.
```
