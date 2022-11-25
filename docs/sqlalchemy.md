# PGAdapter - SQLAlchemy Connection Options

## Limitations

- DDL: Must include `?options=-c spanner.ddl_transaction_mode=AutocommitExplicitTransaction` in the
  connection string.
- Savepoints are not supported
- INSERT .. ON CONFLICT is not supported
- Server side cursors are not supported
- Using the default Serializable isolation level is not recommended, as it will cause each query to
  start a new read/write transaction. Instead, it would be better to use either autocommit or read-
  committed.
- Stored procedures and user-defined functions are not supported
- 