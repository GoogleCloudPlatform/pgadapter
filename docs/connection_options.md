# Connection Options

PGAdapter can be configured to always connect to a single Cloud Spanner database, or it
can be configured to allow connections to different databases. This is controlled through
the `-p <project-id>`, `-i <instance-id>` and `-d <database-id>` command line arguments.

## Always Connect to a Single Database
PGAdapter will always connect to the database that is specified in the command line if all
the options `-p <project-id>`, `-i <instance-id>` and `-d <database-id>` have been set. Any
database name in a connection request will be ignored. The `\c` psql meta-command will have
no effect.

## Set a Default Instance
If the command line arguments `-p <project-id>` and `-i <instance-id>` have been set,
PGAdapter will consider this the default instance where to look for a database. If a
database request contains only a database id (not the fully qualified database name),
PGAdapter will try to connect to the database with the given id on the default instance.

## No Connection Options
Connection requests must contain the fully qualified database name if none of the options
`-p <project-id>`, `-i <instance-id>` and `-d <database-id>` have been set. Fully qualified
database names have the form `projects/my-project/instances/my-instance/databases/my-database`.

__NOTE__: Fully qualified database names contain multiple slashes. These must be URL-encoded
when used in for example JDBC connection strings.

## Credentials
The `-c <credentialspath>` command line arguments specifies the fixed credentials that should be
used for all connections from PGAdapter to Cloud Spanner. See [authentication](authentication.md)
for more authentication options, including options to use different credentials for different
connections.

## Examples

### Fixed Database

```shell
# Start PGAdapter with a fixed database 
java -jar pgadapter.jar -p my-project -i my-instance -d my-database

# No need to specify a database id in a connection request
psql -h /tmp

# PGAdapter will ignore the 'some-other-database' name in this connection request and instead
# connect to 'my-database'.
psql -h /tmp -d some-other-database
```

### Default Instance

```shell
# Start PGAdapter with a default instance but no fixed database. 
java -jar pgadapter.jar -p my-project -i my-instance

# We must specify a database in the connection request. The id is enough for databases
# on the default instance.
psql -h /tmp -d my-database

# Use a fully qualified database name to connect to a database on a different instance.
psql -h /tmp -d projects/other-project/instances/other-instance/databases/other-database
```

