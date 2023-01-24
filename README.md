# Google Cloud Spanner MyAdater

## Usage
MyAdapter can be started both as a standalone process.

### Standalone with locally built jar
1. Build a jar file and assemble all dependencies by running

```shell
mvn package -P assembly
```

2. Execute (the binaries are in the target/pgadapter folder)
```shell
cd target/myadadpter
java -jar myadapter.jar -p my-project -i my-instance -d my-database
```

See [Options](#Options) for an explanation of all further options.

### Options

The following list contains the most frequently used startup options for PGAdapter.

```
-p <projectname>
  * The project name where the Spanner database(s) is/are running. If omitted, all connection
    requests must use a fully qualified database name in the format
    'projects/my-project/instances/my-instance/databases/my-database'.
    
-i <instanceid>
  * The instance ID where the Spanner database(s) is/are running. If omitted, all connection
    requests must use a fully qualified database name in the format
    'projects/my-project/instances/my-instance/databases/my-database'.

-d <databasename>
  * The default Spanner database name to connect to. This is only required if you want PGAdapter to
    ignore the database that is given in the connection request from the client and to always
    connect to this database.
  * If set, any database given in a connection request will be ignored. \c commands in psql will not
    change the underlying database that PGAdapter is connected to.
  * If not set, the database to connect to must be included in any connection request. \c commands
    in psql will change the underlying database that PGAdapter connects to.
