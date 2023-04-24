# Command Line Arguments

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

-c <credentialspath>
  * This argument should not be used in combination with -a (authentication mode).
  * This is only required if you have not already set up default credentials on the system where you
    are running PGAdapter. See https://cloud.google.com/spanner/docs/getting-started/set-up#set_up_authentication_and_authorization
    for more information on setting up authentication for Cloud Spanner.
  * The full path for the file containing the service account credentials in JSON format.
  * Do remember to grant the service account sufficient credentials to access the database. See
    https://cloud.google.com/docs/authentication/production for more information.

-s <port>
  * The inbound port for the proxy. Defaults to 5432. Choose a different port if you already have
    PostgreSQL running on your local system.

-dir <socket-file-directory>
  * This proxy's domain socket directory. Defaults to '/tmp'.
    Note: Some distributions of PostgreSQL and psql use '/var/run/postgresql' as the
    default directory for Unix domain socket file names. When connecting to PGAdapter
    using psql from one of these distributions, you either need to use 'psql -h /tmp'
    or change the default Unix domain socket directory used by PGAdapter to '/var/run/postgresql'.

-ddl <ddl-transaction-mode>
  * Sets the way PGAdapter should handle DDL statements in implicit and explicit
    transactions. Cloud Spanner does not support DDL transactions. The possible modes are:
  * Single: Only allows single DDL statements outside implicit and explicit transactions.
  * Batch: Allows batches that contain only DDL statements. Does not allow mixed batches of DDL and
    other statements, or DDL statements in transactions. This is the default.
  * AutocommitImplicitTransaction: Allows mixed batches of DDL and other statements.
    Automatically commits the implicit transaction when a DDL statement is encountered in a batch.
    DDL statements in explicit transactions are not allowed.
  * AutocommitExplicitTransaction: Allows mixed batches of DDL and other statements.
    Automatically commits the current transaction when a DDL statement is encountered.
    Automatically converts DDL transactions to DDL batches. This setting is recommended when using
    schema management tools like Liquibase.

-a
  * Turns on authentication for the proxy server. Clients are then requested
    to supply a username and password during a connection request.
  * The username and password must contain one of the following:
    1. The password field must contain the JSON payload of a credentials file, for example from a service account key file. The username will be ignored in this case.
    2. The password field must contain the private key from a service account key file. The username must contain the email address of the corresponding service account.
  * Note that SSL is not supported for the connection between the client and
    PGAdapter. The proxy should therefore only be used within a private network.
    The connection between PGAdapter and Cloud Spanner is always secured with SSL.

-x
  * PGAdapter by default only allows TCP connections from localhost or Unix Domain Socket connections.
    Use the -x switch to turn off the localhost check. This is required when running PGAdapter in a
    Docker container, as the connections from the host machine will not be seen as a connection from
    localhost in the container.

-r
  * PGAdapter internally uses the core engine of the Cloud Spanner JDBC driver. This option specifies
    additional properties that will be used with that internal JDBC connection. All connection properties
    that are supported by the Cloud Spanner JDBC driver can also be used with this option.
    They should be in the format <key1>=<value1>;<key2>=<value2>;...
  * Example: -r minSessions=500;maxSessions=1000;numChannels=10

-v
  * This option specifies what server_version PGAdapter should claim to be. If not specified
    it will default to version 1.0.0.
  * Be careful when changing this value. Unless otherwise specified, all clients and drivers that
    have been tested with PGAdapter have been tested using the default value for this option. Changing
    the value of this option could cause a client or driver to alter its behavior and cause unexpected
    errors when used with PGAdapter.

-max_backlog <maximum number of backlog connections>
  * Maximum queue length of incoming pending connections. Defaults to 1000.

-e <endpoint>
  * The Cloud Spanner endpoint that PGAdapter should connect to. Defaults to https://spanner.googleapis.com.

-disable_auto_detect_client
  * PGAdapter will automatically try to detect the type of client that is connecting (e.g. psql or JDBC).
    This detection is used to re-write specific metadata queries and commands that use pg_catalog tables
    that are currently not supported on Cloud Spanner. Set this option to disable the automatic detection
    of the client that is connecting to PGAdapter.

-disable_default_local_statements
  * PGAdapter will execute some specific statements without a round-trip to Cloud Spanner. This is done
    for statements that are currently not supported by Cloud Spanner, such as `select current_schema()`.
    Set this option to disable this local execution. This will cause unsupported statements to fail.

-debug
  * Enables debug mode for PGAdapter and is only intended for testing. Do not use this in production.

-j <commandmetadatapath>
  * The full path for a file containing a JSON object to do SQL translation
    based on RegEx replacement. Any item matching the input_pattern will be
    replaced by the output_pattern string, wherein capture groups are allowed and
    their order is specified via the matcher_array item. Match replacement must be
    defined via %s in the output_pattern string. Set matcher_array to [] if no
    matches exist. Alternatively, you may place the matching group names 
    directly within the output_pattern string using matcher.replaceAll() rules
    (that is to say, placing the item within braces, preceded by a dollar sign);
    For this specific case, matcher_array must be left empty. User-specified 
    patterns will precede internal matches. Escaped and general regex syntax 
    matches Java RegEx syntax; more information on the Java RegEx syntax found 
    here: 
    https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
    * Example:
        { 
          "commands": 
            [ 
              {
                "input_pattern": "^SELECT * FROM users;$", 
                "output_pattern": "SELECT 1;",
                "matcher_array": []
              }, 
              {
                "input_pattern": "^ab(?<firstgroup>\\d)(?<secondgroup>\\d)$",
                "output_pattern": "second number: %s, first number: %s",
                "matcher_array": ["secondgroup", "firstgroup"] 
              },
              {
                "input_pattern": "^cd(?<firstgroup>\\d)(?<secondgroup>\\d)$",
                "output_pattern": "second number: ${secondgroup}, first number: ${firstgroup}",
                "matcher_array": [] 
              }
            ]
        }
        
        
        Input queries:
        "SELECT * FROM users;"
        "ab12"
        "cd34"

        Output queries:
        "SELECT 1;"
        "second number: 2, first number: 1"
        "second number: 4, first number: 3"

```

## Deprecated Command Line Arguments

The following command line arguments are deprecated and should no longer be used.

```
-q
  * This option turns on PSQL mode. This mode allows better compatibility to PSQL, with an
    added performance cost. PSQL mode is implemented using predefined dynamic matchers
    and as such cannot be used with the option -j. This mode should not be used for
    production, and we do not guarantee its functionality beyond the basics.
  * DEPRECATED: PGAdapter will automatically detect connections from psql.

-jdbc
  * This option turns on JDBC mode. This mode allows better compatibility with the
    PostgreSQL JDBC driver. It will automatically inspect incoming queries to look for
    known JDBC metadata queries, and replace these with queries that are compatible with
    Cloud Spanner. JDBC mode is implemented using predefined fixed matchers and should
    not be used in combination with options -q (psql mode) or -j (custom matchers). It
    should be enabled if you intend to connect to PGAdapter using the PostgreSQL JDBC
    driver.
  * DEPRECATED: PGAdapter will automatically detect connections from JDBC.

-b
  * Force the server to send data back in binary PostgreSQL format when no
    specific format has been requested. The PostgreSQL wire protocol specifies 
    that the server should send data in text format in those cases. This 
    setting overrides this default and should be used with caution, for example
    for testing purposes, as clients might not accept this behavior. This 
    setting only affects query results in extended query mode. Queries in 
    simple query mode will always return results in text format. If you do not 
    know what extended query mode and simple query mode is, then you should 
    probably not be using this setting.
  * DEPRECATED: This option violates the PostgreSQL protocol. Using this option
    with standard PostgreSQL clients can lead to unexpected errors.
```
