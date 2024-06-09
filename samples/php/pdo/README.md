<meta name='keywords' content='pgadapter, php, pdo, spanner, cloud spanner, emulator'>

# PGAdapter Spanner and PHP PDO

PGAdapter has experimental support for the [PostgreSQL PHP PDO driver](https://www.php.net/manual/en/ref.pdo-pgsql.php).
This sample application shows how to connect to PGAdapter with PHP PDO, and how to execute a simple query on Spanner.

The sample uses the Spanner emulator. You can run the sample on the emulator with this command:

```shell
php pdo_sample.php
```

PGAdapter and the emulator are started in a Docker test container by the sample application.
Docker is therefore required to be installed on your system to run this sample.
