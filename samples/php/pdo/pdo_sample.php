<?php

/*
 * Sample for connecting to PGAdapter with the PostgreSQL PDO driver.
 *
 * This sample application shows how to connect to PGAdapter and Cloud Spanner
 * using the PostgreSQL PDO driver. The sample starts PGAdapter in an embedded
 * Docker container and then connects through this container to the Cloud Spanner
 * emulator or real Cloud Spanner.
 *
 * The sample uses the Cloud Spanner emulator by default.
 *
 * This sample requires Docker to be installed on the local environment.
 *
 * Usage (emulator):
 *   php pdo_sample.php
 *
 * Usage (Cloud Spanner):
 *   php pdo_sample.py -p my-project -i my-instance -d my-database
*/

require 'vendor/autoload.php';

use Testcontainers\Container\Container;
use Testcontainers\Wait\WaitForLog;

// Start PGAdapter+Emulator in a Docker container.
[$pg_adapter, $port] = start_pg_adapter();

// Connect to PGAdapter using the PostgreSQL PDO driver.
$dsn = sprintf("pgsql:host=localhost;port=%s;dbname=test", $port);
$connection = new PDO($dsn);

// Execute a query on Spanner through PGAdapter.
$statement = $connection->query("SELECT 'Hello World!' as hello");
$rows = $statement->fetchAll();

echo sprintf("Greeting from Cloud Spanner PostgreSQL: %s\n", $rows[0][0]);

// Cleanup resources.
$rows = null;
$statement = null;
$connection = null;

// Starts PGAdapter+Emulator in a Docker test container. This setup is useful for development and test
// purposes, but should not be used in production, as the host-to-Docker network bridge can be slow.
// The recommended setup in production for PGAdapter is to run it as a side-car container or directly
// on the host as a Java application. See https://cloud.google.com/spanner/docs/pgadapter-start#run-pgadapter
// for more information.
function start_pg_adapter(): array
{
    // The 'pgadapter-emulator' Docker image is a combined Docker image of PGAdapter + the Spanner emulator.
    // This Docker image automatically creates any Spanner instance and database that you try to connect to,
    // which means that you don't have to manually create the instance/database before connecting to it.
    $pg_adapter = Container::make("gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator");
    // Map port '5432' in the container to a random port on the host.
    $pg_adapter->withPort("0", "5432");
    $pg_adapter->withWait(new WaitForLog('Server started on port'));
    $pg_adapter->run();

    // Get the mapped host port of port '5432' in the container and use that port number to connect
    // to PGAdapter using the PHP PDO driver.
    $reflected_pg_adapter = new ReflectionObject($pg_adapter);
    $inspected_data = $reflected_pg_adapter->getProperty('inspectedData');
    $inspected_data->setAccessible(true);
    $ports = $inspected_data->getValue($pg_adapter)[0]['NetworkSettings']['Ports'];
    $port = $ports['5432/tcp'][0]['HostPort'];

    return [$pg_adapter, $port];
}