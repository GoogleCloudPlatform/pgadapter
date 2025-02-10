<?php

// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

require '../vendor/autoload.php';
require '../samples/create_tables.php';
require '../samples/create_connection.php';
require '../samples/write_data_with_dml.php';
require '../samples/write_data_with_dml_batch.php';
require '../samples/write_data_with_copy.php';
require '../samples/query_data.php';
require '../samples/query_data_with_parameter.php';
require '../samples/statement_timeout.php';
require '../samples/add_column.php';
require '../samples/ddl_batch.php';
require '../samples/update_data_with_copy.php';
require '../samples/query_data_with_new_column.php';
require '../samples/update_data_with_transaction.php';
require '../samples/tags.php';
require '../samples/read_only_transaction.php';
require '../samples/data_boost.php';
require '../samples/partitioned_dml.php';

use PHPUnit\Framework\TestCase;
use Testcontainers\Container\Container;
use Testcontainers\Wait\WaitForLog;

class SamplesTest extends TestCase
{
    function test_run_samples(): void
    {
        [$pg_adapter, $port] = start_pg_adapter();
        $host = "localhost";
        $database = "test-database";

        ob_start();
        create_tables($host, $port, $database);
        $this->assertEquals("Created Singers & Albums tables in database: [test-database]\n", ob_get_clean());

        ob_start();
        create_connection($host, $port, $database);
        $this->assertEquals("Greeting from Cloud Spanner PostgreSQL: Hello world!\n", ob_get_clean());

        ob_start();
        write_data_with_dml($host, $port, $database);
        $this->assertEquals("4 records inserted\n", ob_get_clean());

        ob_start();
        write_data_with_dml_batch($host, $port, $database);
        $this->assertEquals("{1,1,1} records inserted\n", ob_get_clean());

        ob_start();
        write_data_with_copy($host, $port, $database);
        $this->assertEquals("Copied 5 singers\nCopied 5 albums\n", ob_get_clean());

        ob_start();
        query_data($host, $port, $database);
        $this->assertEquals("1\t1\tTotal Junk\n"
            ."1\t2\tGo, Go, Go\n"
            ."2\t1\tGreen\n"
            ."2\t2\tForever Hold Your Peace\n"
            ."2\t3\tTerrified\n",
            ob_get_clean());

        ob_start();
        query_data_with_parameter($host, $port, $database);
        $this->assertEquals("12\tMelissa\tGarcia\n", ob_get_clean());

        ob_start();
        query_with_timeout($host, $port, $database);
        $this->assertEquals("", ob_get_clean());

        ob_start();
        add_column($host, $port, $database);
        $this->assertEquals("Added marketing_budget column\n", ob_get_clean());

        ob_start();
        ddl_batch($host, $port, $database);
        $this->assertEquals("Added venues and concerts tables\n", ob_get_clean());

        ob_start();
        update_data_with_copy($host, $port, $database);
        $this->assertEquals("Updated 2 albums\n", ob_get_clean());

        ob_start();
        query_data_with_new_column($host, $port, $database);
        $this->assertEquals("1\t1\t100000\n"
            ."1\t2\t\n"
            ."2\t1\t\n"
            ."2\t2\t500000\n"
            ."2\t3\t\n",
            ob_get_clean());

        ob_start();
        update_data_with_transaction($host, $port, $database);
        $this->assertEquals("Transferred marketing budget from Album 2 to Album 1\n", ob_get_clean());

        ob_start();
        tags($host, $port, $database);
        $this->assertEquals("Reduced marketing budget\n", ob_get_clean());
        
        ob_start();
        read_only_transaction($host, $port, $database);
        $this->assertEquals(
            "1\t1\tTotal Junk\n"
            ."1\t2\tGo, Go, Go\n"
            ."2\t1\tGreen\n"
            ."2\t2\tForever Hold Your Peace\n"
            ."2\t3\tTerrified\n"
            ."2\t2\tForever Hold Your Peace\n"
            ."1\t2\tGo, Go, Go\n"
            ."2\t1\tGreen\n"
            ."2\t3\tTerrified\n"
            ."1\t1\tTotal Junk\n",
            ob_get_clean());

        ob_start();
        data_boost($host, $port, $database);
        $this->assertStringContainsString("17\tEthan\tMiller\n", ob_get_clean());

        ob_start();
        execute_partitioned_dml($host, $port, $database);
        $this->assertEquals("Updated at least 3 albums\n", ob_get_clean());
        
        $pg_adapter->stop();
    }
    
}

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
