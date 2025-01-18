<?php

$method = $argv[1];
$host = $argv[2];
$port = $argv[3];
$dsn = sprintf("pgsql:host=%s;port=%s;dbname=test", $host, $port);
$method($dsn);

function select1($dsn): void
{
    // Connect to PGAdapter using the PostgreSQL PDO driver.
    $connection = new PDO($dsn);

    // Execute a query on Spanner through PGAdapter.
    $statement = $connection->query("SELECT 1");
    $rows = $statement->fetchAll();

    echo sprintf("Result: %s\n", $rows[0][0]);

    // Cleanup resources.
    $rows = null;
    $statement = null;
    $connection = null;
}

function show_server_version($dsn): void
{
    $connection = new PDO($dsn);
    $statement = $connection->query("show server_version");
    $rows = $statement->fetchAll();

    print_r($rows);

    $rows = null;
    $statement = null;
    $connection = null;
}

function show_application_name($dsn): void
{
    $connection = new PDO($dsn);
    $statement = $connection->query("show application_name");
    $rows = $statement->fetchAll();

    print_r($rows);

    $rows = null;
    $statement = null;
    $connection = null;
}

function query_with_parameter($dsn): void
{
    $connection = new PDO($dsn);
    $statement = $connection->prepare("SELECT * FROM FOO WHERE BAR=:bar");
    $statement->execute(["bar" => "baz"]);
    $rows = $statement->fetchAll();

    print_r($rows);

    $rows = null;
    $statement = null;
    $connection = null;
}

function query_with_parameter_twice($dsn): void
{
    $connection = new PDO($dsn);
    $values = ["baz", "foo"];
    foreach ($values as $value) {
        $statement = $connection->prepare("SELECT * FROM FOO WHERE BAR=:bar");
        $statement->execute(["bar" => $value]);
        $rows = $statement->fetchAll();

        print_r($rows);

        $rows = null;
        $statement = null;
    }
    $connection = null;
}

function query_all_data_types($dsn): void
{
    $connection = new PDO($dsn);
    $statement = $connection->query("SELECT * FROM all_types WHERE col_bigint=1");
    $rows = $statement->fetchAll();

    print_r($rows);

    $rows = null;
    $statement = null;
    $connection = null;
}

function query_all_data_types_with_parameter($dsn): void
{
    $connection = new PDO($dsn);
    $statement = $connection->prepare("SELECT * FROM all_types WHERE col_bigint=:id");
    $statement->execute(["id" => 1]);
    $rows = $statement->fetchAll();

    print_r($rows);

    $rows = null;
    $statement = null;
    $connection = null;
}

function update_all_data_types($dsn): void
{
    $connection = new PDO($dsn);
    $statement = $connection->prepare(
        "UPDATE all_types SET col_bool=:bool, col_bytea=:bytea, col_float4=:float4, 
                     col_float8=:float8, col_int=:int, col_numeric=:numeric, col_timestamptz=:timestamptz, 
                     col_date=:date, col_varchar=:varchar, col_jsonb=:jsonb WHERE col_varchar = :filter");
    $statement->bindValue("bool", true, PDO::PARAM_BOOL);
    $statement->bindValue("bytea", "test_bytes", PDO::PARAM_LOB);
    $statement->bindValue("float4", 3.14);
    $statement->bindValue("float8", 3.14);
    $statement->bindValue("int", 1);
    $statement->bindValue("numeric", "6.626");
    $statement->bindValue("timestamptz", "2022-03-24T06:39:10.123456Z");
    $statement->bindValue("date", "2022-03-24");
    $statement->bindValue("varchar", "test_string");
    $statement->bindValue("jsonb", '{"key": "value"}');
    $statement->bindValue("filter", "test");
    $statement->execute();
    $rowCount = $statement->rowCount();

    printf("Update count: %s\n", $rowCount);

    $statement = null;
    $connection = null;
}

function insert_all_data_types($dsn): void
{
    $connection = new PDO($dsn);
    $statement = $connection->prepare(
        "INSERT INTO all_types
        (col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int,
        col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    bind_insert_statement(100, $statement);
    $statement->execute();
    $rowCount = $statement->rowCount();

    printf("Insert count: %s\n", $rowCount);

    $statement = null;
    $connection = null;
}

function insert_nulls_all_data_types($dsn): void
{
    $connection = new PDO($dsn);
    $statement = $connection->prepare(
        "INSERT INTO all_types
        (col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int,
        col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    $statement->bindValue(1, 100);
    $statement->bindValue(2, null);
    $statement->bindValue(3, null);
    $statement->bindValue(4, null);
    $statement->bindValue(5, null);
    $statement->bindValue(6, null);
    $statement->bindValue(7, null);
    $statement->bindValue(8, null);
    $statement->bindValue(9, null);
    $statement->bindValue(10, null);
    $statement->bindValue(11, null);
    $statement->execute();
    $rowCount = $statement->rowCount();

    printf("Insert count: %s\n", $rowCount);

    $statement = null;
    $connection = null;
}

function text_copy_in($dsn): void
{
    $connection = new PDO($dsn);
    $result = $connection->pgsqlCopyFromArray("all_types", [
        "1\ttrue\t\\x6538306638626334343565636366626135616363333764333163323537323133\t3.14\t3.14\t10\t6.626\t2022-03-24T06:39:10.123456Z\t2022-03-24\ttest_string\t{\"key\": \"value\"}\n" .
        "2\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\n"
    ]);

    printf("Copy result: %s\n", $result);

    $connection = null;
}

function text_copy_out($dsn): void
{
    $connection = new PDO($dsn);
    $result = $connection->pgsqlCopyToArray("all_types");

    print_r($result);

    $connection = null;
}

function read_write_transaction($dsn): void
{
    $connection = new PDO($dsn);
    $connection->beginTransaction();
    
    $statement = $connection->query("SELECT 1");
    $rows = $statement->fetchAll();
    print_r($rows);
    $rows = null;
    
    $statement = $connection->prepare(
        "INSERT INTO all_types
        (col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int,
        col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    bind_insert_statement(10, $statement);
    $statement->execute();
    $rowCount = $statement->rowCount();
    printf("Insert count: %s\n", $rowCount);

    bind_insert_statement(20, $statement);
    $statement->execute();
    $rowCount = $statement->rowCount();
    printf("Insert count: %s\n", $rowCount);
    
    $connection->commit();

    $statement = null;
    $connection = null;
}

function read_only_transaction($dsn): void
{
    $connection = new PDO($dsn);
    $connection->beginTransaction();
    $connection->exec("set transaction read only");

    $statement = $connection->query("SELECT 1");
    $rows = $statement->fetchAll();
    print_r($rows);
    $rows = null;

    $statement = $connection->query("SELECT 2");
    $rows = $statement->fetchAll();
    print_r($rows);
    $rows = null;

    $connection->commit();

    $statement = null;
    $connection = null;
}

function bind_insert_statement($id, $statement): void
{
    $statement->bindValue(1, $id);
    $statement->bindValue(2, true);
    $statement->bindValue(3, "test_bytes");
    $statement->bindValue(4, 3.14);
    $statement->bindValue(5, 3.14);
    $statement->bindValue(6, 1);
    $statement->bindValue(7, "6.626");
    $statement->bindValue(8, "2022-03-24T06:39:10.123456Z");
    $statement->bindValue(9, "2022-03-24");
    $statement->bindValue(10, "test_string");
    $statement->bindValue(11, '{"key": "value"}');
}

function batch_dml($dsn): void
{
    $connection = new PDO($dsn);
    $connection->exec("START BATCH DML");
    $statement = $connection->prepare("insert into my_table (id, value) values (:id, :value)");
    $statement->execute(["id" => 1, "value" => "One"]);
    $statement->execute(["id" => 2, "value" => "Two"]);
    $connection->exec("RUN BATCH");

    print("Inserted two rows\n");

    $statement = null;
    $connection = null;
}

function batch_dml_in_transaction($dsn): void
{
    $connection = new PDO($dsn);
    $connection->beginTransaction();
    $connection->exec("START BATCH DML");
    $statement = $connection->prepare("insert into my_table (id, value) values (:id, :value)");
    $statement->execute(["id" => 1, "value" => "One"]);
    $statement->execute(["id" => 2, "value" => "Two"]);
    $connection->exec("RUN BATCH");
    $connection->commit();

    print("Inserted two rows\n");

    $statement = null;
    $connection = null;
}

function batch_ddl($dsn): void
{
    $connection = new PDO($dsn);
    $connection->exec("START BATCH DDL");
    $connection->exec("CREATE TABLE my_table (id bigint primary key, value varchar)");
    $connection->exec("CREATE INDEX my_index ON my_table (value)");
    $connection->exec("RUN BATCH");

    print("Created a table and an index in one batch\n");

    $connection = null;
}
