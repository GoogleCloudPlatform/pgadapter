<?php

require 'create_tables.php';

/**
 * @throws Exception if the number of command line arguments is not equal to 4
 */
function parse_arguments(): array {
    global $argc, $argv;
    
    if (!$argc == 4) {
        throw new Exception(sprintf("Invalid number of arguments: %d\nExpected: 4", $argc));
    }
    $host = $argv[1];
    $port = $argv[2];
    $database = $argv[3];
    return [$host, $port, $database];
}

try {
    [$host, $port, $database] = parse_arguments();
    create_tables($host, $port, $database);
} catch (Exception $e) {
    printf("Failed to run sample: %s\n", $e);
}
