<?php

/**
 * @throws Exception if the number of command line arguments is not equal to 2 or 4
 */
function parse_arguments(): array {
    global $argc, $argv;

    if (!($argc == 2 || $argc == 4)) {
        throw new Exception(sprintf("Invalid number of arguments: %d\nExpected: 2 or 4", $argc));
    }
    $database = $argv[1];
    if ($argc == 4) {
        $host = $argv[2];
        $port = $argv[3];
    } else {
        $host = "localhost";
        $port = 5432;
    }
    return [$host, $port, $database];
}

function run_sample($sample): void {
    try {
        [$host, $port, $database] = parse_arguments();
        $sample($host, $port, $database);
    } catch (Exception $e) {
        printf("Failed to run sample: %s\n", $e);
    }
}