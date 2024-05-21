package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang-snippets/samples"
)

func main() {
	host, port, sample, database := extractArguments()
	var err error
	switch sample {
	case "createtables":
		err = samples.CreateTables(host, port, database)
	default:
		err = fmt.Errorf("unknown sample: %s\n", sample)
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func extractArguments() (host string, port int, sample, database string) {
	if len(os.Args) < 3 || len(os.Args) > 4 {
		fmt.Printf("Unexpected number of arguments: %v\n", len(os.Args))
		fmt.Println("Usage: go run <sample-name> <database-name> [<host>:<port>]")
		os.Exit(1)
	}
	sample = os.Args[1]
	database = os.Args[2]
	host = "localhost"
	port = 5432
	if len(os.Args) == 4 {
		if strings.Contains(os.Args[3], ":") {
			fmt.Println("The PGAdapter host/port must be given in the format 'host:port' (e.g. localhost:5432)")
			os.Exit(1)
		}
		parts := strings.Split(os.Args[3], ":")
		host = parts[0]
		var err error
		port, err = strconv.Atoi(parts[1])
		if err != nil {
			fmt.Printf("Invalid port number: %v", parts[1])
			os.Exit(1)
		}
	}
	return host, port, sample, database
}
