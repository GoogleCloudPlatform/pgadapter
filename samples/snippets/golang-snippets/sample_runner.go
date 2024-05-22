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
	case "createconnection":
		err = samples.CreateConnection(host, port, database)
	case "writeusingdml":
		err = samples.WriteDataWithDml(host, port, database)
	case "writeusingdmlbatch":
		err = samples.WriteDataWithDmlBatch(host, port, database)
	case "write":
		err = samples.WriteDataWithCopy(host, port, database)
	case "query":
		err = samples.QueryData(host, port, database)
	case "querywithparameter":
		err = samples.QueryDataWithParameter(host, port, database)
	case "addmarketingbudget":
		err = samples.AddColumn(host, port, database)
	case "ddlbatch":
		err = samples.DdlBatch(host, port, database)
	case "update":
		err = samples.UpdateDataWithCopy(host, port, database)
	case "querymarketingbudget":
		err = samples.QueryDataWithNewColumn(host, port, database)
	case "writewithtransactionusingdml":
		err = samples.WriteWithTransactionUsingDml(host, port, database)
	case "tags":
		err = samples.Tags(host, port, database)
	case "readonlytransaction":
		err = samples.ReadOnlyTransaction(host, port, database)
	case "databoost":
		err = samples.DataBoost(host, port, database)
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
		if !strings.Contains(os.Args[3], ":") {
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
