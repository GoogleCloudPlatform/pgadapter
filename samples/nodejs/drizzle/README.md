<meta name='keywords' content='pgadapter, drizzle, drizzle-orm, spanner, cloud spanner, node, node.js, typescript'>

# PGAdapter Spanner and Drizzle ORM

This directory contains a sample application demonstrating how to connect to Cloud Spanner through PGAdapter using the [Drizzle ORM](https://orm.drizzle.team/) with the standard Node.js `pg` driver.

It demonstrates executing simple queries, batch DML, transactions, and Spanner-specific settings like stale reads.

## Prerequisites

- Node.js (version 24 or newer is recommended)
- Docker (required to run PGAdapter and the Spanner Emulator via Testcontainers)

## Running the Sample

To install the dependencies and execute the sample:

```shell
npm install
npm start
```

The sample application will:
1. Automatically pull and start a Docker container running PGAdapter and the Spanner Emulator on a random port.
2. Initialize connection configurations and define the database schema.
3. Perform insert and query operations inside a transaction.
4. Sell a ticket (inserting into `ticket_sales`) to demonstrate auto-generating sequence-based primary keys.
5. Demonstrate a stale read by configuring `SET spanner.read_only_staleness`.
6. Terminate the container once execution completes.
