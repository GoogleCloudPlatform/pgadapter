# Open Telemetry in PGAdapter

PGAdapter supports Open Telemetry tracing. You can enable this by adding the `-enable_otel` command
line argument when starting PGAdapter.

Optionally, you can also set a trace sample ratio to limit the number of traces that will be
collected and set with the `-otel_trace_ratio=<ratio>` command line argument. If you omit this
argument, all traces will be collected and exported. The ratio must be in the range `[0.0, 1.0]`.

Example:

```shell
docker run \
  -d -p 5432:5432 \
  -v /path/to/credentials.json:/credentials.json:ro \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -c /credentials.json -x \
  -enable_otel -otel_trace_ratio=0.1
```

## Exporter
PGAdapter will by default export traces to [Google Cloud Trace](https://cloud.google.com/trace).
PGAdapter uses the [OpenTelemetry SDK Autoconfigure module](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md)
for configuring OpenTelemetry. You can use the options of this module to configure another exporter.

PGAdapter uses a different set of default values for the following OpenTelemetry Autoconfigure
properties:
- `otel.traces.exporter`: `none`
- `otel.metrics.exporter`: `none`
- `otel.logs.exporter`: `none`

In addition, PGAdapter always adds an exporter for Google Cloud Trace.

## Traces

The traces are by default exported to [Google Cloud Trace](https://cloud.google.com/trace). The
traces follow the structure of the PostgreSQL wire-protocol. Traces typically consist of the
following spans:

1. `query_protocol_handler`: The [PostgreSQL wire-protocol](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
   consists of multiple messages to parse, describe, bind, and execute SQL statements. Clients and
   drivers will send multiple of these to PGAdapter, and PGAdapter will translate these into a set
   of RPC invocations on CLoud Spanner. PGAdapter minimizes the number of RPC invocations that is
   needed to respond to these messages. One span of `query_protocol_handler` consists of all the
   round-trips that PGAdapter needed to respond to the messages that it received during one query
   session.
2. `execute` / `analyze` / `execute_batch`: PGAdapter will translate the incoming messages into
   `execute`, `analyze`, and `execute_batch` actions on Cloud Spanner. One span of
   `query_protocol_handler` normally consists of one or more of these actions.
3. `execute_on_spanner`: This span contains the time when PGAdapter hands off the execution of a
   statement to the Cloud Spanner Java client, and is roughly identical to the end-to-end
   execution of the statement.
4. `execute_batch`: This span indicates a batch of DML or DDL statements that are being received
   and buffered by PGAdapter. The statements will be sent as one batch to Cloud Spanner for
   execution. The actual execution of the batch is recorded in the `execute_batch_on_spanner` span.
5. `send_result_set`: This span is only recorded for queries that send rows back to the client. It
   shows the time that it took for PGAdapter to receive all rows from Cloud Spanner and send these
   to the client. This span will be longer for queries that return a large number of rows. The time
   it takes to send the results to the client also depends on how quickly the client application can
   consume the rows that are being sent, and on the network speed between PGAdapter and Cloud
   Spanner.

### Examples

The following sections show samples of commonly executed statements. All spans that are exported by
Cloud Spanner contain the following attributes:
1. `db.statement`: The SQL statement that is being executed.
2. `pgadapter.connection_id`: Each connection in PGAdapter is assigned a random ID. You can use this
   ID to filter spans for a specific connection.
3. `pgadapter.transaction_id`: Each transaction in PGAdapter is assigned a random ID. YOu can use
   this ID to filter spans for a specific transaction. This field is not present for statements that
   are executed outside an (explicit) transaction.

#### Query (SELECT)

A typical execution of a query consists of the following spans:
1. `query_protocol_handler`: The front-end query protocol handler of PGAdapter that receives
   messages from the client and returns results to the client. The query handler normally receives
   five messages from the client when executing a query:
    1. `P` (Parse): Parse the query string.
    2. `B` (Bind): Bind the query parameter values.
    3. `D` (Describe): Describe the query result (which columns will be returned by the query).
    4. `E` (Execute): Execute the statement and return the results.
    5. `S` (Sync): Flush all buffered messages and return all results.
2. `execute`: The front-end query protocol handler requests the backend connection to Cloud Spanner
   to execute the query that it constructed from the messages it received.
3. `execute_on_spanner`: This the actual RPC invocation on Cloud Spanner.
4. `send_result_set`: Shows the time it takes to receive all rows from Cloud Spanner and the time it
   takes to send these to the client application.

![PGAdapter Cloud Trace - Query example](img/query_trace_sample.png?raw=true "PGAdapter Cloud Trace - Query example")

The `query_protocol_handler` span also includes the time it takes to send the results to the client.
This time will also depend on the time that the client requires to actually receive and process the
results. You can see this time in the example above as the time that the `query_protocol_handler`
span is active after the statement has been executed.

#### DML (INSERT/UPDATE/DELETE)

A typical execution of a DML statement consists of the following spans:
1. `query_protocol_handler`: The front-end query protocol handler of PGAdapter that receives
   messages from the client and returns results to the client. The query handler normally receives
   five messages from the client when executing a DML statement:
    1. `P` (Parse): Parse the DML string.
    2. `B` (Bind): Bind the DML parameter values.
    3. `D` (Describe): Describe the result of the statement. For DML statement, this is an empty result.
    4. `E` (Execute): Execute the statement and return the update count.
    5. `S` (Sync): Flush all buffered messages and return all results.
2. `execute`: The front-end query protocol handler requests the backend connection to Cloud Spanner
   to execute the DML statement that it constructed from the messages it received.
3. `execute_on_spanner`: This the actual RPC invocation on Cloud Spanner.

![PGAdapter Cloud Trace - DML example](img/dml_trace_sample.png?raw=true "PGAdapter Cloud Trace - DML example")

The `query_protocol_handler` span also includes the time it takes to send the update count to the
client. This time is negligible compared to sending a large result set from PGAdapter to the client.

#### Batch

The PostgreSQL wire-protocol also supports batching multiple statements into one execution. This is
done by sending multiple `E` (Execute) messages to the server (PGAdapter) without sending an `S`
(Sync) message. This will allow the server to buffer the statements and wait with sending the
results of these until it sees an `S` (Sync) message. A typical execution of a batch of DML
statements consists of the following spans:
1. `query_protocol_handler`: The front-end query protocol handler of PGAdapter that receives
   messages from the client and returns results to the client. The query handler normally receives
   four messages for each DML statement in a batch from the client:
   1. `P` (Parse): Parse the DML string.
   2. `B` (Bind): Bind the DML parameter values.
   3. `D` (Describe): Describe the result of the statement. For DML statement, this is an empty result.
   4. `E` (Execute): Execute the statement and return the update count.
2. It then receives an `S` (Sync) message when the client has sent all the DML statements. 
3. `execute_batch`: The front-end query protocol handler requests the backend connection
   to create a DML batch, buffer the DML statements on the client, and then execute the statements
   on Cloud Spanner. The total time of this span is the time it took to buffer all the statements on
   the client and to execute the batch on Cloud Spanner.  
4. `buffer`: For each DML statement, a `buffer` span is created within the DML batch. These
   statements are not yet executed on spanner, but buffered in the Spanner client until
   all the DML statements have been collected.
5. `execute_batch_on_spanner`: Once all DML statements have been collected, PGAdapter creates one
   batch and sends this as one request to Cloud Spanner. This span shows the time it takes to
   execute this batch request on Cloud Spanner.

![PGAdapter Cloud Trace - Batch DML example](img/dml_batch_trace_sample.png?raw=true "PGAdapter Cloud Trace - Batch DML example")

## Frequently Asked Questions

#### How can I find all the statements that were executed on the same connection as the trace I'm looking at?

All traces include an attribute `pgadapter.connection_id`. This connection ID is randomly chosen,
but remains the same for a connection during its lifetime. You can use this ID to look up all other
traces for the same connection by adding a filter `pgadapter.connection_id:<id>`.

![PGAdapter Cloud Trace - Filter connection id](img/trace_filter_connection_id.png?raw=true "PGAdapter Cloud Trace - Filter connection id")

#### How can I find all the statements in the same transaction?

All traces for statements that used an explicit transaction include an attribute `pgadapter.transaction_id`.
This transaction ID is randomly chosen, but remains the same for a transaction during its lifetime.
You can use this ID to look up all other traces for the same transaction by adding a filter `pgadapter.transaction_id:<id>`.

![PGAdapter Cloud Trace - Filter transaction id](img/trace_filter_transaction_id.png?raw=true "PGAdapter Cloud Trace - Filter transaction id")

#### How can I find statements that waited for an aborted read/write transaction to retry?

Cloud Spanner can abort any read/write transaction. A transaction can be aborted for various
reasons:
- Lock conflicts
- Database schema changes
- Too long inactivity (a read/write transaction is aborted after 10 seconds of inactivity)

But it can also be aborted for internal technical reasons in Cloud Spanner. PGAdapter will attempt
to retry transactions that are aborted by Cloud Spanner. This can cause some statements to seem to
execute much slower than they should, as they have to wait for the internal retry attempt to finish
first. You can find statements that waited for a transaction retry by adding the filter
`HasLabel:pgadapter.retry_attempt`.

![PGAdapter Cloud Trace - Filter for retried transactions](img/trace_filter_retry_attempt.png?raw=true "PGAdapter Cloud Trace - Filter for retried transactions")

Note from the above screenshot:
1. The retry internal retry is initiated and executed as part of the statement execution. This makes
   it seem like the statement itself takes long to execute.
2. Cloud Spanner also sends the client a back-off value that it should wait before retrying the
   transaction. This is the reason that the total execution time of the statements seems to be much
   longer than the transaction retry, as the Java client library will wait for this back-off time
   before actually starting the retry.
