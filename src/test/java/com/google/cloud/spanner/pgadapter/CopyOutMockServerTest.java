// Copyright 2022 Google LLC
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

package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement.Format;
import com.google.cloud.spanner.pgadapter.utils.CopyInParser;
import com.google.cloud.spanner.pgadapter.utils.CopyRecord;
import com.google.protobuf.ByteString;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.Partition;
import com.google.spanner.v1.PartitionQueryRequest;
import com.google.spanner.v1.PartitionResponse;
import com.google.spanner.v1.Transaction;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

@RunWith(Parameterized.class)
public class CopyOutMockServerTest extends AbstractMockServerTest {
  @Parameter public boolean useDomainSocket;

  @Parameters(name = "useDomainSocket = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled() ? new Object[] {true, false} : new Object[] {false};
  }

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        createMockSpannerServiceWithQueryPartitions(), "d", Collections.emptyList());
  }

  private static MockSpannerServiceImpl createMockSpannerServiceWithQueryPartitions() {
    return new MockSpannerServiceImpl() {
      @Override
      public void partitionQuery(
          PartitionQueryRequest request, StreamObserver<PartitionResponse> responseObserver) {
        // Only partition queries that use the random result generator.
        if (!request.getSql().equals("select * from random")) {
          super.partitionQuery(request, responseObserver);
          return;
        }

        CountDownLatch partitionedLatch = new CountDownLatch(1);
        executeSql(
            ExecuteSqlRequest.newBuilder()
                .setSql(request.getSql())
                .setSession(request.getSession())
                .setTransaction(request.getTransaction())
                .build(),
            new StreamObserver<com.google.spanner.v1.ResultSet>() {
              @Override
              public void onNext(com.google.spanner.v1.ResultSet resultSet) {
                long rowsPerPartition =
                    resultSet.getRowsCount() / request.getPartitionOptions().getMaxPartitions();
                if (resultSet.getRowsCount() % request.getPartitionOptions().getMaxPartitions()
                    != 0) {
                  rowsPerPartition++;
                }
                int rowIndex = 0;
                for (int i = 0; i < request.getPartitionOptions().getMaxPartitions(); i++) {
                  com.google.spanner.v1.ResultSet.Builder builder =
                      com.google.spanner.v1.ResultSet.newBuilder()
                          .setMetadata(resultSet.getMetadata());
                  long partitionRows = 0L;
                  while (partitionRows < rowsPerPartition && rowIndex < resultSet.getRowsCount()) {
                    builder.addRows(resultSet.getRows(rowIndex));
                    rowIndex++;
                    partitionRows++;
                  }
                  putStatementResult(
                      StatementResult.query(
                          Statement.of(request.getSql() + " - partition " + i), builder.build()));
                }
              }

              @Override
              public void onError(Throwable throwable) {
                partitionedLatch.countDown();
              }

              @Override
              public void onCompleted() {
                partitionedLatch.countDown();
              }
            });

        try {
          partitionedLatch.await();

          PartitionResponse.Builder builder =
              PartitionResponse.newBuilder()
                  .setTransaction(
                      Transaction.newBuilder().setId(request.getTransaction().getId()).build());
          for (long i = 0; i < Math.max(1, request.getPartitionOptions().getMaxPartitions()); i++) {
            builder.addPartitions(
                Partition.newBuilder()
                    .setPartitionToken(
                        ByteString.copyFrom(String.format("%d", i), StandardCharsets.UTF_8))
                    .build());
          }
          responseObserver.onNext(builder.build());
          responseObserver.onCompleted();
        } catch (StatusRuntimeException exception) {
          responseObserver.onError(exception);
        } catch (Throwable exception) {
          responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
      }

      @Override
      public void executeStreamingSql(
          ExecuteSqlRequest request, StreamObserver<PartialResultSet> responseObserver) {
        if (!request.getPartitionToken().isEmpty()) {
          request =
              request
                  .toBuilder()
                  .setSql(
                      request.getSql()
                          + " - partition "
                          + request.getPartitionToken().toString(StandardCharsets.UTF_8))
                  .setPartitionToken(ByteString.EMPTY)
                  .build();
        }
        super.executeStreamingSql(request, responseObserver);
      }
    };
  }

  private String createUrl() {
    if (useDomainSocket) {
      return String.format(
          "jdbc:postgresql://localhost/?"
              + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
              + "&socketFactoryArg=/tmp/.s.PGSQL.%d",
          pgServer.getLocalPort());
    }
    return String.format("jdbc:postgresql://localhost:%d/", pgServer.getLocalPort());
  }

  @Test
  public void testCopyOut() throws SQLException, IOException {
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("select * from all_types"), ALL_TYPES_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      StringWriter writer = new StringWriter();
      copyManager.copyOut("COPY all_types TO STDOUT", writer);

      assertEquals(
          "1\tt\t\\\\x74657374\t3.14\t100\t6.626\t2022-02-16 13:18:02.123456789+00\t2022-03-29\ttest\t{\"key\": \"value\"}\n",
          writer.toString());

      // Verify that we can use the connection for normal queries.
      try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    List<ExecuteSqlRequest> sqlRequests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, sqlRequests.size());
    ExecuteSqlRequest request = sqlRequests.get(0);
    assertEquals("select * from all_types", request.getSql());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testCopyOutWithColumns() throws SQLException, IOException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select col_bigint, col_varchar from all_types"), ALL_TYPES_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      StringWriter writer = new StringWriter();
      copyManager.copyOut("COPY all_types (col_bigint, col_varchar) TO STDOUT", writer);

      assertEquals(
          "1\tt\t\\\\x74657374\t3.14\t100\t6.626\t2022-02-16 13:18:02.123456789+00\t2022-03-29\ttest\t{\"key\": \"value\"}\n",
          writer.toString());

      // Verify that we can use the connection for normal queries.
      try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    List<ExecuteSqlRequest> sqlRequests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, sqlRequests.size());
    ExecuteSqlRequest request = sqlRequests.get(0);
    assertEquals("select col_bigint, col_varchar from all_types", request.getSql());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testCopyOutCsv() throws SQLException, IOException {
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("select * from all_types"), ALL_TYPES_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      StringWriter writer = new StringWriter();
      copyManager.copyOut(
          "COPY all_types TO STDOUT (format csv, escape '~', delimiter '-')", writer);

      assertEquals(
          "1-t-\\x74657374-3.14-100-6.626-\"2022-02-16 13:18:02.123456789+00\"-\"2022-03-29\"-test-\"{~\"key~\": ~\"value~\"}\"\n",
          writer.toString());
    }
  }

  @Test
  public void testCopyOutCsvWithHeader() throws SQLException, IOException {
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("select * from all_types"), ALL_TYPES_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      StringWriter writer = new StringWriter();
      copyManager.copyOut(
          "COPY all_types TO STDOUT (header, format csv, escape '~', delimiter '-')", writer);

      assertEquals(
          "col_bigint-col_bool-col_bytea-col_float8-col_int-col_numeric-col_timestamptz-col_date-col_varchar-col_jsonb\n"
              + "1-t-\\x74657374-3.14-100-6.626-\"2022-02-16 13:18:02.123456789+00\"-\"2022-03-29\"-test-\"{~\"key~\": ~\"value~\"}\"\n",
          writer.toString());
    }
  }

  @Test
  public void testCopyOutCsvWithQueryAndHeader() throws SQLException, IOException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from all_types order by col_bigint"), ALL_TYPES_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      StringWriter writer = new StringWriter();
      copyManager.copyOut(
          "COPY (select * from all_types order by col_bigint) TO STDOUT (header, format csv, escape '\\', delimiter '|')",
          writer);

      assertEquals(
          "col_bigint|col_bool|col_bytea|col_float8|col_int|col_numeric|col_timestamptz|col_date|col_varchar|col_jsonb\n"
              + "1|t|\"\\\\x74657374\"|3.14|100|6.626|2022-02-16 13:18:02.123456789+00|2022-03-29|test|\"{\\\"key\\\": \\\"value\\\"}\"\n",
          writer.toString());
    }
  }

  @Test
  public void testCopyOutCsvWithQuote() throws SQLException, IOException {
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("select * from all_types"), ALL_TYPES_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      StringWriter writer = new StringWriter();
      copyManager.copyOut(
          "COPY all_types TO STDOUT (format csv, escape '\\', delimiter '|', quote '\"')", writer);

      assertEquals(
          "1|t|\"\\\\x74657374\"|3.14|100|6.626|2022-02-16 13:18:02.123456789+00|2022-03-29|test|\"{\\\"key\\\": \\\"value\\\"}\"\n",
          writer.toString());
    }
  }

  @Test
  public void testCopyOutNulls() throws SQLException, IOException {
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("select * from all_types"), ALL_TYPES_NULLS_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      StringWriter writer = new StringWriter();
      copyManager.copyOut("COPY all_types TO STDOUT", writer);

      assertEquals("\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\n", writer.toString());
    }
  }

  private static boolean isPsqlAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand = new String[] {"psql", "--version"};
    builder.command(psqlCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  @Test
  public void testCopyOutBinaryPsql() throws Exception {
    assumeTrue("This test requires psql", isPsqlAvailable());
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("select * from all_types"), ALL_TYPES_RESULTSET));

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(
        "psql",
        "-h",
        (useDomainSocket ? "/tmp" : "localhost"),
        "-p",
        String.valueOf(pgServer.getLocalPort()),
        "-c",
        "copy all_types to stdout binary");
    Process process = builder.start();
    StringBuilder errorBuilder = new StringBuilder();
    try (Scanner scanner = new Scanner(new InputStreamReader(process.getErrorStream()))) {
      while (scanner.hasNextLine()) {
        errorBuilder.append(scanner.nextLine()).append('\n');
      }
    }
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    PipedInputStream inputStream = new PipedInputStream(pipedOutputStream, 1 << 16);
    CopyInParser copyParser = CopyInParser.create(Format.BINARY, null, inputStream, false);
    int b;
    while ((b = process.getInputStream().read()) != -1) {
      pipedOutputStream.write(b);
    }
    int res = process.waitFor();
    assertEquals("", errorBuilder.toString());
    assertEquals(0, res);

    Iterator<CopyRecord> iterator = copyParser.iterator();
    assertTrue(iterator.hasNext());
    CopyRecord record = iterator.next();
    assertFalse(iterator.hasNext());

    assertEquals(Value.int64(1L), record.getValue(Type.int64(), 0));
    assertEquals(Value.bool(true), record.getValue(Type.bool(), 1));
    assertEquals(Value.bytes(ByteArray.copyFrom("test")), record.getValue(Type.bytes(), 2));
    assertEquals(Value.float64(3.14), record.getValue(Type.float64(), 3));
    assertEquals(Value.int64(100L), record.getValue(Type.int64(), 4));
    assertEquals(Value.pgNumeric("6.626"), record.getValue(Type.pgNumeric(), 5));
    // Note: The binary format truncates timestamptz value to microsecond precision.
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2022-02-16T13:18:02.123456000Z")),
        record.getValue(Type.timestamp(), 6));
    assertEquals(Value.date(Date.parseDate("2022-03-29")), record.getValue(Type.date(), 7));
    assertEquals(Value.string("test"), record.getValue(Type.string(), 8));
  }

  @Test
  public void testCopyOutNullsBinaryPsql() throws Exception {
    assumeTrue("This test requires psql", isPsqlAvailable());
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("select * from all_types"), ALL_TYPES_NULLS_RESULTSET));

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(
        "psql",
        "-h",
        (useDomainSocket ? "/tmp" : "localhost"),
        "-p",
        String.valueOf(pgServer.getLocalPort()),
        "-c",
        "copy all_types to stdout binary");
    Process process = builder.start();
    StringBuilder errorBuilder = new StringBuilder();
    try (Scanner scanner = new Scanner(new InputStreamReader(process.getErrorStream()))) {
      while (scanner.hasNextLine()) {
        errorBuilder.append(scanner.nextLine()).append('\n');
      }
    }
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    PipedInputStream inputStream = new PipedInputStream(pipedOutputStream, 1 << 16);
    CopyInParser copyParser = CopyInParser.create(Format.BINARY, null, inputStream, false);
    int b;
    while ((b = process.getInputStream().read()) != -1) {
      pipedOutputStream.write(b);
    }
    int res = process.waitFor();
    assertEquals("", errorBuilder.toString());
    assertEquals(0, res);

    Iterator<CopyRecord> iterator = copyParser.iterator();
    assertTrue(iterator.hasNext());
    CopyRecord record = iterator.next();
    assertFalse(iterator.hasNext());

    for (int col = 0; col < record.numColumns(); col++) {
      // Note: Null values in a COPY BINARY stream are untyped, so it does not matter what type we
      // specify when getting the value.
      assertTrue(record.getValue(Type.string(), col).isNull());
    }
  }

  @Test
  public void testCopyOutPartitioned() throws SQLException, IOException {
    final int expectedRowCount = 100;
    RandomResultSetGenerator randomResultSetGenerator =
        new RandomResultSetGenerator(expectedRowCount, Dialect.POSTGRESQL);
    com.google.spanner.v1.ResultSet resultSet = randomResultSetGenerator.generate();
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("select * from random"), resultSet));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      StringWriter writer = new StringWriter();
      long rows = copyManager.copyOut("COPY random TO STDOUT", writer);

      assertEquals(expectedRowCount, rows);

      try (Scanner scanner = new Scanner(writer.toString())) {
        int lineCount = 0;
        while (scanner.hasNextLine()) {
          lineCount++;
          String line = scanner.nextLine();
          String[] columns = line.split("\t");
          int index = findIndex(resultSet, columns);
          assertNotEquals(String.format("Row %d not found: %s", lineCount, line), -1, index);
        }
        assertEquals(expectedRowCount, lineCount);
      }
    }
  }

  static int findIndex(com.google.spanner.v1.ResultSet resultSet, String[] cols) {
    for (int index = 0; index < resultSet.getRowsCount(); index++) {
      boolean nullValuesEqual = true;
      for (int colIndex = 0; colIndex < cols.length; colIndex++) {
        if (cols[colIndex].equals("\\N")
            && !resultSet.getRows(index).getValues(colIndex).hasNullValue()) {
          nullValuesEqual = false;
          break;
        }
        if (!cols[colIndex].equals("\\N")
            && resultSet.getRows(index).getValues(colIndex).hasNullValue()) {
          nullValuesEqual = false;
          break;
        }
      }
      if (!nullValuesEqual) {
        continue;
      }

      boolean valuesEqual = true;
      for (int colIndex = 0; colIndex < cols.length; colIndex++) {
        if (!resultSet.getRows(index).getValues(colIndex).hasNullValue()) {
          if (resultSet.getMetadata().getRowType().getFields(colIndex).getType().getCode()
                  == TypeCode.STRING
              && !cols[colIndex].equals(
                  resultSet.getRows(index).getValues(colIndex).getStringValue())) {
            valuesEqual = false;
            break;
          }
          if (resultSet.getRows(index).getValues(colIndex).hasBoolValue()
              && !cols[colIndex].equals(
                  resultSet.getRows(index).getValues(colIndex).getBoolValue() ? "t" : "f")) {
            valuesEqual = false;
            break;
          }
        }
      }
      if (valuesEqual) {
        return index;
      }
    }
    return -1;
  }
}
