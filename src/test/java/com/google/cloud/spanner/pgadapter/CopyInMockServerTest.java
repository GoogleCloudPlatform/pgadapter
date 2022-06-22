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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.Mutation;
import com.google.spanner.v1.Mutation.OperationCase;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.PGStream;
import org.postgresql.core.QueryExecutorBase;
import org.postgresql.core.v3.CopyOperationImpl;
import org.postgresql.core.v3.QueryExecutorImpl;

@RunWith(Parameterized.class)
public class CopyInMockServerTest extends AbstractMockServerTest {

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

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default extended
   * mode for queries and DML statements.
   */
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
  public void testCopyIn() throws SQLException, IOException {
    setupCopyInformationSchemaResults();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      copyManager.copyIn("COPY users FROM STDIN;", new StringReader("5\t5\t5\n6\t6\t6\n7\t7\t7\n"));

      // Verify that we can use the connection for normal queries.
      try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
    CommitRequest commitRequest = commitRequests.get(0);
    assertEquals(1, commitRequest.getMutationsCount());

    Mutation mutation = commitRequest.getMutations(0);
    assertEquals(OperationCase.INSERT, mutation.getOperationCase());
    assertEquals(3, mutation.getInsert().getValuesCount());
  }

  @Test
  public void testCopyIn_Small() throws SQLException, IOException {
    setupCopyInformationSchemaResults();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      PGConnection pgConnection = connection.unwrap(PGConnection.class);
      CopyManager copyManager = pgConnection.getCopyAPI();
      long copyCount =
          copyManager.copyIn(
              "copy all_types from stdin;",
              new FileInputStream("./src/test/resources/all_types_data_small.txt"));
      assertEquals(100L, copyCount);
    }
  }

  @Test
  public void testCopyIn_Large_FailsWhenAtomic() throws SQLException {
    setupCopyInformationSchemaResults();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      PGConnection pgConnection = connection.unwrap(PGConnection.class);
      CopyManager copyManager = pgConnection.getCopyAPI();
      SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  copyManager.copyIn(
                      "copy all_types from stdin;",
                      new FileInputStream("./src/test/resources/all_types_data.txt")));
      // The JDBC driver CopyManager takes the COPY protocol quite literally, and as the COPY
      // protocol does not include any error handling, the JDBC driver will just send all data to
      // the server and ignore any error messages the server might send during the copy operation.
      // PGAdapter therefore drops the connection if it continues to receive CopyData messages after
      // it sent back an error message.
      assertTrue(exception.getMessage().contains("Database connection failed"));
    }
  }

  @Test
  public void testCopyIn_Large_SucceedsWhenNonAtomic() throws SQLException, IOException {
    setupCopyInformationSchemaResults();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection
          .createStatement()
          .execute("set spanner.autocommit_dml_mode='partitioned_non_atomic'");

      PGConnection pgConnection = connection.unwrap(PGConnection.class);
      CopyManager copyManager = pgConnection.getCopyAPI();
      long copyCount =
          copyManager.copyIn(
              "copy all_types from stdin;",
              new FileInputStream("./src/test/resources/all_types_data.txt"));
      assertEquals(10_000L, copyCount);
    }
  }

  @Test
  public void testCopyInError() throws SQLException {
    setupCopyInformationSchemaResults();
    mockSpanner.setCommitExecutionTime(
        SimulatedExecutionTime.ofException(Status.INVALID_ARGUMENT.asRuntimeException()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  copyManager.copyIn(
                      "COPY users FROM STDIN;", new StringReader("5\t5\t5\n6\t6\t6\n7\t7\t7\n")));
      assertTrue(
          exception.getMessage().contains("io.grpc.StatusRuntimeException: INVALID_ARGUMENT"));
    }

    // The server should receive one commit request, but this commit failed in this case.
    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
  }

  @Test
  public void testCopyIn_TableNotFound() throws SQLException {
    setupCopyInformationSchemaResults(false);

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> copyManager.copyIn("COPY users FROM STDIN;", new StringReader("5\t5\t5\n")));
      assertEquals(
          "ERROR: INVALID_ARGUMENT: Table users is not found in information_schema",
          exception.getMessage());

      // Verify that we can use the connection for normal queries.
      try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertTrue(commitRequests.isEmpty());
  }

  @Test
  public void testCopyInWithInvalidRow() throws SQLException {
    setupCopyInformationSchemaResults();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      // This row does not contain all the necessary columns.
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> copyManager.copyIn("COPY users FROM STDIN;", new StringReader("5\n")));
      assertTrue(
          exception
              .getMessage()
              .contains("Row length mismatched. Expected 3 columns, but only found 1"));
    }

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertTrue(commitRequests.isEmpty());
  }

  @Test
  public void testCopyInExceedsCommitSizeLimit_FailsInAtomicMode() throws SQLException {
    setupCopyInformationSchemaResults();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      System.setProperty("copy_in_commit_limit", "10");
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  copyManager.copyIn(
                      "COPY users FROM STDIN;", new StringReader("5\t5\t5\n6\t6\t6\n7\t7\t7\n")));
      assertTrue(exception.getMessage().contains("Commit size: 20 has exceeded the limit: 10"));
    } finally {
      System.getProperties().remove("copy_in_commit_limit");
    }

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(0, commitRequests.size());
  }

  @Test
  public void testCopyInExceedsCommitSizeLimit_BatchesInNonAtomicMode()
      throws SQLException, IOException {
    setupCopyInformationSchemaResults();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      System.setProperty("copy_in_commit_limit", "10");
      connection
          .createStatement()
          .execute("set spanner.autocommit_dml_mode='partitioned_non_atomic'");
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      copyManager.copyIn("COPY users FROM STDIN;", new StringReader("5\t5\t5\n6\t6\t6\n7\t7\t7\n"));
    } finally {
      System.getProperties().remove("copy_in_commit_limit");
    }

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(3, commitRequests.size());
    for (CommitRequest request : commitRequests) {
      assertEquals(1, request.getMutationsCount());
      Mutation mutation = request.getMutations(0);
      assertEquals(OperationCase.INSERT, mutation.getOperationCase());
      assertEquals(1, mutation.getInsert().getValuesCount());
    }
  }

  @Test
  public void testCopyInError_BatchedNonAtomic() throws SQLException {
    setupCopyInformationSchemaResults();
    mockSpanner.setCommitExecutionTime(
        SimulatedExecutionTime.ofException(Status.INVALID_ARGUMENT.asRuntimeException()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      System.setProperty("copy_in_commit_limit", "10");
      connection
          .createStatement()
          .execute("set spanner.autocommit_dml_mode='partitioned_non_atomic'");
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  copyManager.copyIn(
                      "COPY users FROM STDIN;", new StringReader("5\t5\t5\n6\t6\t6\n7\t7\t7\n")));
      assertTrue(
          exception.getMessage().contains("io.grpc.StatusRuntimeException: INVALID_ARGUMENT"));
    } finally {
      System.getProperties().remove("copy_in_commit_limit");
    }

    // The server should receive between 1 and 3 commit requests. We don't know exactly how many as
    // the commits can be executed in parallel.
    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertTrue(
        "Number of commits should be between 1 and 3",
        commitRequests.size() >= 1 && commitRequests.size() <= 3);
  }

  @Test
  public void testCopyIn_Cancel() throws SQLException {
    setupCopyInformationSchemaResults();

    byte[] payload = "5\t5\t5\n".getBytes(StandardCharsets.UTF_8);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      CopyIn copyOperation = copyManager.copyIn("COPY users FROM STDIN;");
      copyOperation.writeToCopy(payload, 0, payload.length);
      copyOperation.cancelCopy();

      // Verify that we can use the connection for normal queries.
      try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertTrue(commitRequests.isEmpty());
  }

  @Test
  public void testCopyIn_QueryDuringCopy()
      throws SQLException, NoSuchFieldException, IllegalAccessException, IOException {
    setupCopyInformationSchemaResults();

    byte[] payload = "5\t5\t5\n".getBytes(StandardCharsets.UTF_8);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      // Start a copy operation and then try to execute a query during the copy.
      CopyIn copyOperation = copyManager.copyIn("COPY users FROM STDIN;");
      copyOperation.writeToCopy(payload, 0, payload.length);
      copyOperation.flushCopy();

      // Use reflection to get hold of the underlying stream, so we can send an invalid message.
      java.lang.reflect.Field queryExecutorField =
          CopyOperationImpl.class.getDeclaredField("queryExecutor");
      queryExecutorField.setAccessible(true);
      QueryExecutorImpl queryExecutor = (QueryExecutorImpl) queryExecutorField.get(copyOperation);
      java.lang.reflect.Field pgStreamField = QueryExecutorBase.class.getDeclaredField("pgStream");
      pgStreamField.setAccessible(true);
      PGStream stream = (PGStream) pgStreamField.get(queryExecutor);
      stream.sendChar('Q');
      // Length = 4 + 8 + 1 = 13
      // (msg length 4 bytes, 8 bytes for SELECT 1, 1 byte for \0)
      stream.sendInteger4(13);
      stream.send("SELECT 1".getBytes(StandardCharsets.UTF_8));
      stream.sendChar(0);
      stream.flush();

      // PGAdapter drops the connection if an invalid message is received during a COPY. This is a
      // safety measure as there is no other error handling in the COPY protocol, and the server
      // could otherwise have been completely flushed with garbage if it continued to receive
      // messages after receiving an invalid message.
      SQLException exception = assertThrows(SQLException.class, copyOperation::endCopy);
      assertEquals("Database connection failed when ending copy", exception.getMessage());
    }

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertTrue(commitRequests.isEmpty());
  }

  private void setupCopyInformationSchemaResults() {
    setupCopyInformationSchemaResults(true);
  }

  private void setupCopyInformationSchemaResults(boolean tableFound) {
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("column_name")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("data_type")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .build();
    com.google.spanner.v1.ResultSet resultSet;
    if (tableFound) {
      resultSet =
          com.google.spanner.v1.ResultSet.newBuilder()
              .addRows(
                  ListValue.newBuilder()
                      .addValues(Value.newBuilder().setStringValue("id").build())
                      .addValues(Value.newBuilder().setStringValue("bigint").build())
                      .build())
              .addRows(
                  ListValue.newBuilder()
                      .addValues(Value.newBuilder().setStringValue("age").build())
                      .addValues(Value.newBuilder().setStringValue("bigint").build())
                      .build())
              .addRows(
                  ListValue.newBuilder()
                      .addValues(Value.newBuilder().setStringValue("name").build())
                      .addValues(Value.newBuilder().setStringValue("character varying").build())
                      .build())
              .setMetadata(metadata)
              .build();
    } else {
      resultSet = com.google.spanner.v1.ResultSet.newBuilder().setMetadata(metadata).build();
    }

    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.newBuilder(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1")
                .bind("p1")
                .to("users")
                .build(),
            resultSet));
    com.google.spanner.v1.ResultSet allTypesResultSet =
        com.google.spanner.v1.ResultSet.newBuilder()
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("col_bigint").build())
                    .addValues(Value.newBuilder().setStringValue("bigint").build())
                    .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("col_bool").build())
                    .addValues(Value.newBuilder().setStringValue("boolean").build())
                    .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("col_bytea").build())
                    .addValues(Value.newBuilder().setStringValue("bytea").build())
                    .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("col_float8").build())
                    .addValues(Value.newBuilder().setStringValue("float8").build())
                    .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("col_int").build())
                    .addValues(Value.newBuilder().setStringValue("bigint").build())
                    .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("col_numeric").build())
                    .addValues(Value.newBuilder().setStringValue("numeric").build())
                    .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("col_timestamptz").build())
                    .addValues(
                        Value.newBuilder().setStringValue("timestamp with time zone").build())
                    .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("col_date").build())
                    .addValues(Value.newBuilder().setStringValue("date").build())
                    .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("col_varchar").build())
                    .addValues(Value.newBuilder().setStringValue("character varying").build())
                    .build())
            .setMetadata(metadata)
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.newBuilder(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1")
                .bind("p1")
                .to("all_types")
                .build(),
            allTypesResultSet));

    String indexedColumnsCountSql =
        "SELECT COUNT(*) FROM information_schema.index_columns WHERE table_schema='public' and table_name=$1 and column_name in ($2, $3, $4)";
    ResultSetMetadata indexedColumnsCountMetadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .build())
                    .build())
            .build();
    com.google.spanner.v1.ResultSet indexedColumnsCountResultSet =
        com.google.spanner.v1.ResultSet.newBuilder()
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("2").build())
                    .build())
            .setMetadata(indexedColumnsCountMetadata)
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.newBuilder(indexedColumnsCountSql)
                .bind("p1")
                .to("users")
                .bind("p2")
                .to("id")
                .bind("p3")
                .to("age")
                .bind("p4")
                .to("name")
                .build(),
            indexedColumnsCountResultSet));

    String allTypesIndexedColumnsCountSql =
        "SELECT COUNT(*) FROM information_schema.index_columns WHERE table_schema='public' and table_name=$1 and column_name in ($2, $3, $4, $5, $6, $7, $8, $9, $10)";
    ResultSetMetadata allTypesIndexedColumnsCountMetadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .build())
                    .build())
            .build();
    com.google.spanner.v1.ResultSet allTypesIndexedColumnsCountResultSet =
        com.google.spanner.v1.ResultSet.newBuilder()
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("1").build())
                    .build())
            .setMetadata(allTypesIndexedColumnsCountMetadata)
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.newBuilder(allTypesIndexedColumnsCountSql)
                .bind("p1")
                .to("all_types")
                .bind("p2")
                .to("col_bigint")
                .bind("p3")
                .to("col_bool")
                .bind("p4")
                .to("col_bytea")
                .bind("p5")
                .to("col_float8")
                .bind("p6")
                .to("col_int")
                .bind("p7")
                .to("col_numeric")
                .bind("p8")
                .to("col_timestamptz")
                .bind("p9")
                .to("col_date")
                .bind("p10")
                .to("col_varchar")
                .build(),
            allTypesIndexedColumnsCountResultSet));
  }
}
