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
import static org.junit.Assume.assumeTrue;

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
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
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
import org.postgresql.core.v3.QueryExecutorImpl;
import org.postgresql.jdbc.PgConnection;

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
    return createUrl("extended");
  }

  private String createUrl(String queryMode) {
    if (useDomainSocket) {
      return String.format(
          "jdbc:postgresql://localhost/?"
              + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
              + "&socketFactoryArg=/tmp/.s.PGSQL.%d"
              + "&preferQueryMode=%s",
          pgServer.getLocalPort(), queryMode);
    }
    return String.format(
        "jdbc:postgresql://localhost:%d/?preferQueryMode=%s", pgServer.getLocalPort(), queryMode);
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
  public void testCopyInWithExplicitTransaction() throws SQLException, IOException {
    setupCopyInformationSchemaResults();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      copyManager.copyIn("COPY users FROM STDIN;", new StringReader("5\t5\t5\n6\t6\t6\n7\t7\t7\n"));

      // Verify that we can use the connection for normal queries.
      try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // Verify that the transaction has not yet been committed.
      assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));

      connection.commit();
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
  public void testCopyInWithExplicitTransaction_Rollback() throws SQLException, IOException {
    setupCopyInformationSchemaResults();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      copyManager.copyIn("COPY users FROM STDIN;", new StringReader("5\t5\t5\n6\t6\t6\n7\t7\t7\n"));

      // Verify that we can use the connection for normal queries.
      try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      connection.rollback();
    }

    // Verify that the COPY operation was not committed.
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
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
  public void testCopyIn_Large_FailsWhenAtomic() throws Exception {
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
      assertTrue(
          exception.getMessage(),
          exception
                  .getMessage()
                  .contains("FAILED_PRECONDITION: Record count: 2001 has exceeded the limit: 2000.")
              || exception
                  .getMessage()
                  .contains("Database connection failed when canceling copy operation"));
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
          exception.getMessage(),
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
  public void testCopyInWithInvalidRow() throws SQLException, IOException {
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
      assertTrue(
          exception.getMessage(),
          exception.getMessage().contains("Commit size: 20 has exceeded the limit: 10"));
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
      PgConnection pgConnection = connection.unwrap(PgConnection.class);
      QueryExecutorImpl queryExecutor = (QueryExecutorImpl) pgConnection.getQueryExecutor();
      // Use reflection to get hold of the underlying stream, so we can send any message that we
      // want.
      java.lang.reflect.Field pgStreamField = QueryExecutorBase.class.getDeclaredField("pgStream");
      pgStreamField.setAccessible(true);
      PGStream stream = (PGStream) pgStreamField.get(queryExecutor);

      String sql = "copy users from stdin;";
      int length = sql.length() + 5;
      stream.sendChar('Q');
      stream.sendInteger4(length);
      stream.send(sql.getBytes(StandardCharsets.UTF_8));
      stream.sendChar(0);
      stream.flush();

      // Send a CopyData message and then a 'Q', which is not allowed.
      stream.sendChar('d');
      stream.sendInteger4(payload.length + 4);
      stream.send(payload);

      stream.sendChar('Q');
      // Length = 4 + 8 + 1 = 13
      // (msg length 4 bytes, 8 bytes for SELECT 1, 1 byte for \0)
      stream.sendInteger4(13);
      stream.send("SELECT 1".getBytes(StandardCharsets.UTF_8));
      stream.sendChar(0);
      stream.flush();

      boolean receivedErrorMessage = false;
      StringBuilder errorMessage = new StringBuilder();
      while (!receivedErrorMessage) {
        int command = stream.receiveChar();
        if (command == 'E') {
          receivedErrorMessage = true;
          // Read and ignore the length.
          stream.receiveInteger4();
          while (stream.receiveChar() > 0) {
            errorMessage.append(stream.receiveString()).append('\n');
          }
        } else {
          // Just skip everything in the message.
          length = stream.receiveInteger4();
          stream.skip(length - 4);
        }
      }
      assertEquals(
          "ERROR\n"
              + "XX000\n"
              + "Expected CopyData ('d'), CopyDone ('c') or CopyFail ('f') messages, got: 'Q'\n",
          errorMessage.toString());

      stream.sendChar('c');
      stream.sendInteger4(4);
      stream.sendChar('x');
      stream.sendInteger4(4);
      stream.flush();
    }

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertTrue(commitRequests.isEmpty());
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
  public void testCopyInBatchPsql() throws Exception {
    assumeTrue("This test requires psql to be installed", isPsqlAvailable());
    setupCopyInformationSchemaResults();

    String host = useDomainSocket ? "/tmp" : "localhost";
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand =
        new String[] {"psql", "-h", host, "-p", String.valueOf(pgServer.getLocalPort())};
    builder.command(psqlCommand);
    Process process = builder.start();
    String errors;
    String output;

    try (OutputStreamWriter writer = new OutputStreamWriter(process.getOutputStream());
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      writer.write(
          "SELECT 1\\;copy users from stdin\\;copy users from stdin\\;SELECT 2;\n"
              + "1\t2\t3\n"
              + "\\.\n"
              + "4\t5\t6\n"
              + "\\.\n"
              + "\n"
              + "\\q\n");
      writer.flush();
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    assertEquals("", errors);
    assertEquals(" C \n---\n 2\n(1 row)\n", output);
    int res = process.waitFor();
    assertEquals(0, res);

    // The batch is executed as one implicit transaction.
    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
    CommitRequest commitRequest = commitRequests.get(0);
    assertEquals(1, commitRequest.getMutationsCount());
    // We buffer 2 mutations, but the proto builder in the Java client library combines these two
    // mutations into 1 mutation with 2 rows.
    assertEquals(2, commitRequest.getMutations(0).getInsert().getValuesCount());
    assertEquals(OperationCase.INSERT, commitRequest.getMutations(0).getOperationCase());
    assertEquals(3, commitRequest.getMutations(0).getInsert().getColumnsCount());
    assertEquals(
        "1", commitRequest.getMutations(0).getInsert().getValues(0).getValues(0).getStringValue());
    assertEquals(
        "2", commitRequest.getMutations(0).getInsert().getValues(0).getValues(1).getStringValue());
    assertEquals(
        "3", commitRequest.getMutations(0).getInsert().getValues(0).getValues(2).getStringValue());
    assertEquals(OperationCase.INSERT, commitRequest.getMutations(0).getOperationCase());
    assertEquals(
        "4", commitRequest.getMutations(0).getInsert().getValues(1).getValues(0).getStringValue());
    assertEquals(
        "5", commitRequest.getMutations(0).getInsert().getValues(1).getValues(1).getStringValue());
    assertEquals(
        "6", commitRequest.getMutations(0).getInsert().getValues(1).getValues(2).getStringValue());
  }

  @Test
  public void testCopyInBatch() throws Exception {
    setupCopyInformationSchemaResults();

    byte[] row1 = "5\t6\t7\n".getBytes(StandardCharsets.UTF_8);
    byte[] row2 = "6\t7\t8\n".getBytes(StandardCharsets.UTF_8);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      PgConnection pgConnection = connection.unwrap(PgConnection.class);
      QueryExecutorImpl queryExecutor = (QueryExecutorImpl) pgConnection.getQueryExecutor();
      // Use reflection to get hold of the underlying stream, so we can send any message that we
      // want.
      java.lang.reflect.Field pgStreamField = QueryExecutorBase.class.getDeclaredField("pgStream");
      pgStreamField.setAccessible(true);
      PGStream stream = (PGStream) pgStreamField.get(queryExecutor);

      String sql = "SELECT 1;copy users from stdin;copy users from stdin;SELECT 2;";
      int length = sql.length() + 5;
      stream.sendChar('Q');
      stream.sendInteger4(length);
      stream.send(sql.getBytes(StandardCharsets.UTF_8));
      stream.sendChar(0);
      stream.flush();

      // Send CopyData + CopyDone for the first copy operation.
      stream.sendChar('d');
      stream.sendInteger4(row1.length + 4);
      stream.send(row1);
      stream.sendChar('c');
      stream.sendInteger4(4);
      stream.flush();

      // Send CopyData + CopyDone for the second copy operation.
      stream.sendChar('d');
      stream.sendInteger4(row2.length + 4);
      stream.send(row2);
      stream.sendChar('c');
      stream.sendInteger4(4);
      stream.flush();

      boolean receivedReadyForQuery = false;
      while (!receivedReadyForQuery) {
        int command = stream.receiveChar();
        if (command == 'Z') {
          receivedReadyForQuery = true;
        } else {
          // Just skip everything in the message.
          length = stream.receiveInteger4();
          stream.skip(length - 4);
        }
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(1, commitRequest.getMutationsCount());
    assertEquals(2, commitRequest.getMutations(0).getInsert().getValuesCount());
    assertEquals(
        "5", commitRequest.getMutations(0).getInsert().getValues(0).getValues(0).getStringValue());
    assertEquals(
        "6", commitRequest.getMutations(0).getInsert().getValues(0).getValues(1).getStringValue());
    assertEquals(
        "7", commitRequest.getMutations(0).getInsert().getValues(0).getValues(2).getStringValue());
    assertEquals(
        "6", commitRequest.getMutations(0).getInsert().getValues(1).getValues(0).getStringValue());
    assertEquals(
        "7", commitRequest.getMutations(0).getInsert().getValues(1).getValues(1).getStringValue());
    assertEquals(
        "8", commitRequest.getMutations(0).getInsert().getValues(1).getValues(2).getStringValue());
  }

  @Test
  public void testCopyInBatchWithCopyFail() throws Exception {
    setupCopyInformationSchemaResults();

    byte[] row1 = "5\t6\t7\n".getBytes(StandardCharsets.UTF_8);
    byte[] row2 = "8\t9\t10\n".getBytes(StandardCharsets.UTF_8);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      PgConnection pgConnection = connection.unwrap(PgConnection.class);
      QueryExecutorImpl queryExecutor = (QueryExecutorImpl) pgConnection.getQueryExecutor();
      // Use reflection to get hold of the underlying stream, so we can send any message that we
      // want.
      java.lang.reflect.Field pgStreamField = QueryExecutorBase.class.getDeclaredField("pgStream");
      pgStreamField.setAccessible(true);
      PGStream stream = (PGStream) pgStreamField.get(queryExecutor);

      String sql = "SELECT 1;copy users from stdin;copy users from stdin;SELECT 2;";
      int length = sql.getBytes(StandardCharsets.UTF_8).length + 5;
      stream.sendChar('Q');
      stream.sendInteger4(length);
      stream.send(sql.getBytes(StandardCharsets.UTF_8));
      stream.sendChar(0);
      stream.flush();

      // Send CopyData + CopyDone for the first copy operation.
      stream.sendChar('d');
      stream.sendInteger4(row1.length + 4);
      stream.send(row1);
      stream.sendChar('c');
      stream.sendInteger4(4);
      stream.flush();

      // Send CopyData + CopyFail for the second copy operation.
      stream.sendChar('d');
      stream.sendInteger4(row2.length + 4);
      stream.send(row2);
      String error = "Changed my mind";
      stream.sendChar('f');
      stream.sendInteger4(4 + error.getBytes(StandardCharsets.UTF_8).length + 1);
      stream.send(error.getBytes(StandardCharsets.UTF_8));
      stream.sendChar(0);
      stream.flush();

      boolean receivedReadyForQuery = false;
      boolean receivedErrorMessage = false;
      StringBuilder errorMessage = new StringBuilder();
      while (!receivedReadyForQuery) {
        int command = stream.receiveChar();
        if (command == 'Z') {
          receivedReadyForQuery = true;
        } else if (command == 'E') {
          receivedErrorMessage = true;
          // Read and ignore the length.
          stream.receiveInteger4();
          while (stream.receiveChar() > 0) {
            errorMessage.append(stream.receiveString()).append('\n');
          }
        } else {
          // Just skip everything in the message.
          length = stream.receiveInteger4();
          stream.skip(length - 4);
        }
      }
      assertTrue(receivedErrorMessage);
      assertEquals(
          "ERROR\n"
              + "P0001\n"
              + "CANCELLED: Changed my mind\n"
              + "ERROR\n"
              + "P0001\n"
              + "INVALID_ARGUMENT: current transaction is aborted, commands ignored until end of transaction block\n",
          errorMessage.toString());
    }
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
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
