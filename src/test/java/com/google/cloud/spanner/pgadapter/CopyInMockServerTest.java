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

import static com.google.cloud.spanner.pgadapter.ITPsqlTest.POSTGRES_DATABASE;
import static com.google.cloud.spanner.pgadapter.ITPsqlTest.POSTGRES_HOST;
import static com.google.cloud.spanner.pgadapter.ITPsqlTest.POSTGRES_PORT;
import static com.google.cloud.spanner.pgadapter.ITPsqlTest.POSTGRES_USER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.Mutation;
import com.google.spanner.v1.Mutation.OperationCase;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.RollbackRequest;
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
import java.util.Base64;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
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
  @Rule public Timeout globalTimeout = Timeout.seconds(10);

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
  public void testCopyInNullValues() throws SQLException, IOException {
    setupCopyInformationSchemaResults();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      copyManager.copyIn(
          "COPY users FROM STDIN;", new StringReader("5\t\\N\t\\N\n6\t\\N\t\\N\n7\t\\N\t\\N\n"));

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
    Value nullValue = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    assertEquals("5", mutation.getInsert().getValues(0).getValues(0).getStringValue());
    assertEquals(nullValue, mutation.getInsert().getValues(0).getValues(1));
    assertEquals(nullValue, mutation.getInsert().getValues(0).getValues(2));
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
  public void testCopyIn_Nulls() throws SQLException, IOException {
    setupCopyInformationSchemaResults();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      PGConnection pgConnection = connection.unwrap(PGConnection.class);
      CopyManager copyManager = pgConnection.getCopyAPI();
      long copyCount =
          copyManager.copyIn(
              "copy all_types from stdin;",
              new FileInputStream("./src/test/resources/all_types_data_nulls.txt"));
      assertEquals(1L, copyCount);
    }

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
    CommitRequest commitRequest = commitRequests.get(0);
    assertEquals(1, commitRequest.getMutationsCount());

    Mutation mutation = commitRequest.getMutations(0);
    assertEquals(OperationCase.INSERT, mutation.getOperationCase());
    assertEquals(1, mutation.getInsert().getValuesCount());
    Value nullValue = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    // The first column is not null, as it is the primary key.
    for (int col = 1; col < mutation.getInsert().getColumnsCount(); col++) {
      assertEquals(nullValue, mutation.getInsert().getValues(0).getValues(col));
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

      assertEquals(
          "ERROR: FAILED_PRECONDITION: Record count: 2001 has exceeded the limit: 2000.\n"
              + "\n"
              + "The number of mutations per record is equal to the number of columns in the record plus the number of indexed columns in the record. The maximum number of mutations in one transaction is 20000.\n"
              + "\n"
              + "Execute `SET AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'` before executing a large COPY operation to instruct PGAdapter to automatically break large transactions into multiple smaller. This will make the COPY operation non-atomic.\n\n",
          exception.getMessage());
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

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
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

      // Wait for the CopyInResponse.
      boolean receivedCopyInResponse = false;
      while (!receivedCopyInResponse) {
        int command = stream.receiveChar();
        if (command == 'G') {
          receivedCopyInResponse = true;
        }
        // Just skip everything in the message.
        length = stream.receiveInteger4();
        stream.skip(length - 4);
      }

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

      String error = "Error";
      stream.sendChar('f');
      stream.sendInteger4(4 + error.getBytes(StandardCharsets.UTF_8).length + 1);
      stream.send(error.getBytes(StandardCharsets.UTF_8));
      stream.sendChar(0);
      stream.flush();

      boolean receivedReadyForQuery = false;
      StringBuilder errorMessage = new StringBuilder();
      while (!receivedReadyForQuery) {
        int command = stream.receiveChar();
        if (command == 'Z') {
          receivedReadyForQuery = true;
          // Skip the status flag.
          stream.skip(1);
        } else if (command == 'E') {
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
              + "Expected CopyData ('d'), CopyDone ('c') or CopyFail ('f') messages, got: 'Q'\n"
              + "ERROR\n"
              + "P0001\n"
              + "CANCELLED: Error\n",
          errorMessage.toString());

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
  public void testCopyInBatchPsqlWithError() throws Exception {
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
      // The 'INSERT INTO FOO VALUES ('abc')' statement will return an error.
      writer.write(
          "SELECT 1\\;copy users from stdin\\;INSERT INTO FOO VALUES ('abc')\\;copy users from stdin\\;SELECT 2;\n"
              + "1\t2\t3\n"
              + "\\.\n"
              + "\n"
              + "\\q\n");
      writer.flush();
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    assertEquals(
        "ERROR:  INVALID_ARGUMENT: com.google.api.gax.rpc.InvalidArgumentException: io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Statement is invalid.",
        errors);
    assertEquals("", output);
    int res = process.waitFor();
    assertEquals(0, res);

    // The batch is executed as one implicit transaction. That transaction should be rolled back.
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
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

      // Wait for the first CopyInResponse.
      boolean receivedCopyInResponse = false;
      while (!receivedCopyInResponse) {
        int command = stream.receiveChar();
        if (command == 'G') {
          receivedCopyInResponse = true;
        }
        // Just skip everything in the message.
        length = stream.receiveInteger4();
        stream.skip(length - 4);
      }

      // Send CopyData + CopyDone for the first copy operation.
      stream.sendChar('d');
      stream.sendInteger4(row1.length + 4);
      stream.send(row1);
      stream.sendChar('c');
      stream.sendInteger4(4);
      stream.flush();

      // Wait for CommandComplete
      boolean receivedCommandComplete = false;
      while (!receivedCommandComplete) {
        int command = stream.receiveChar();
        if (command == 'C') {
          receivedCommandComplete = true;
        }
        // Just skip everything in the message.
        length = stream.receiveInteger4();
        stream.skip(length - 4);
      }

      // Wait for the second CopyInResponse.
      receivedCopyInResponse = false;
      while (!receivedCopyInResponse) {
        int command = stream.receiveChar();
        if (command == 'G') {
          receivedCopyInResponse = true;
        }
        // Just skip everything in the message.
        length = stream.receiveInteger4();
        stream.skip(length - 4);
      }

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
        }
        // Just skip everything in the message.
        length = stream.receiveInteger4();
        stream.skip(length - 4);
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

      // Wait for the CopyInResponse.
      boolean receivedCopyInResponse = false;
      while (!receivedCopyInResponse) {
        int command = stream.receiveChar();
        if (command == 'G') {
          receivedCopyInResponse = true;
        }
        // Just skip everything in the message.
        length = stream.receiveInteger4();
        stream.skip(length - 4);
      }

      // Send CopyData + CopyDone for the first copy operation.
      stream.sendChar('d');
      stream.sendInteger4(row1.length + 4);
      stream.send(row1);
      stream.sendChar('c');
      stream.sendInteger4(4);
      stream.flush();

      // Wait for CommandComplete
      boolean receivedCommandComplete = false;
      while (!receivedCommandComplete) {
        int command = stream.receiveChar();
        if (command == 'C') {
          receivedCommandComplete = true;
        }
        // Just skip everything in the message.
        length = stream.receiveInteger4();
        stream.skip(length - 4);
      }

      // Wait for the second CopyInResponse.
      receivedCopyInResponse = false;
      while (!receivedCopyInResponse) {
        int command = stream.receiveChar();
        if (command == 'G') {
          receivedCopyInResponse = true;
        }
        // Just skip everything in the message.
        length = stream.receiveInteger4();
        stream.skip(length - 4);
      }

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
          // Skip the status flag.
          stream.skip(1);
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
      assertEquals("ERROR\n" + "P0001\n" + "CANCELLED: Changed my mind\n", errorMessage.toString());
    }
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  private static boolean isBashAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand = new String[] {"bash", "--version"};
    builder.command(psqlCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  private static void assumeLocalPostgreSQLSetup() {
    assumeFalse(
        "This test the environment variable POSTGRES_HOST to point to a valid PostgreSQL host",
        Strings.isNullOrEmpty(POSTGRES_HOST));
    assumeFalse(
        "This test the environment variable POSTGRES_PORT to point to a valid PostgreSQL port number",
        Strings.isNullOrEmpty(POSTGRES_PORT));
    assumeFalse(
        "This test the environment variable POSTGRES_USER to point to a valid PostgreSQL user",
        Strings.isNullOrEmpty(POSTGRES_USER));
    assumeFalse(
        "This test the environment variable POSTGRES_DATABASE to point to a valid PostgreSQL database",
        Strings.isNullOrEmpty(POSTGRES_DATABASE));
  }

  @Test
  public void testCopyBinaryPsql() throws Exception {
    assumeLocalPostgreSQLSetup();
    assumeTrue("This test requires psql", isPsqlAvailable());
    assumeTrue("This test requires bash", isBashAvailable());

    setupCopyInformationSchemaResults();

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(
        "bash",
        "-c",
        "psql"
            + " -h "
            + POSTGRES_HOST
            + " -p "
            + POSTGRES_PORT
            + " -U "
            + POSTGRES_USER
            + " -d "
            + POSTGRES_DATABASE
            + " -c \"copy (\n"
            + "    select 1::bigint as id, 30::bigint as age, 'One'::varchar as name\n"
            + "    union all\n"
            + "    select null::bigint as id, null::bigint as age, null::varchar as name\n"
            + "    union all\n"
            + "    select 2::bigint as id, 40::bigint as age, 'Two'::varchar as name\n"
            + "  ) to stdout binary\" "
            + "  | psql "
            + " -h "
            + (useDomainSocket ? "/tmp" : "localhost")
            + " -p "
            + pgServer.getLocalPort()
            + " -c \"copy users from stdin binary;\"\n");
    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(1, commitRequest.getMutationsCount());
    Mutation mutation = commitRequest.getMutations(0);
    assertEquals(OperationCase.INSERT, mutation.getOperationCase());
    assertEquals(3, mutation.getInsert().getValuesCount());
    assertEquals("id", mutation.getInsert().getColumns(0));
    assertEquals("age", mutation.getInsert().getColumns(1));
    assertEquals("name", mutation.getInsert().getColumns(2));
    ListValue row1 = mutation.getInsert().getValues(0);
    assertEquals("1", row1.getValues(0).getStringValue());
    assertEquals("30", row1.getValues(1).getStringValue());
    assertEquals("One", row1.getValues(2).getStringValue());
    ListValue row2 = mutation.getInsert().getValues(1);
    assertTrue(row2.getValues(0).hasNullValue());
    assertTrue(row2.getValues(1).hasNullValue());
    assertTrue(row2.getValues(2).hasNullValue());
    ListValue row3 = mutation.getInsert().getValues(2);
    assertEquals("2", row3.getValues(0).getStringValue());
    assertEquals("40", row3.getValues(1).getStringValue());
    assertEquals("Two", row3.getValues(2).getStringValue());
  }

  @Test
  public void testCopyBinaryPsql_wrongTypeWithValidLength() throws Exception {
    assumeLocalPostgreSQLSetup();
    assumeTrue("This test requires psql", isPsqlAvailable());
    assumeTrue("This test requires bash", isBashAvailable());

    setupCopyInformationSchemaResults();

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(
        "bash",
        "-c",
        "psql"
            + " -h "
            + POSTGRES_HOST
            + " -p "
            + POSTGRES_PORT
            + " -U "
            + POSTGRES_USER
            + " -d "
            + POSTGRES_DATABASE
            + " -c \"copy (\n"
            // Note: float8 has the same length as bigint, so the receiving part will not notice
            // that the type is invalid. Instead, we will get a wrong value in the column.
            + "    select 1::float8 as id, 30::bigint as age, 'One'::varchar as name\n"
            + "  ) to stdout binary\" "
            + "  | psql "
            + " -h "
            + (useDomainSocket ? "/tmp" : "localhost")
            + " -p "
            + pgServer.getLocalPort()
            + " -c \"copy users from stdin binary;\"\n");
    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(1, commitRequest.getMutationsCount());
    Mutation mutation = commitRequest.getMutations(0);
    assertEquals(OperationCase.INSERT, mutation.getOperationCase());
    ListValue row1 = mutation.getInsert().getValues(0);
    // The float8 is interpreted as a bigint.
    assertEquals("4607182418800017408", row1.getValues(0).getStringValue());
    assertEquals("30", row1.getValues(1).getStringValue());
    assertEquals("One", row1.getValues(2).getStringValue());
  }

  @Test
  public void testCopyBinaryPsql_wrongTypeWithInvalidLength() throws Exception {
    assumeLocalPostgreSQLSetup();
    assumeTrue("This test requires psql", isPsqlAvailable());
    assumeTrue("This test requires bash", isBashAvailable());

    setupCopyInformationSchemaResults();

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(
        "bash",
        "-c",
        "psql"
            + " -h "
            + POSTGRES_HOST
            + " -p "
            + POSTGRES_PORT
            + " -U "
            + POSTGRES_USER
            + " -d "
            + POSTGRES_DATABASE
            + " -c \"copy (\n"
            // Note: int2 has an invalid length for a bigint column.
            + "    select 1::int2 as id, 30::bigint as age, 'One'::varchar as name\n"
            + "  ) to stdout binary\" "
            + "  | psql "
            + " -h "
            + (useDomainSocket ? "/tmp" : "localhost")
            + " -p "
            + pgServer.getLocalPort()
            + " -c \"copy users from stdin binary;\"\n");
    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(1, res);
    StringBuilder error = new StringBuilder();
    Scanner scanner = new Scanner(new InputStreamReader(process.getErrorStream()));
    while (scanner.hasNextLine()) {
      error.append(scanner.nextLine()).append('\n');
    }
    assertEquals("ERROR:  INVALID_ARGUMENT: Invalid length for int8: 2\n", error.toString());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testCopyBinaryPsql_Int4ToInt8() throws Exception {
    assumeLocalPostgreSQLSetup();
    assumeTrue("This test requires psql", isPsqlAvailable());
    assumeTrue("This test requires bash", isBashAvailable());

    setupCopyInformationSchemaResults();

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(
        "bash",
        "-c",
        "psql"
            + " -h "
            + POSTGRES_HOST
            + " -p "
            + POSTGRES_PORT
            + " -U "
            + POSTGRES_USER
            + " -d "
            + POSTGRES_DATABASE
            + " -c \"copy (\n"
            // Note: int4 has an invalid length for a bigint column, but PGAdapter specifically
            // allows this conversion.
            + "    select 1::int4 as id, 30::bigint as age, 'One'::varchar as name\n"
            + "  ) to stdout binary\" "
            + "  | psql "
            + " -h "
            + (useDomainSocket ? "/tmp" : "localhost")
            + " -p "
            + pgServer.getLocalPort()
            + " -c \"copy users from stdin binary;\"\n");
    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(1, commitRequest.getMutationsCount());
    Mutation mutation = commitRequest.getMutations(0);
    assertEquals(OperationCase.INSERT, mutation.getOperationCase());
    assertEquals(1, mutation.getInsert().getValuesCount());
    ListValue row = mutation.getInsert().getValues(0);
    assertEquals("1", row.getValues(0).getStringValue());
    assertEquals("30", row.getValues(1).getStringValue());
    assertEquals("One", row.getValues(2).getStringValue());
  }

  @Test
  public void testCopyAllTypesBinaryPsql() throws Exception {
    assumeLocalPostgreSQLSetup();
    assumeTrue("This test requires psql", isPsqlAvailable());
    assumeTrue("This test requires bash", isBashAvailable());

    setupCopyInformationSchemaResults();

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(
        "bash",
        "-c",
        "psql"
            + " -h "
            + POSTGRES_HOST
            + " -p "
            + POSTGRES_PORT
            + " -U "
            + POSTGRES_USER
            + " -d "
            + POSTGRES_DATABASE
            + " -c \"copy (\n"
            + "    select 1::bigint as col_bigint, true::bool as col_bool,\n"
            + "           sha256('hello world')::bytea as col_bytea,\n"
            + "           3.14::float8 as col_float8, 100::bigint as col_int,\n"
            + "           6.626::numeric as col_numeric,\n"
            + "           '2022-07-07 08:16:48.123456+02:00'::timestamptz as col_timestamptz,\n"
            + "           '2022-07-07'::date as col_date, 'hello world'::varchar as col_varchar\n"
            + "    union all\n"
            + "    select null::bigint as col_bigint, null::bool as col_bool,\n"
            + "           null::bytea as col_bytea, null::float8 as col_float8,\n"
            + "           null::bigint as col_int, null::numeric as col_numeric,"
            + "           null::timestamptz as col_timestamptz, null::date as col_date,"
            + "           null::varchar as col_varchar\n"
            + "  ) to stdout binary\" "
            + "  | psql "
            + " -h "
            + (useDomainSocket ? "/tmp" : "localhost")
            + " -p "
            + pgServer.getLocalPort()
            + " -c \"copy all_types from stdin binary;\"\n");
    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(1, commitRequest.getMutationsCount());
    Mutation mutation = commitRequest.getMutations(0);
    assertEquals(OperationCase.INSERT, mutation.getOperationCase());
    assertEquals(2, mutation.getInsert().getValuesCount());
    assertEquals("col_bigint", mutation.getInsert().getColumns(0));
    assertEquals("col_bool", mutation.getInsert().getColumns(1));
    assertEquals("col_bytea", mutation.getInsert().getColumns(2));
    assertEquals("col_float8", mutation.getInsert().getColumns(3));
    assertEquals("col_int", mutation.getInsert().getColumns(4));
    assertEquals("col_numeric", mutation.getInsert().getColumns(5));
    assertEquals("col_timestamptz", mutation.getInsert().getColumns(6));
    assertEquals("col_date", mutation.getInsert().getColumns(7));
    assertEquals("col_varchar", mutation.getInsert().getColumns(8));

    ListValue row1 = mutation.getInsert().getValues(0);
    assertEquals("1", row1.getValues(0).getStringValue());
    assertTrue(row1.getValues(1).getBoolValue());
    assertArrayEquals(
        Hashing.sha256().hashString("hello world", StandardCharsets.UTF_8).asBytes(),
        Base64.getDecoder().decode(row1.getValues(2).getStringValue()));
    assertEquals(3.14, row1.getValues(3).getNumberValue(), 0.0);
    assertEquals("100", row1.getValues(4).getStringValue());
    assertEquals("6.626", row1.getValues(5).getStringValue());
    assertEquals("2022-07-07T06:16:48.123456000Z", row1.getValues(6).getStringValue());
    assertEquals("2022-07-07", row1.getValues(7).getStringValue());
    assertEquals("hello world", row1.getValues(8).getStringValue());

    ListValue row2 = mutation.getInsert().getValues(1);
    for (int i = 0; i < row2.getValuesCount(); i++) {
      assertTrue(row2.getValues(i).hasNullValue());
    }
  }

  private void setupCopyInformationSchemaResults() {
    setupCopyInformationSchemaResults(true);
  }

  private void setupCopyInformationSchemaResults(boolean tableFound) {
    setupCopyInformationSchemaResults(mockSpanner, tableFound);
  }

  public static void setupCopyInformationSchemaResults(
      MockSpannerServiceImpl mockSpanner, boolean tableFound) {
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
