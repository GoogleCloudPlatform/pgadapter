// Copyright 2026 Google LLC
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import io.opentelemetry.api.OpenTelemetry;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests that verify that {@link SpannerPGConnector} correctly routes a real PostgreSQL client tool
 * to PGAdapter. These tests are skipped if psql is not installed.
 */
@RunWith(JUnit4.class)
public class SpannerPGConnectorMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void checkPsqlAvailable() {
    assumeTrue("This test requires psql to be installed", isPsqlAvailable());
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    // Taken from the connector itself so these tests cannot drift from what it configures.
    DdlTransactionMode ddlTransactionMode =
        SpannerPGConnector.createOptionsMetadata(ImmutableMap.of()).getDdlTransactionMode();
    doStartMockSpannerAndPgAdapterServers(
        createMockSpannerThatReturnsOneQueryPartition(),
        "my-database",
        builder -> builder.setDdlTransactionMode(ddlTransactionMode),
        OpenTelemetry.noop());
  }

  // Do not add a clearRequests() method here: AbstractMockServerTest already declares an @Before
  // method with that name, and redeclaring it overrides rather than adds, leaving the admin mocks
  // un-reset between tests.

  private static boolean isPsqlAvailable() {
    try {
      return new ProcessBuilder("psql", "--version").start().waitFor() == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  /** Runs the given command in a background thread and returns its exit code. */
  private static int runCommand(String database, String... command) throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Integer> future =
          executor.submit(() -> SpannerPGConnector.runCommand(pgServer, database, command));
      // Bounded: a psql that hangs or prompts would otherwise stall the entire build.
      return future.get(60L, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Verifies that the client tool connects to the database that was selected with -d, even though
   * the tool itself does not receive a -d argument. This is the PGDATABASE environment variable
   * that is set by {@link SpannerPGConnector}.
   */
  @Test
  public void testDatabaseIsPassedThroughPgDatabase() throws Exception {
    String sql = "select value from my_table where id=1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    assertEquals(0, runCommand("my-database", "psql", "-c", sql));

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testExitCodeIsPropagated() throws Exception {
    String sql = "select value from my_table where id=1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    assertEquals(0, runCommand("my-database", "psql", "-v", "ON_ERROR_STOP=1", "-c", sql));
    // An unknown statement must result in a non-zero exit code.
    assertNotEquals(
        0,
        runCommand(
            "my-database", "psql", "-v", "ON_ERROR_STOP=1", "-c", "select * from unknown_table"));
  }

  /**
   * Verifies that a -d argument on the command line of the client tool is passed through to the
   * tool unmodified.
   */
  @Test
  public void testDatabaseArgumentIsPassedThrough() throws Exception {
    String sql = "select value from my_table where id=1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    assertEquals(0, runCommand(null, "psql", "-d", "my-database", "-c", sql));

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  private static ImmutableList<UpdateDatabaseDdlRequest> getDdlRequests() {
    return mockDatabaseAdmin.getRequests().stream()
        .filter(request -> request instanceof UpdateDatabaseDdlRequest)
        .map(request -> (UpdateDatabaseDdlRequest) request)
        .collect(ImmutableList.toImmutableList());
  }

  /** A mixed DDL/DML batch is rejected by the default {@link DdlTransactionMode#Batch}. */
  @Test
  public void testDdlAndDmlInTheSameMessageIsAllowed() throws Exception {
    String ddl = "create table my_table (id bigint primary key)";
    String dml = "insert into my_table (id) values (1)";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(dml), 1L));
    addDdlResponseToSpannerAdmin();

    assertEquals(
        0,
        runCommand("my-database", "psql", "-v", "ON_ERROR_STOP=1", "-c", ddl + "; " + dml + ";"));

    assertEquals(1, getDdlRequests().size());
    assertEquals(ImmutableList.of(ddl), getDdlRequests().get(0).getStatementsList());
  }

  /**
   * {@code psql --single-transaction} sends each statement as its own message, but the DDL is
   * collected into a single {@link UpdateDatabaseDdlRequest} at commit.
   */
  @Test
  public void testDdlInSingleTransactionIsSentAsOneBatch() throws Exception {
    ImmutableList<String> statements =
        ImmutableList.of(
            "create table table1 (id bigint primary key)",
            "create table table2 (id bigint primary key)",
            "create table table3 (id bigint primary key)");
    addDdlResponseToSpannerAdmin();

    Path schema = Files.createTempFile("schema", ".sql");
    try {
      Files.write(
          schema,
          statements.stream().map(sql -> sql + ";").collect(ImmutableList.toImmutableList()),
          StandardCharsets.UTF_8);

      assertEquals(
          0,
          runCommand(
              "my-database",
              "psql",
              "-v",
              "ON_ERROR_STOP=1",
              "--single-transaction",
              "-f",
              schema.toString()));
    } finally {
      Files.deleteIfExists(schema);
    }

    assertEquals(1, getDdlRequests().size());
    assertEquals(statements, getDdlRequests().get(0).getStatementsList());
  }
}
