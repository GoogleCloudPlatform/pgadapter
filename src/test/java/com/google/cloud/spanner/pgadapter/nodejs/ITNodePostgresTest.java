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

package com.google.cloud.spanner.pgadapter.nodejs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class ITNodePostgresTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  @Parameter public boolean useDomainSocket;

  @Parameters(name = "useDomainSocket = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled() ? new Object[] {true, false} : new Object[] {false};
  }

  @BeforeClass
  public static void setup() throws Exception {
    testEnv.setUp();
    database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  private static Iterable<String> getDdlStatements() {
    return ImmutableList.of(
        "create table users (name varchar primary key)",
        "create table alltypes ("
            + "col_bigint bigint not null primary key, "
            + "col_bool bool, "
            + "col_bytea bytea, "
            + "col_float8 float8, "
            + "col_int int, "
            + "col_numeric numeric, "
            + "col_timestamptz timestamptz, "
            + "col_date date, "
            + "col_varchar varchar(100),"
            + "col_jsonb text)");
  }

  @Before
  public void clearTestData() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        ImmutableList.of(
            Mutation.delete("users", KeySet.all()), Mutation.delete("alltypes", KeySet.all())));
  }

  private void insertTestRow() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        ImmutableList.of(
            Mutation.newInsertOrUpdateBuilder("alltypes")
                .set("col_bigint")
                .to(1L)
                .set("col_bool")
                .to(true)
                .set("col_bytea")
                .to(ByteArray.copyFrom("test"))
                .set("col_float8")
                .to(3.14d)
                .set("col_int")
                .to(100)
                .set("col_numeric")
                .to(new BigDecimal("6.626"))
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("2022-02-16T14:18:02.123456789+01:00"))
                .set("col_date")
                .to(Date.parseDate("2022-03-29"))
                .set("col_varchar")
                .to("test")
                .set("col_jsonb")
                .to("{\"key\": \"value\"}")
                .build()));
  }

  private void insertNullTestRow() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        ImmutableList.of(
            Mutation.newInsertOrUpdateBuilder("alltypes")
                .set("col_bigint")
                .to(1L)
                .set("col_bool")
                .to((Boolean) null)
                .set("col_bytea")
                .to((ByteArray) null)
                .set("col_float8")
                .to((Double) null)
                .set("col_int")
                .to((Long) null)
                .set("col_numeric")
                .to(Value.pgNumeric(null))
                .set("col_timestamptz")
                .to((Timestamp) null)
                .set("col_date")
                .to((Date) null)
                .set("col_varchar")
                .to((String) null)
                .set("col_jsonb")
                .to((String) null)
                .build()));
  }

  private String getHost() {
    if (useDomainSocket) {
      return "/tmp";
    }
    return "localhost";
  }

  @Test
  public void testSelect1() throws Exception {
    String output = runTest("testSelect1", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("\n\nSELECT 1 returned: 1\n", output);
  }

  @Test
  public void testInsert() throws Exception {
    String output = runTest("testInsert", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("\n\nInserted 1 row(s)\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM users"))) {
      assertTrue(resultSet.next());
      assertEquals("foo", resultSet.getString("name"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testInsertExecutedTwice() throws Exception {
    String output = runTest("testInsertTwice", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("\n\nInserted 1 row(s)\nInserted 1 row(s)\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM users ORDER BY name"))) {
      assertTrue(resultSet.next());
      assertEquals("bar", resultSet.getString("name"));
      assertTrue(resultSet.next());
      assertEquals("foo", resultSet.getString("name"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testInsertAutoCommit() throws IOException, InterruptedException {
    String output = runTest("testInsertAutoCommit", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("\n\nInserted 1 row(s)\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM users"))) {
      assertTrue(resultSet.next());
      assertEquals("foo", resultSet.getString("name"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testInsertAllTypes() throws IOException, InterruptedException {
    String output = runTest("testInsertAllTypes", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("\n\nInserted 1 row(s)\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM alltypes"))) {
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getLong("col_bigint"));
      assertTrue(resultSet.getBoolean("col_bool"));
      assertEquals(
          ByteArray.copyFrom("some random string".getBytes(StandardCharsets.UTF_8)),
          resultSet.getBytes("col_bytea"));
      assertEquals(3.14d, resultSet.getDouble("col_float8"), 0.0d);
      assertEquals(100, resultSet.getLong("col_int"));
      assertEquals("234.54235", resultSet.getString("col_numeric"));
      assertEquals(
          Timestamp.parseTimestamp("2022-07-22T18:15:42.011Z"),
          resultSet.getTimestamp("col_timestamptz"));
      assertEquals(Date.parseDate("2022-07-22"), resultSet.getDate("col_date"));
      assertEquals("some-random-string", resultSet.getString("col_varchar"));
      assertEquals("{\"my_key\":\"my-value\"}", resultSet.getString("col_jsonb"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testInsertAllTypesNull() throws IOException, InterruptedException {
    String output =
        runTest("testInsertAllTypesNull", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("\n\nInserted 1 row(s)\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM alltypes"))) {
      assertTrue(resultSet.next());
      assertEquals(1L, resultSet.getLong("col_bigint"));
      assertTrue(resultSet.isNull("col_bool"));
      assertTrue(resultSet.isNull("col_bytea"));
      assertTrue(resultSet.isNull("col_float8"));
      assertTrue(resultSet.isNull("col_int"));
      assertTrue(resultSet.isNull("col_numeric"));
      assertTrue(resultSet.isNull("col_timestamptz"));
      assertTrue(resultSet.isNull("col_date"));
      assertTrue(resultSet.isNull("col_varchar"));
      assertTrue(resultSet.isNull("col_jsonb"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testInsertAllTypesPreparedStatement() throws IOException, InterruptedException {
    String output =
        runTest(
            "testInsertAllTypesPreparedStatement", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("\n\nInserted 1 row(s)\nInserted 1 row(s)\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM alltypes"))) {
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getLong("col_bigint"));
      assertTrue(resultSet.getBoolean("col_bool"));
      assertEquals(
          ByteArray.copyFrom("some random string".getBytes(StandardCharsets.UTF_8)),
          resultSet.getBytes("col_bytea"));
      assertEquals(3.14d, resultSet.getDouble("col_float8"), 0.0d);
      assertEquals(100, resultSet.getLong("col_int"));
      assertEquals("234.54235", resultSet.getString("col_numeric"));
      assertEquals(
          Timestamp.parseTimestamp("2022-07-22T18:15:42.011Z"),
          resultSet.getTimestamp("col_timestamptz"));
      assertEquals(Date.parseDate("2022-07-22"), resultSet.getDate("col_date"));
      assertEquals("some-random-string", resultSet.getString("col_varchar"));
      assertEquals("{\"my_key\":\"my-value\"}", resultSet.getString("col_jsonb"));

      assertTrue(resultSet.next());
      assertEquals(2L, resultSet.getLong("col_bigint"));
      assertTrue(resultSet.isNull("col_bool"));
      assertTrue(resultSet.isNull("col_bytea"));
      assertTrue(resultSet.isNull("col_float8"));
      assertTrue(resultSet.isNull("col_int"));
      assertTrue(resultSet.isNull("col_numeric"));
      assertTrue(resultSet.isNull("col_timestamptz"));
      assertTrue(resultSet.isNull("col_date"));
      assertTrue(resultSet.isNull("col_varchar"));
      assertTrue(resultSet.isNull("col_jsonb"));

      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testSelectAllTypes() throws IOException, InterruptedException {
    insertTestRow();

    String output = runTest("testSelectAllTypes", getHost(), testEnv.getServer().getLocalPort());
    assertEquals(
        "\n\nSelected {"
            + "\"col_bigint\":\"1\","
            + "\"col_bool\":true,"
            + "\"col_bytea\":{\"type\":\"Buffer\",\"data\":[116,101,115,116]},"
            + "\"col_float8\":3.14,"
            + "\"col_int\":\"100\","
            + "\"col_numeric\":\"6.626\","
            + "\"col_timestamptz\":\"2022-02-16T13:18:02.123Z\","
            + "\"col_date\":\"2022-03-29\","
            + "\"col_varchar\":\"test\","
            + "\"col_jsonb\":\"{\\\"key\\\": \\\"value\\\"}\""
            + "}\n",
        output);
  }

  @Test
  public void testSelectAllTypesNull() throws IOException, InterruptedException {
    insertNullTestRow();

    String output = runTest("testSelectAllTypes", getHost(), testEnv.getServer().getLocalPort());
    assertEquals(
        "\n\nSelected {"
            + "\"col_bigint\":\"1\","
            + "\"col_bool\":null,"
            + "\"col_bytea\":null,"
            + "\"col_float8\":null,"
            + "\"col_int\":null,"
            + "\"col_numeric\":null,"
            + "\"col_timestamptz\":null,"
            + "\"col_date\":null,"
            + "\"col_varchar\":null,"
            + "\"col_jsonb\":null"
            + "}\n",
        output);
  }

  @Test
  public void testErrorInReadWriteTransaction() throws IOException, InterruptedException {
    // Insert a row that will conflict with the new row that the test will try to insert.
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        ImmutableList.of(Mutation.newInsertBuilder("users").set("name").to("foo").build()));

    String output =
        runTest("testErrorInReadWriteTransaction", getHost(), testEnv.getServer().getLocalPort());
    assertEquals(
        "\n\nInsert error: error: com.google.api.gax.rpc.AlreadyExistsException: io.grpc.StatusRuntimeException: ALREADY_EXISTS: Row [foo] in table users already exists\n"
            + "Second insert failed with error: error: current transaction is aborted, commands ignored until end of transaction block\n"
            + "SELECT 1 returned: 1\n",
        output);
  }

  @Test
  public void testReadOnlyTransaction() throws Exception {
    String output =
        runTest("testReadOnlyTransaction", getHost(), testEnv.getServer().getLocalPort());

    assertEquals("\n\nexecuted read-only transaction\n", output);
  }

  @Test
  public void testReadOnlyTransactionWithError() throws Exception {
    String output =
        runTest("testReadOnlyTransactionWithError", getHost(), testEnv.getServer().getLocalPort());
    assertEquals(
        "\n\ncurrent transaction is aborted, commands ignored until end of transaction block\n"
            + "[ { '?column?': '2' } ]\n",
        output);
  }

  @Test
  public void testCopyTo() throws Exception {
    insertTestRow();

    String output = runTest("testCopyTo", getHost(), testEnv.getServer().getLocalPort());
    assertEquals(
        "\n\n1\tt\t\\\\x74657374\t3.14\t100\t6.626\t2022-02-16 13:18:02.123456789+00\t2022-03-29\ttest\t{\"key\": \"value\"}\n",
        output);
  }

  @Test
  public void testCopyFrom() throws Exception {
    String output = runTest("testCopyFrom", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("\n\nFinished copy operation\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM alltypes"))) {
      assertTrue(resultSet.next());
      assertEquals(100L, resultSet.getLong(0));
      assertFalse(resultSet.next());
    }
  }

  static String runTest(String testName, String host, int port)
      throws IOException, InterruptedException {
    return NodeJSTest.runTest(
        "node-postgres", testName, host, port, database.getId().getDatabase());
  }
}
