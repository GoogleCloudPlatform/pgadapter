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
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigDecimal;
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
public class ITDrizzleTest implements IntegrationTest {
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
    NodeJSTest.installDependencies("drizzle-tests");

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
        "create table posts ("
            + "id bigint primary key, "
            + "title varchar(255) not null, "
            + "user_name varchar(255) not null, "
            + "foreign key (user_name) references users(name))",
        "create table alltypes ("
            + "col_bigint bigint not null primary key, "
            + "col_bool bool, "
            + "col_bytea bytea, "
            + "col_float4 float8, " // Drizzle doublePrecision maps to Spanner float8
            + "col_float8 float8, "
            + "col_int bigint, "
            + "col_numeric numeric, "
            + "col_timestamptz timestamptz, "
            + "col_date date, "
            + "col_varchar varchar(100),"
            + "col_jsonb jsonb)");
  }

  @Before
  public void clearTestData() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        ImmutableList.of(
            Mutation.delete("posts", KeySet.all()),
            Mutation.delete("users", KeySet.all()),
            Mutation.delete("alltypes", KeySet.all())));
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
                .set("col_float4")
                .to(3.14d)
                .set("col_float8")
                .to(3.14d)
                .set("col_int")
                .to(100L)
                .set("col_numeric")
                .to(new BigDecimal("6.626"))
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("2022-02-16T14:18:02.123456Z"))
                .set("col_date")
                .to(Date.parseDate("2022-03-29"))
                .set("col_varchar")
                .to("testÄ")
                .set("col_jsonb")
                .to("{\"key\":\"value\"}")
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
    assertEquals("SELECT 1 returned: 1\n", output);
  }

  @Test
  @org.junit.Ignore("Lateral subqueries are not supported by Cloud Spanner")
  public void testSelectRelationalQueries() throws Exception {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        ImmutableList.of(
            Mutation.newInsertOrUpdateBuilder("users").set("name").to("Alice").build(),
            Mutation.newInsertOrUpdateBuilder("users").set("name").to("Bob").build(),
            Mutation.newInsertOrUpdateBuilder("posts")
                .set("id")
                .to(1L)
                .set("title")
                .to("First Post")
                .set("user_name")
                .to("Alice")
                .build(),
            Mutation.newInsertOrUpdateBuilder("posts")
                .set("id")
                .to(2L)
                .set("title")
                .to("Second Post")
                .set("user_name")
                .to("Alice")
                .build()));

    String output =
        runTest("testSelectRelationalQueries", getHost(), testEnv.getServer().getLocalPort());
    assertEquals(
        "Relational query returned: [{\"name\":\"Alice\",\"posts\":[{\"id\":1,\"title\":\"First Post\",\"user_name\":\"Alice\"},{\"id\":2,\"title\":\"Second Post\",\"user_name\":\"Alice\"}]},{\"name\":\"Bob\",\"posts\":[]}]\n",
        output);
  }

  @Test
  public void testInsert() throws Exception {
    String output = runTest("testInsert", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("Inserted 1 row(s)\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM users"))) {
      assertTrue(resultSet.next());
      assertEquals("foo", resultSet.getString("name"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testUpdate() throws Exception {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        ImmutableList.of(
            Mutation.newInsertOrUpdateBuilder("alltypes")
                .set("col_bigint")
                .to(1L)
                .set("col_varchar")
                .to("foo")
                .build()));

    String output = runTest("testUpdate", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("Updated 1 row(s)\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM alltypes"))) {
      assertTrue(resultSet.next());
      assertEquals("bar", resultSet.getString("col_varchar"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testDelete() throws Exception {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        ImmutableList.of(Mutation.newInsertOrUpdateBuilder("users").set("name").to("bar").build()));

    String output = runTest("testDelete", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("Deleted 1 row(s)\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM users"))) {
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testBatchDml() throws Exception {
    String output = runTest("testBatchDml", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("Executed Batch DML\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM users ORDER BY name"))) {
      assertTrue(resultSet.next());
      assertEquals("batch-bar", resultSet.getString("name"));
      assertTrue(resultSet.next());
      assertEquals("batch-foo", resultSet.getString("name"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testInsertExecutedTwice() throws Exception {
    String output = runTest("testInsertTwice", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("Inserted 1 row(s)\nInserted 1 row(s)\n", output);

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
  public void testInsertAutoCommit() throws Exception {
    String output = runTest("testInsertAutoCommit", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("Inserted 1 row(s)\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM users"))) {
      assertTrue(resultSet.next());
      assertEquals("foo", resultSet.getString("name"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testInsertAllTypes() throws Exception {
    String output = runTest("testInsertAllTypes", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("Inserted 1 row(s)\n", output);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT * FROM alltypes"))) {
      assertTrue(resultSet.next());
      assertEquals(1L, resultSet.getLong("col_bigint"));
      assertTrue(resultSet.getBoolean("col_bool"));
      assertEquals(ByteArray.copyFrom("some random string"), resultSet.getBytes("col_bytea"));
      assertEquals(3.14d, resultSet.getDouble("col_float4"), 0.0d);
      assertEquals(3.14d, resultSet.getDouble("col_float8"), 0.0d);
      assertEquals(100L, resultSet.getLong("col_int"));
      assertEquals("234.54235", resultSet.getString("col_numeric"));
      assertEquals(
          Timestamp.parseTimestamp("2022-07-22T18:15:42.011Z"),
          resultSet.getTimestamp("col_timestamptz"));
      assertEquals(Date.parseDate("2022-07-22"), resultSet.getDate("col_date"));
      assertEquals("some-random-string", resultSet.getString("col_varchar"));
      assertEquals("{\"my_key\": \"my-value\"}", resultSet.getPgJsonb("col_jsonb"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testSelectAllTypes() throws Exception {
    insertTestRow();

    String output = runTest("testSelectAllTypes", getHost(), testEnv.getServer().getLocalPort());

    assertTrue(output.contains("Selected {"));
    assertTrue(output.contains("\"col_bigint\":\"1\""));
    assertTrue(output.contains("\"col_bool\":true"));
    assertTrue(output.contains("\"col_bytea\":{\"type\":\"Buffer\",\"data\":[116,101,115,116]}"));
    assertTrue(output.contains("\"col_float4\":3.14"));
    assertTrue(output.contains("\"col_float8\":3.14"));
    assertTrue(output.contains("\"col_int\":\"100\""));
    assertTrue(output.contains("\"col_numeric\":\"6.626\""));
    assertTrue(output.contains("\"col_timestamptz\":\"2022-02-16T14:18:02.123Z\""));
    assertTrue(output.contains("\"col_date\":\"2022-03-29\""));
    assertTrue(output.contains("\"col_varchar\":\"testÄ\""));
    assertTrue(output.contains("\"col_jsonb\":{\"key\":\"value\"}"));
  }

  @Test
  public void testReadOnlyTransaction() throws Exception {
    String output =
        runTest("testReadOnlyTransaction", getHost(), testEnv.getServer().getLocalPort());
    assertEquals("executed read-only transaction\n", output);
  }

  static String runTest(String testName, String host, int port)
      throws IOException, InterruptedException {
    return NodeJSTest.runTest("drizzle-tests", testName, host, port, "db");
  }
}
