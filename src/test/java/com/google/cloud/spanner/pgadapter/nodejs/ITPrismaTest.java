// Copyright 2024 Google LLC
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

import static com.google.cloud.spanner.pgadapter.PgAdapterTestEnv.getOnlyAllTypesDdl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(IntegrationTest.class)
public class ITPrismaTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  @BeforeClass
  public static void setup() throws Exception {
    NodeJSTest.installDependencies("prisma-tests");

    testEnv.setUp();
    database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection
          .createStatement()
          .execute(
              "insert into \"User\" (id, name, email) values "
                  + "(1, 'Peter', 'peter@prisma.com'), "
                  + "(2, 'Alice', 'alice@prisma.com'), "
                  + "(3, 'Hannah', 'hannah@prisma.com')");
    }
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @After
  public void cleanupTestData() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("truncate \"AllTypes\"");
      connection.createStatement().execute("truncate \"Post\"");
      connection.createStatement().execute("delete from \"User\" where id not in ('1','2','3')");
    }
  }

  private static Iterable<String> getDdlStatements() {
    return ImmutableList.of(
        "create table \"User\" (\n"
            + "    id varchar(36) not null primary key,\n"
            + "    email varchar not null,\n"
            + "    name varchar\n"
            + ")",
        "create unique index idx_user_email on \"User\" (email)",
        "create table \"Post\" (\n"
            + "    id varchar(36) not null primary key,\n"
            + "    title varchar not null,\n"
            + "    content text,\n"
            + "    published bool default false,\n"
            + "    \"authorId\" varchar(36),\n"
            + "    constraint fk_post_user foreign key (\"authorId\") references \"User\" (id)\n"
            + ")",
        getOnlyAllTypesDdl().get(0).replace("create table all_types", "create table \"AllTypes\""));
  }

  private static String createUrl() {
    return String.format(
        "jdbc:postgresql://%s:%d/%s",
        testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort(), database.getId().getDatabase());
  }

  @Test
  public void testSelect1() throws Exception {
    String output = runTest("testSelect1", testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    assertEquals("[ { '?column?': 1n } ]\n", output);
  }

  @Test
  public void testFindAllUsers() throws Exception {
    String output =
        runTest("testFindAllUsers", testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    assertEquals(
        "[\n"
            + "  { id: '1', email: 'peter@prisma.com', name: 'Peter' },\n"
            + "  { id: '2', email: 'alice@prisma.com', name: 'Alice' },\n"
            + "  { id: '3', email: 'hannah@prisma.com', name: 'Hannah' }\n"
            + "]\n",
        output);
  }

  @Test
  public void testFindUniqueUser() throws IOException, InterruptedException {
    String output =
        runTest("testFindUniqueUser", testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    assertEquals("{ id: '1', email: 'peter@prisma.com', name: 'Peter' }\n", output);
  }

  @Test
  public void testFindTwoUniqueUsers() throws IOException, InterruptedException {
    for (boolean stale : new boolean[] {false, true}) {
      String output =
          runTest(
              stale ? "testFindTwoUniqueUsersUsingStaleRead" : "testFindTwoUniqueUsers",
              testEnv.getPGAdapterHost(),
              testEnv.getPGAdapterPort());
      assertEquals(
          "{ id: '1', email: 'peter@prisma.com', name: 'Peter' }\n"
              + "{ id: '2', email: 'alice@prisma.com', name: 'Alice' }\n",
          output);
    }
  }

  @Test
  public void testCreateUser() throws IOException, InterruptedException {
    assumeFalse(
        "The emulator does not return the columns of a DML statement with a RETURNING clause, which breaks the creation of a prepared statement",
        IntegrationTest.isRunningOnEmulator());

    String output =
        runTest("testCreateUser", testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    assertEquals(
        "{\n"
            + "  id: '2373a81d-772c-4221-adf0-06965bc02c2c',\n"
            + "  email: 'alice@prisma.io',\n"
            + "  name: 'Alice'\n"
            + "}\n",
        output);
  }

  @Test
  public void testNestedWrite() throws IOException, InterruptedException {
    assumeFalse(
        "The emulator does not return the columns of a DML statement with a RETURNING clause, which breaks the creation of a prepared statement",
        IntegrationTest.isRunningOnEmulator());

    String output =
        runTest("testNestedWrite", testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    assertEquals("{ id: '4', email: 'alice2@prisma.io', name: null }\n", output);
  }

  @Test
  public void testReadOnlyTransaction() throws IOException, InterruptedException {
    String output =
        runTest("testReadOnlyTransaction", testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    assertEquals(
        "{ id: '1', email: 'peter@prisma.com', name: 'Peter' }\n"
            + "{ id: '2', email: 'alice@prisma.com', name: 'Alice' }\n",
        output);
  }

  @Test
  public void testCreateAllTypes() throws IOException, InterruptedException {
    assumeFalse(
        "The emulator does not return the columns of a DML statement with a RETURNING clause, which breaks the creation of a prepared statement",
        IntegrationTest.isRunningOnEmulator());

    String output =
        runTest("testCreateAllTypes", testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: true,\n"
            + "  col_bytea: <Buffer 74 65 73 74>,\n"
            + "  col_float4: 3.14,\n"
            + "  col_float8: 3.14,\n"
            + "  col_int: 100,\n"
            + "  col_numeric: 6.626,\n"
            + "  col_timestamptz: 2022-02-16T13:18:02.123Z,\n"
            + "  col_date: 2022-03-29T00:00:00.000Z,\n"
            + "  col_varchar: 'test',\n"
            + "  col_jsonb: { key: 'value' },\n"
            + "  col_array_bigint: [],\n"
            + "  col_array_bool: [],\n"
            + "  col_array_bytea: [],\n"
            + "  col_array_float4: [],\n"
            + "  col_array_float8: [],\n"
            + "  col_array_int: [],\n"
            + "  col_array_numeric: [],\n"
            + "  col_array_timestamptz: [],\n"
            + "  col_array_date: [],\n"
            + "  col_array_varchar: [],\n"
            + "  col_array_jsonb: []\n"
            + "}\n",
        output);
  }

  @Test
  public void testUpdateAllTypes() throws Exception {
    assumeFalse(
        "The emulator does not return the columns of a DML statement with a RETURNING clause, which breaks the creation of a prepared statement",
        IntegrationTest.isRunningOnEmulator());

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("insert into \"AllTypes\" (col_bigint) values (1)");
    }

    String output =
        runTest("testUpdateAllTypes", testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: false,\n"
            + "  col_bytea: <Buffer 75 70 64 61 74 65 64>,\n"
            + "  col_float4: 3.14,\n"
            + "  col_float8: 6.626,\n"
            + "  col_int: -100,\n"
            + "  col_numeric: 3.14,\n"
            + "  col_timestamptz: 2023-03-13T05:40:02.123Z,\n"
            + "  col_date: 2023-03-13T00:00:00.000Z,\n"
            + "  col_varchar: 'updated',\n"
            + "  col_jsonb: { key: 'updated' },\n"
            + "  col_array_bigint: [],\n"
            + "  col_array_bool: [],\n"
            + "  col_array_bytea: [],\n"
            + "  col_array_float4: [],\n"
            + "  col_array_float8: [],\n"
            + "  col_array_int: [],\n"
            + "  col_array_numeric: [],\n"
            + "  col_array_timestamptz: [],\n"
            + "  col_array_date: [],\n"
            + "  col_array_varchar: [],\n"
            + "  col_array_jsonb: []\n"
            + "}\n",
        output);
  }

  @Ignore("INSERT OR IGNORE ... RETURNING is not supported")
  @Test
  public void testUpsertAllTypes() throws IOException, InterruptedException {
    String output =
        runTest("testUpsertAllTypes", testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: false,\n"
            + "  col_bytea: <Buffer 75 70 64 61 74 65 64>,\n"
            + "  col_float4: 3.14,\n"
            + "  col_float8: 6.626,\n"
            + "  col_int: -100,\n"
            + "  col_numeric: 3.14,\n"
            + "  col_timestamptz: 2023-03-13T05:40:02.123Z,\n"
            + "  col_date: 2023-03-13T00:00:00.000Z,\n"
            + "  col_varchar: 'updated',\n"
            + "  col_jsonb: { key: 'updated' },\n"
            + "  col_array_bigint: [],\n"
            + "  col_array_bool: [],\n"
            + "  col_array_bytea: [],\n"
            + "  col_array_float4: [],\n"
            + "  col_array_float8: [],\n"
            + "  col_array_int: [],\n"
            + "  col_array_numeric: [],\n"
            + "  col_array_timestamptz: [],\n"
            + "  col_array_date: [],\n"
            + "  col_array_varchar: [],\n"
            + "  col_array_jsonb: []\n"
            + "}\n",
        output);
  }

  @Test
  public void testDeleteAllTypes() throws Exception {
    assumeFalse(
        "The emulator does not return the columns of a DML statement with a RETURNING clause, which breaks the creation of a prepared statement",
        IntegrationTest.isRunningOnEmulator());

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("insert into \"AllTypes\" (col_bigint) values (1)");
    }

    String output =
        runTest("testDeleteAllTypes", testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: null,\n"
            + "  col_bytea: null,\n"
            + "  col_float4: null,\n"
            + "  col_float8: null,\n"
            + "  col_int: null,\n"
            + "  col_numeric: null,\n"
            + "  col_timestamptz: null,\n"
            + "  col_date: null,\n"
            + "  col_varchar: null,\n"
            + "  col_jsonb: null,\n"
            + "  col_array_bigint: [],\n"
            + "  col_array_bool: [],\n"
            + "  col_array_bytea: [],\n"
            + "  col_array_float4: [],\n"
            + "  col_array_float8: [],\n"
            + "  col_array_int: [],\n"
            + "  col_array_numeric: [],\n"
            + "  col_array_timestamptz: [],\n"
            + "  col_array_date: [],\n"
            + "  col_array_varchar: [],\n"
            + "  col_array_jsonb: []\n"
            + "}\n",
        output);
  }

  @Test
  public void testCreateManyAllTypes() throws IOException, InterruptedException {
    String output =
        runTest("testCreateManyAllTypes", testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    assertEquals("{ count: 2 }\n", output);
  }

  @Test
  public void testCreateMultipleUsersWithoutTransaction() throws Exception {
    assumeFalse(
        "The emulator does not return the columns of a DML statement with a RETURNING clause, which breaks the creation of a prepared statement",
        IntegrationTest.isRunningOnEmulator());

    String output =
        runTest(
            "testCreateMultipleUsersWithoutTransaction",
            testEnv.getPGAdapterHost(),
            testEnv.getPGAdapterPort());
    assertEquals("Created two users\n", output);
  }

  @Test
  public void testCreateMultipleUsersInTransaction() throws Exception {
    assumeFalse(
        "The emulator does not return the columns of a DML statement with a RETURNING clause, which breaks the creation of a prepared statement",
        IntegrationTest.isRunningOnEmulator());

    String output =
        runTest(
            "testCreateMultipleUsersInTransaction",
            testEnv.getPGAdapterHost(),
            testEnv.getPGAdapterPort());
    assertEquals("Created two users\n", output);
  }

  @Test
  public void testUnhandledErrorInTransaction() throws Exception {
    String output =
        runTest(
            "testUnhandledErrorInTransaction",
            testEnv.getPGAdapterHost(),
            testEnv.getPGAdapterPort());
    assertTrue(output, output.contains("Transaction failed:"));
    assertTrue(output, output.contains("Unique constraint failed"));
  }

  @Test
  public void testHandledErrorInTransaction() throws Exception {
    String output =
        runTest(
            "testHandledErrorInTransaction",
            testEnv.getPGAdapterHost(),
            testEnv.getPGAdapterPort());

    assertTrue(output, output.contains("Transaction failed:"));
    assertTrue(output, output.contains("Insert statement failed:"));
    assertTrue(output, output.contains("Unique constraint failed"));
    assertTrue(
        output,
        output.contains(
            "current transaction is aborted, commands ignored until end of transaction block"));
    assertFalse(output, output.contains("Created user with id 2"));
  }

  @Test
  public void testTransactionIsolationLevel() throws Exception {
    String output =
        runTest(
            "testTransactionIsolationLevel",
            testEnv.getPGAdapterHost(),
            testEnv.getPGAdapterPort());

    assertTrue(output, output.contains("Transaction failed:"));
    assertTrue(
        output,
        output.contains(
            "current transaction is aborted, commands ignored until end of transaction block"));
  }

  static String runTest(String testName, String host, int port)
      throws IOException, InterruptedException {
    return NodeJSTest.runTest("prisma-tests", testName, host, port, "db");
  }
}
