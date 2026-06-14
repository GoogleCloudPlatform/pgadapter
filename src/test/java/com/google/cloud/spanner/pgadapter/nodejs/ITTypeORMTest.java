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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(IntegrationTest.class)
public class ITTypeORMTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  @BeforeClass
  public static void setup() throws Exception {
    NodeJSTest.installDependencies("typeorm/data-test");

    testEnv.setUp();
    database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @Before
  public void setupTestData() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("truncate all_types");
      connection.createStatement().execute("truncate \"user\"");
      connection
          .createStatement()
          .execute(
              "insert into \"user\" (id, \"firstName\", \"lastName\", age) values "
                  + "(1, 'Timber', 'Saw', 25)");
      connection
          .createStatement()
          .execute(
              "insert into all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) values "
                  + "(1, true, 'some random string', 0.123456789, 123456789, 234.54235, '2022-07-22 18:15:42.011+00', '2022-07-22', 'some random string', '{\"key\":\"value\"}')");
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
      connection.createStatement().execute("truncate all_types");
      connection.createStatement().execute("truncate \"user\"");
    }
  }

  private static Iterable<String> getDdlStatements() {
    return ImmutableList.of(
        "create table \"user\" (id bigint not null primary key, \"firstName\" varchar, \"lastName\" varchar, age bigint)",
        "create table all_types ("
            + "col_bigint bigint not null primary key, "
            + "col_bool bool, "
            + "col_bytea bytea, "
            + "col_float8 float8, "
            + "col_int bigint, "
            + "col_numeric numeric, "
            + "col_timestamptz timestamptz, "
            + "col_date date, "
            + "col_varchar varchar(100), "
            + "col_jsonb jsonb)");
  }

  private static String createUrl() {
    return String.format(
        "jdbc:postgresql://%s:%d/%s",
        testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort(), database.getId().getDatabase());
  }

  private static String runTest(String testName) throws IOException, InterruptedException {
    return NodeJSTest.runTest(
        "typeorm/data-test",
        testName,
        testEnv.getPGAdapterHost(),
        testEnv.getPGAdapterPort(),
        database.getId().getDatabase());
  }

  @Test
  public void testFindOneUser() throws Exception {
    String output = runTest("findOneUser");
    assertEquals("Found user 1 with name Timber Saw\n", output);
  }

  @Test
  public void testCreateUser() throws Exception {
    String output = runTest("createUser");
    assertEquals("Found user 1 with name Timber Saw\n", output);
  }

  @Test
  public void testUpdateUser() throws Exception {
    String output = runTest("updateUser");
    assertEquals("Updated user 1\n", output);
  }

  @Test
  public void testDeleteUser() throws Exception {
    String output = runTest("deleteUser");
    assertEquals("Deleted user 1\n", output);
  }

  @Test
  public void testFindOneAllTypes() throws Exception {
    String output = runTest("findOneAllTypes");
    assertEquals(
        "Found row 1\n"
            + "AllTypes {\n"
            + "  col_bigint: '1',\n"
            + "  col_bool: true,\n"
            + "  col_bytea: <Buffer 73 6f 6d 65 20 72 61 6e 64 6f 6d 20 73 74 72 69 6e 67>,\n"
            + "  col_float8: 0.123456789,\n"
            + "  col_int: '123456789',\n"
            + "  col_numeric: '234.54235',\n"
            + "  col_timestamptz: 2022-07-22T18:15:42.011Z,\n"
            + "  col_date: '2022-07-22',\n"
            + "  col_varchar: 'some random string',\n"
            + "  col_jsonb: { key: 'value' }\n"
            + "}\n",
        output);
  }

  @Test
  public void testCreateAllTypes() throws Exception {
    String output = runTest("createAllTypes");
    assertEquals("Created one record\n", output);
  }

  @Test
  public void testUpdateAllTypes() throws Exception {
    String output = runTest("updateAllTypes");
    assertEquals("Updated one record\n", output);
  }
}
