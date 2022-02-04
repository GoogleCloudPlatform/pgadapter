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
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITJdbcTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static ProxyServer server;
  private static Database database;

  @BeforeClass
  public static void setup() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    testEnv.setUp();
    if (testEnv.isUseExistingDb()) {
      database = testEnv.getExistingDatabase();
    } else {
      database = testEnv.createDatabase();
      testEnv.updateDdl(
          database.getId().getDatabase(),
          Arrays.asList(
              "create table numbers (num int not null primary key, name varchar(100))",
              "create table all_types ("
                  + "col_bigint bigint not null primary key, "
                  + "col_bool bool, "
                  + "col_bytea bytea, "
                  + "col_float8 float8, "
                  + "col_int int, "
                  + "col_numeric numeric, "
                  + "col_timestamptz timestamptz, "
                  + "col_varchar varchar(100))"));
    }
    String credentials = testEnv.getCredentials();
    ImmutableList.Builder<String> argsListBuilder =
        ImmutableList.<String>builder()
            .add(
                "-p",
                testEnv.getProjectId(),
                "-i",
                testEnv.getInstanceId(),
                "-d",
                database.getId().getDatabase(),
                "-s",
                String.valueOf(testEnv.getPort()),
                "-e",
                testEnv.getUrl().getHost());
    if (credentials != null) {
      argsListBuilder.add("-c", testEnv.getCredentials());
    }
    String[] args = argsListBuilder.build().toArray(new String[0]);
    server = new ProxyServer(new OptionsMetadata(args));
    server.startServer();
  }

  @AfterClass
  public static void teardown() {
    if (server != null) {
      server.stopServer();
    }
    testEnv.cleanUp();
  }

  @Before
  public void insertTestData() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(databaseId, Collections.singleton(Mutation.delete("numbers", KeySet.all())));
    testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));
    testEnv.write(
        databaseId,
        Arrays.asList(
            Mutation.newInsertBuilder("numbers").set("num").to(1L).set("name").to("One").build(),
            Mutation.newInsertBuilder("all_types")
                .set("col_bigint")
                .to(1L)
                .set("col_bool")
                .to(true)
                .set("col_bytea")
                .to(ByteArray.copyFrom("test"))
                .set("col_float8")
                .to(3.14d)
                .set("col_int")
                .to(1)
                .set("col_numeric")
                .to(new BigDecimal("3.14"))
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00"))
                .set("col_varchar")
                .to("test")
                .build()));
  }

  @Test
  public void testSelectHelloWorld() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            String.format("jdbc:postgresql://localhost:%d/", testEnv.getPort()))) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("SELECT 'Hello World!'")) {
        assertTrue(resultSet.next());
        assertEquals("Hello World!", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testTwoDmlStatementsInSimpleMode() throws SQLException {
    // This connection prefers the simple query mode. This makes the JDBC driver behave very similar
    // to psql, and enables us to send multiple simple unparameterized statements in one batch.
    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:postgresql://localhost:%d/?preferQueryMode=simple", testEnv.getPort()))) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result is an update count or no result.
        assertFalse(
            statement.execute(
                "INSERT INTO numbers VALUES (2, 'Two'); UPDATE numbers SET name=name || ' - Updated';"));

        // Note that we have sent two DML statements to the database in one string. These should be
        // treated as separate statements, and there should therefore be two results coming back
        // from the server. That is; The first update count should be 1 (the INSERT), and the second
        // should be 2 (the UPDATE).
        assertEquals(1, statement.getUpdateCount());

        // The following is a prime example of how not to design an API, but this is how JDBC works.
        // getMoreResults() returns true if the next result is a ResultSet. However, if the next
        // result is an update count, it returns false, and we have to check getUpdateCount() to
        // verify whether there were any more results.
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());

        // There are no more results. This is indicated by getMoreResults returning false AND
        // getUpdateCount returning -1.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        // Read back the data to verify.
        try (ResultSet resultSet =
            statement.executeQuery("SELECT name FROM numbers ORDER BY num")) {
          assertTrue(resultSet.next());
          assertEquals("One - Updated", resultSet.getString("name"));
          assertTrue(resultSet.next());
          assertEquals("Two - Updated", resultSet.getString("name"));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testDmlAndQueryInBatchInSimpleMode() throws SQLException {
    // This connection prefers the simple query mode. This makes the JDBC driver behave very similar
    // to psql, and enables us to send multiple simple unparameterized statements in one batch.
    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:postgresql://localhost:%d/?preferQueryMode=simple", testEnv.getPort()))) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertFalse(
            statement.execute(
                "INSERT INTO numbers VALUES (2, 'Two'); SELECT name FROM numbers ORDER BY num;"));
        assertEquals(1, statement.getUpdateCount());

        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals("One", resultSet.getString("name"));
          assertTrue(resultSet.next());
          assertEquals("Two", resultSet.getString("name"));
          assertFalse(resultSet.next());
        }

        // There are no more results. This is indicated by getMoreResults returning false AND
        // getUpdateCount returning -1.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }
  }
}
