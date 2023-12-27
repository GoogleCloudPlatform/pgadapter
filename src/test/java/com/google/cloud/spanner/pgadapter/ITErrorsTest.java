// Copyright 2023 Google LLC
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.util.PSQLException;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITErrorsTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  @BeforeClass
  public static void setup() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    testEnv.setUp();
    database =
        testEnv.createDatabase(
            ImmutableList.of(
                "create table my_table (id bigint primary key, value varchar)",
                "create unique index my_index on my_table (value)",
                "create table my_other_table (id bigint primary key, my_table_id bigint, constraint fk_my_table foreign key (my_table_id) references my_table (id))"));
    testEnv.startPGAdapterServer(Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @After
  public void clearTestData() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      connection.createStatement().execute("delete from my_other_table");
      connection.createStatement().execute("delete from my_table");
    }
  }

  private String getConnectionUrl() {
    return String.format(
        "jdbc:postgresql://%s/%s",
        testEnv.getPGAdapterHostAndPort(), database.getId().getDatabase());
  }

  @Test
  public void testTableNotFound() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      PSQLException exception =
          assertThrows(
              PSQLException.class,
              () ->
                  connection
                      .createStatement()
                      .executeUpdate("insert into my_non_existing_table (id, ref) values (1, 1)"));
      assertNotNull(exception.getServerErrorMessage());
      assertEquals(
          SQLState.UndefinedTable.toString(), exception.getServerErrorMessage().getSQLState());
      assertEquals(SQLState.UndefinedTable.toString(), exception.getSQLState());
    }
  }

  @Test
  public void testColumnNotFound() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      PSQLException exception =
          assertThrows(
              PSQLException.class,
              () ->
                  connection
                      .createStatement()
                      .executeUpdate("insert into my_other_table (id, ref) values (1, 1)"));
      assertNotNull(exception.getServerErrorMessage());
      assertEquals(
          SQLState.UndefinedColumn.toString(), exception.getServerErrorMessage().getSQLState());
      assertEquals(SQLState.UndefinedColumn.toString(), exception.getSQLState());
    }
  }

  @Test
  public void testPrimaryKeyConstraintViolation() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      assertEquals(
          1,
          connection
              .createStatement()
              .executeUpdate("insert into my_table (id, value) values (1, 'One')"));
      PSQLException exception =
          assertThrows(
              PSQLException.class,
              () ->
                  connection
                      .createStatement()
                      .executeUpdate("insert into my_table (id, value) values (1, 'Other one')"));
      assertNotNull(exception.getServerErrorMessage());
      assertEquals(
          SQLState.UniqueViolation.toString(), exception.getServerErrorMessage().getSQLState());
      assertEquals(SQLState.UniqueViolation.toString(), exception.getSQLState());
    }
  }

  @Test
  public void testUniqueIndexViolation() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      assertEquals(
          1,
          connection
              .createStatement()
              .executeUpdate("insert into my_table (id, value) values (1, 'One')"));
      PSQLException exception =
          assertThrows(
              PSQLException.class,
              () ->
                  connection
                      .createStatement()
                      .executeUpdate("insert into my_table (id, value) values (2, 'One')"));
      assertNotNull(exception.getServerErrorMessage());
      assertEquals(
          SQLState.UniqueViolation.toString(), exception.getServerErrorMessage().getSQLState());
      assertEquals(SQLState.UniqueViolation.toString(), exception.getSQLState());
    }
  }

  @Test
  public void testForeignKeyConstraintViolation() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      PSQLException exception =
          assertThrows(
              PSQLException.class,
              () ->
                  connection
                      .createStatement()
                      .executeUpdate("insert into my_other_table (id, my_table_id) values (1, 1)"));
      assertNotNull(exception.getServerErrorMessage());
      assertEquals(
          SQLState.ForeignKeyViolation.toString(), exception.getServerErrorMessage().getSQLState());
      assertEquals(SQLState.ForeignKeyViolation.toString(), exception.getSQLState());
    }
  }
}
