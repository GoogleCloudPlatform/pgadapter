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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.util.PSQLException;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITDdlTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();

  @BeforeClass
  public static void setup() {
    testEnv.setUp();
    Database database = testEnv.createDatabase(ImmutableList.of());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), ImmutableList.of());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  private String getConnectionUrl() {
    return String.format("jdbc:postgresql://%s/", testEnv.getPGAdapterHostAndPort());
  }

  @Test
  public void testDropTableWithIndex() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      // First create a table with a secondary index.
      try (Statement statement = connection.createStatement()) {
        statement.addBatch("create table test (id bigint primary key, value varchar)");
        statement.addBatch("create index idx_text on test (value)");
        assertArrayEquals(new int[] {0, 0}, statement.executeBatch());
      }

      // Try to drop the table with an index. This will fail.
      PSQLException exception =
          assertThrows(
              PSQLException.class, () -> connection.createStatement().execute("drop table test"));
      assertEquals(SQLState.FeatureNotSupported.toString(), exception.getSQLState());
      assertNotNull(exception.getServerErrorMessage());
      assertEquals(
          "Execute 'set spanner.support_drop_cascade=true' to enable dropping tables with indices",
          exception.getServerErrorMessage().getHint());

      // Now enable drop_cascade.
      connection.createStatement().execute("set spanner.support_drop_cascade to on");
      // Dropping the table should now work.
      assertEquals(0, connection.createStatement().executeUpdate("drop table test"));
    }
  }

  @Test
  public void testDropTableWithForeignKey() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      // First create a table with a secondary index.
      try (Statement statement = connection.createStatement()) {
        statement.addBatch("create table parent (id bigint primary key, value varchar)");
        statement.addBatch(
            "create table child (id bigint primary key, parent_id bigint, constraint fk_child_parent foreign key (parent_id) references parent (id))");
        assertArrayEquals(new int[] {0, 0}, statement.executeBatch());
      }

      // Try to drop the table that is referenced by a foreign key. This will fail.
      PSQLException exception =
          assertThrows(
              PSQLException.class, () -> connection.createStatement().execute("drop table parent"));
      String expectedMessage =
          "Cannot drop table `parent`. It is referenced by one or more foreign keys: `fk_child_parent`. You must drop the foreign keys before dropping the table.";
      assertNotNull(exception.getServerErrorMessage());
      assertNotNull(exception.getServerErrorMessage().getMessage());
      assertTrue(
          exception.getServerErrorMessage().getMessage(),
          exception.getServerErrorMessage().getMessage().endsWith(expectedMessage));

      exception =
          assertThrows(
              PSQLException.class,
              () -> connection.createStatement().execute("drop table parent cascade"));
      System.out.println(exception.getErrorCode());
      System.out.println(exception.getMessage());
      assertEquals(SQLState.FeatureNotSupported.toString(), exception.getSQLState());
      assertNotNull(exception.getServerErrorMessage());
      assertEquals(
          "Execute 'set spanner.support_drop_cascade=true' to enable 'drop {table|schema} cascade' statements.",
          exception.getServerErrorMessage().getHint());

      // Now enable drop_cascade.
      connection.createStatement().execute("set spanner.support_drop_cascade to on");
      // Dropping the table should now work.
      assertEquals(0, connection.createStatement().executeUpdate("drop table parent cascade"));
      // Drop the child table as well to clean up.
      assertEquals(0, connection.createStatement().executeUpdate("drop table child cascade"));
    }
  }

  @Test
  public void testDropSchema() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      // Create a set of tables, views, indexes etc.
      try (Statement statement = connection.createStatement()) {
        statement.addBatch(
            "create table if not exists singers (\n"
                + "    id         varchar not null primary key,\n"
                + "    first_name varchar,\n"
                + "    last_name  varchar not null,\n"
                + "    full_name  varchar generated always as (coalesce(concat(first_name, ' '::varchar, last_name), last_name)) stored,\n"
                + "    active     boolean,\n"
                + "    created_at timestamptz,\n"
                + "    updated_at timestamptz\n"
                + ");");
        statement.addBatch(
            "create table if not exists albums (\n"
                + "    id               varchar not null primary key,\n"
                + "    title            varchar not null,\n"
                + "    marketing_budget numeric,\n"
                + "    release_date     date,\n"
                + "    cover_picture    bytea,\n"
                + "    singer_id        varchar not null,\n"
                + "    created_at       timestamptz,\n"
                + "    updated_at       timestamptz,\n"
                + "    constraint fk_albums_singers foreign key (singer_id) references singers (id)\n"
                + ");");
        statement.addBatch(
            "create table if not exists tracks (\n"
                + "    id           varchar not null,\n"
                + "    track_number bigint not null,\n"
                + "    title        varchar not null,\n"
                + "    sample_rate  float8 not null,\n"
                + "    created_at   timestamptz,\n"
                + "    updated_at   timestamptz,\n"
                + "    primary key (id, track_number)\n"
                + ") interleave in parent albums on delete cascade;");
        statement.addBatch(
            "create table if not exists venues (\n"
                + "    id          varchar not null primary key,\n"
                + "    name        varchar not null,\n"
                + "    description varchar not null,\n"
                + "    created_at  timestamptz,\n"
                + "    updated_at  timestamptz\n"
                + ");");
        statement.addBatch(
            "create table if not exists concerts (\n"
                + "    id          varchar not null primary key,\n"
                + "    venue_id    varchar not null,\n"
                + "    singer_id   varchar not null,\n"
                + "    name        varchar not null,\n"
                + "    start_time  timestamptz not null,\n"
                + "    end_time    timestamptz not null,\n"
                + "    created_at  timestamptz,\n"
                + "    updated_at  timestamptz,\n"
                + "    constraint fk_concerts_venues  foreign key (venue_id)  references venues  (id),\n"
                + "    constraint fk_concerts_singers foreign key (singer_id) references singers (id),\n"
                + "    constraint chk_end_time_after_start_time check (end_time > start_time)\n"
                + ");");
        statement.addBatch("create index idx_concerts_start_time on concerts (start_time)");
        statement.addBatch("create unique index idx_tracks_title on tracks (id, title)");
        statement.addBatch(
            "create view v_singers sql security invoker as select first_name, last_name from singers order by last_name");
        statement.addBatch(
            "create view v_venues sql security invoker as select name, description from venues order by name");

        assertArrayEquals(new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0}, statement.executeBatch());
      }

      // Try to drop the schema.
      PSQLException exception =
          assertThrows(
              PSQLException.class,
              () -> connection.createStatement().execute("drop schema public cascade"));
      System.out.println(exception.getMessage());
      assertEquals(SQLState.FeatureNotSupported.toString(), exception.getSQLState());
      assertNotNull(exception.getServerErrorMessage());
      assertEquals(
          "Execute 'set spanner.support_drop_cascade=true' to enable 'drop {table|schema} cascade' statements.",
          exception.getServerErrorMessage().getHint());

      // Now enable drop_cascade.
      connection.createStatement().execute("set spanner.support_drop_cascade to on");
      // Dropping the schema should now work. This will not actually drop the schema, but it will
      // drop all objects in the schema.
      assertEquals(0, connection.createStatement().executeUpdate("drop schema public cascade"));

      try (ResultSet tablesCount =
          connection
              .createStatement()
              .executeQuery(
                  "select count(1) from information_schema.tables where table_schema='public'")) {
        assertTrue(tablesCount.next());
        assertEquals(0L, tablesCount.getLong(1));
        assertFalse(tablesCount.next());
      }
    }
  }
}
