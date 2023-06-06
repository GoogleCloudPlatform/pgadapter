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
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.TypeCode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DropCascadeMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  protected String createUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/?options=-c%%20spanner.support_drop_cascade=true",
        pgServer.getLocalPort());
  }

  @Test
  public void testDropTableStatementWithoutIndexesAndForeignKeys() throws SQLException {
    String sql = "DROP TABLE foo";
    addDdlResponseToSpannerAdmin();

    addTableIndexesResult(new TableName("public", "foo"), ImmutableList.of());
    addTableForeignKeysResult(new TableName("public", "foo"), ImmutableList.of());

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.support_drop_cascade")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
        assertFalse(resultSet.next());
      }
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(sql, updateDatabaseDdlRequests.get(0).getStatements(0));
  }

  @Test
  public void testDropTableStatementWithIndex() throws SQLException {
    String sql = "DROP TABLE foo";
    addDdlResponseToSpannerAdmin();

    addTableIndexesResult(
        new TableName("public", "foo"), ImmutableList.of(new IndexName("public", "idx_foo")));
    addTableForeignKeysResult(new TableName("public", "foo"), ImmutableList.of());

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(2, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "drop index \"public\".\"idx_foo\"", updateDatabaseDdlRequests.get(0).getStatements(0));
    assertEquals(sql, updateDatabaseDdlRequests.get(0).getStatements(1));
  }

  @Test
  public void testDropTableCascadeStatementWithForeignKey() throws SQLException {
    String sql = "DROP TABLE foo cascade";
    addDdlResponseToSpannerAdmin();

    addTableIndexesResult(new TableName("public", "foo"), ImmutableList.of());
    addTableForeignKeysResult(
        new TableName("public", "foo"),
        ImmutableList.of(new ForeignKey(new TableName("public", "bar"), "fk_foo_bar")));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(2, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "alter table \"public\".\"bar\" drop constraint \"fk_foo_bar\"",
        updateDatabaseDdlRequests.get(0).getStatements(0));
    assertEquals(sql, updateDatabaseDdlRequests.get(0).getStatements(1));
  }

  @Test
  public void testDropTableCascadeStatementWithIndexAndForeignKey() throws SQLException {
    String sql = "DROP TABLE foo cascade";
    addDdlResponseToSpannerAdmin();

    addTableIndexesResult(
        new TableName("public", "foo"), ImmutableList.of(new IndexName("public", "idx_foo")));
    addTableForeignKeysResult(
        new TableName("public", "foo"),
        ImmutableList.of(new ForeignKey(new TableName("public", "bar"), "fk_foo_bar")));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(3, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "drop index \"public\".\"idx_foo\"", updateDatabaseDdlRequests.get(0).getStatements(0));
    assertEquals(
        "alter table \"public\".\"bar\" drop constraint \"fk_foo_bar\"",
        updateDatabaseDdlRequests.get(0).getStatements(1));
    assertEquals(sql, updateDatabaseDdlRequests.get(0).getStatements(2));
  }

  @Test
  public void testDropTableStatementWithIndexAndForeignKey() throws SQLException {
    // DROP TABLE <table> [RESTRICT] removes all indexes but not foreign key constraints.
    for (String sql : new String[] {"DROP TABLE foo", "DROP TABLE foo RESTRICT"}) {
      addDdlResponseToSpannerAdmin();

      addTableIndexesResult(
          new TableName("public", "foo"), ImmutableList.of(new IndexName("public", "idx_foo")));
      addTableForeignKeysResult(
          new TableName("public", "foo"),
          ImmutableList.of(new ForeignKey(new TableName("public", "bar"), "fk_foo_bar")));

      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (Statement statement = connection.createStatement()) {
          assertFalse(statement.execute(sql));
          assertEquals(0, statement.getUpdateCount());
          assertFalse(statement.getMoreResults());
          assertEquals(-1, statement.getUpdateCount());
        }
      }

      List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
          mockDatabaseAdmin.getRequests().stream()
              .filter(request -> request instanceof UpdateDatabaseDdlRequest)
              .map(UpdateDatabaseDdlRequest.class::cast)
              .collect(Collectors.toList());
      assertEquals(1, updateDatabaseDdlRequests.size());
      assertEquals(2, updateDatabaseDdlRequests.get(0).getStatementsCount());
      assertEquals(
          "drop index \"public\".\"idx_foo\"", updateDatabaseDdlRequests.get(0).getStatements(0));
      assertEquals(sql, updateDatabaseDdlRequests.get(0).getStatements(1));
      mockDatabaseAdmin.getRequests().clear();
    }
  }

  @Test
  public void testDropTableCascadeStatementWithSchemaAndIndexAndForeignKey() throws SQLException {
    String sql = "DROP TABLE my_schema.foo cascade";
    addDdlResponseToSpannerAdmin();

    addTableIndexesResult(
        new TableName("my_schema", "foo"), ImmutableList.of(new IndexName("my_schema", "idx_foo")));
    addTableForeignKeysResult(
        new TableName("my_schema", "foo"),
        ImmutableList.of(new ForeignKey(new TableName("my_schema", "bar"), "fk_foo_bar")));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(3, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "drop index \"my_schema\".\"idx_foo\"", updateDatabaseDdlRequests.get(0).getStatements(0));
    assertEquals(
        "alter table \"my_schema\".\"bar\" drop constraint \"fk_foo_bar\"",
        updateDatabaseDdlRequests.get(0).getStatements(1));
    assertEquals(sql, updateDatabaseDdlRequests.get(0).getStatements(2));
  }

  @Test
  public void testDropTableCascadeStatementWithIndexAndForeignKeyInBatch() throws SQLException {
    String sql = "DROP TABLE foo cascade";
    addDdlResponseToSpannerAdmin();

    addTableIndexesResult(new TableName("public", "baz"), ImmutableList.of());
    addTableForeignKeysResult(new TableName("public", "baz"), ImmutableList.of());
    addTableIndexesResult(
        new TableName("public", "foo"), ImmutableList.of(new IndexName("public", "idx_foo")));
    addTableForeignKeysResult(
        new TableName("public", "foo"),
        ImmutableList.of(new ForeignKey(new TableName("public", "bar"), "fk_foo_bar")));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        statement.addBatch("drop table baz");
        statement.addBatch(sql);
        int[] result = statement.executeBatch();
        assertArrayEquals(new int[] {0, 0}, result);
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(4, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals("drop table baz", updateDatabaseDdlRequests.get(0).getStatements(0));
    assertEquals(
        "drop index \"public\".\"idx_foo\"", updateDatabaseDdlRequests.get(0).getStatements(1));
    assertEquals(
        "alter table \"public\".\"bar\" drop constraint \"fk_foo_bar\"",
        updateDatabaseDdlRequests.get(0).getStatements(2));
    assertEquals(sql, updateDatabaseDdlRequests.get(0).getStatements(3));
  }

  @Test
  public void testDropSchemaCascade() throws SQLException {
    String sql = "DROP SCHEMA public cascade ";
    addDdlResponseToSpannerAdmin();

    addSchemaIndexesResult(
        "public",
        ImmutableList.of(
            new IndexName("public", "idx_albums_title"),
            new IndexName("public", "idx_concerts_start_time"),
            new IndexName("public", "idx_singers_last_name")));
    addSchemaForeignKeysResult(
        "public",
        ImmutableList.of(
            new ForeignKey(new TableName("public", "albums"), "fk_albums_singers"),
            new ForeignKey(new TableName("public", "concerts"), "fk_concerts_singers"),
            new ForeignKey(new TableName("public", "concerts"), "fk_concerts_venues")));

    Random random = new Random();
    //noinspection ComparatorMethodParameterNotUsed
    addSchemaTablesResult(
        "public",
        ImmutableList.copyOf(
            ImmutableList.of(
                    new Table(new TableName("public", "albums"), "singers"),
                    new Table(new TableName("public", "concerts"), null),
                    new Table(new TableName("public", "singers"), null),
                    new Table(new TableName("public", "tracks"), "albums"),
                    new Table(new TableName("public", "venues"), null))
                .stream()
                .sorted((t1, t2) -> (random.nextInt(3) - 1))
                .collect(Collectors.toList())));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(11, updateDatabaseDdlRequests.get(0).getStatementsCount());
    int index = -1;
    assertEquals(
        "drop index \"public\".\"idx_albums_title\"",
        updateDatabaseDdlRequests.get(0).getStatements(++index));
    assertEquals(
        "drop index \"public\".\"idx_concerts_start_time\"",
        updateDatabaseDdlRequests.get(0).getStatements(++index));
    assertEquals(
        "drop index \"public\".\"idx_singers_last_name\"",
        updateDatabaseDdlRequests.get(0).getStatements(++index));
    assertEquals(
        "alter table \"public\".\"albums\" drop constraint \"fk_albums_singers\"",
        updateDatabaseDdlRequests.get(0).getStatements(++index));
    assertEquals(
        "alter table \"public\".\"concerts\" drop constraint \"fk_concerts_singers\"",
        updateDatabaseDdlRequests.get(0).getStatements(++index));
    assertEquals(
        "alter table \"public\".\"concerts\" drop constraint \"fk_concerts_venues\"",
        updateDatabaseDdlRequests.get(0).getStatements(++index));
    assertEquals(
        "drop table \"public\".\"tracks\"",
        updateDatabaseDdlRequests.get(0).getStatements(++index));
    assertEquals(
        "drop table \"public\".\"albums\"",
        updateDatabaseDdlRequests.get(0).getStatements(++index));
    assertEquals(
        "drop table \"public\".\"concerts\"",
        updateDatabaseDdlRequests.get(0).getStatements(++index));
    assertEquals(
        "drop table \"public\".\"singers\"",
        updateDatabaseDdlRequests.get(0).getStatements(++index));
    assertEquals(
        "drop table \"public\".\"venues\"",
        updateDatabaseDdlRequests.get(0).getStatements(++index));
  }

  @Test
  public void testDropSchema() throws SQLException {
    for (String sql : new String[] {"DROP SCHEMA public", "drop schema public restrict"}) {
      addDdlResponseToSpannerAdmin();

      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (Statement statement = connection.createStatement()) {
          assertFalse(statement.execute(sql));
          assertEquals(0, statement.getUpdateCount());
          assertFalse(statement.getMoreResults());
          assertEquals(-1, statement.getUpdateCount());
        }
      }

      List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
          mockDatabaseAdmin.getRequests().stream()
              .filter(request -> request instanceof UpdateDatabaseDdlRequest)
              .map(UpdateDatabaseDdlRequest.class::cast)
              .collect(Collectors.toList());
      assertEquals(1, updateDatabaseDdlRequests.size());
      assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
      // DROP SCHEMA without a cascade should be a pass-through.
      assertEquals(sql, updateDatabaseDdlRequests.get(0).getStatements(0));
      mockDatabaseAdmin.getRequests().clear();
    }
  }

  @Test
  public void testDropSchemaRestrict() throws SQLException {
    String sql = "DROP SCHEMA public restrict";
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    // DROP SCHEMA ... RESTRICT should be a pass-through.
    assertEquals(sql, updateDatabaseDdlRequests.get(0).getStatements(0));
  }

  private static final class Table {
    final TableName tableName;
    final String parent_table_name;

    Table(TableName tableName, String parent_table_name) {
      this.tableName = tableName;
      this.parent_table_name = parent_table_name;
    }
  }

  private static class TableName {
    final String table_schema;
    final String table_name;

    TableName(String table_schema, String table_name) {
      this.table_schema = table_schema;
      this.table_name = table_name;
    }
  }

  private static class IndexName {
    final String table_schema;
    final String index_name;

    IndexName(String table_schema, String index_name) {
      this.table_schema = table_schema;
      this.index_name = index_name;
    }
  }

  private static class ForeignKey {
    final TableName tableName;
    final String constraint_name;

    ForeignKey(TableName tableName, String constraint_name) {
      this.tableName = tableName;
      this.constraint_name = constraint_name;
    }
  }

  private static void addSchemaTablesResult(String schemaName, ImmutableList<Table> tables) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.newBuilder(
                    "select table_schema, table_name, parent_table_name "
                        + "from information_schema.tables "
                        + "where table_schema=$1 "
                        + "and table_type='BASE TABLE'")
                .bind("p1")
                .to(schemaName)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING),
                        ImmutableList.of("table_schema", "table_name", "parent_table_name")))
                .addAllRows(
                    tables.stream()
                        .map(
                            table ->
                                ListValue.newBuilder()
                                    .addValues(
                                        Value.newBuilder()
                                            .setStringValue(table.tableName.table_schema)
                                            .build())
                                    .addValues(
                                        Value.newBuilder()
                                            .setStringValue(table.tableName.table_name)
                                            .build())
                                    .addValues(
                                        table.parent_table_name == null
                                            ? Value.newBuilder()
                                                .setNullValue(NullValue.NULL_VALUE)
                                                .build()
                                            : Value.newBuilder()
                                                .setStringValue(table.parent_table_name)
                                                .build())
                                    .build())
                        .collect(Collectors.toList()))
                .build()));
  }

  private static void addTableIndexesResult(TableName tableName, ImmutableList<IndexName> indexes) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.newBuilder(
                    "select table_schema, index_name from information_schema.indexes "
                        + "where table_schema=$1 "
                        + "and table_name=$2 "
                        + "and not index_type = 'PRIMARY_KEY' "
                        + "and spanner_is_managed = 'NO'")
                .bind("p1")
                .to(tableName.table_schema)
                .bind("p2")
                .to(tableName.table_name)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.STRING, TypeCode.STRING),
                        ImmutableList.of("table_schema", "index_name")))
                .addAllRows(
                    indexes.stream()
                        .map(
                            index ->
                                ListValue.newBuilder()
                                    .addValues(
                                        Value.newBuilder()
                                            .setStringValue(index.table_schema)
                                            .build())
                                    .addValues(
                                        Value.newBuilder().setStringValue(index.index_name).build())
                                    .build())
                        .collect(Collectors.toList()))
                .build()));
  }

  private static void addSchemaIndexesResult(String schemaName, ImmutableList<IndexName> indexes) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.newBuilder(
                    "select table_schema, index_name from information_schema.indexes "
                        + "where table_schema=$1 "
                        + "and not index_type = 'PRIMARY_KEY' "
                        + "and spanner_is_managed = 'NO'")
                .bind("p1")
                .to(schemaName)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.STRING, TypeCode.STRING),
                        ImmutableList.of("table_schema", "index_name")))
                .addAllRows(
                    indexes.stream()
                        .map(
                            index ->
                                ListValue.newBuilder()
                                    .addValues(
                                        Value.newBuilder()
                                            .setStringValue(index.table_schema)
                                            .build())
                                    .addValues(
                                        Value.newBuilder().setStringValue(index.index_name).build())
                                    .build())
                        .collect(Collectors.toList()))
                .build()));
  }

  private static void addTableForeignKeysResult(
      TableName tableName, ImmutableList<ForeignKey> foreignKeys) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.newBuilder(
                    "select child.table_schema, child.table_name, rc.constraint_name\n"
                        + "from information_schema.referential_constraints rc\n"
                        + "inner join information_schema.table_constraints parent\n"
                        + "  on  rc.unique_constraint_catalog=parent.constraint_catalog\n"
                        + "  and rc.unique_constraint_schema=parent.constraint_schema\n"
                        + "  and rc.unique_constraint_name=parent.constraint_name\n"
                        + "inner join information_schema.table_constraints child\n"
                        + "  on  rc.constraint_catalog=child.constraint_catalog\n"
                        + "  and rc.constraint_schema=child.constraint_schema\n"
                        + "  and rc.constraint_name=child.constraint_name\n"
                        + "where parent.table_schema=$1\n"
                        + "and parent.table_name=$2")
                .bind("p1")
                .to(tableName.table_schema)
                .bind("p2")
                .to(tableName.table_name)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING),
                        ImmutableList.of("table_schema", "table_name", "constraint_name")))
                .addAllRows(
                    foreignKeys.stream()
                        .map(
                            foreignKey ->
                                ListValue.newBuilder()
                                    .addValues(
                                        Value.newBuilder()
                                            .setStringValue(foreignKey.tableName.table_schema)
                                            .build())
                                    .addValues(
                                        Value.newBuilder()
                                            .setStringValue(foreignKey.tableName.table_name)
                                            .build())
                                    .addValues(
                                        Value.newBuilder()
                                            .setStringValue(foreignKey.constraint_name)
                                            .build())
                                    .build())
                        .collect(Collectors.toList()))
                .build()));
  }

  private static void addSchemaForeignKeysResult(
      String schemaName, ImmutableList<ForeignKey> foreignKeys) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.newBuilder(
                    "select child.table_schema, child.table_name, rc.constraint_name\n"
                        + "from information_schema.referential_constraints rc\n"
                        + "inner join information_schema.table_constraints parent\n"
                        + "  on  rc.unique_constraint_catalog=parent.constraint_catalog\n"
                        + "  and rc.unique_constraint_schema=parent.constraint_schema\n"
                        + "  and rc.unique_constraint_name=parent.constraint_name\n"
                        + "inner join information_schema.table_constraints child\n"
                        + "  on  rc.constraint_catalog=child.constraint_catalog\n"
                        + "  and rc.constraint_schema=child.constraint_schema\n"
                        + "  and rc.constraint_name=child.constraint_name\n"
                        + "where parent.table_schema=$1")
                .bind("p1")
                .to(schemaName)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING),
                        ImmutableList.of("table_schema", "table_name", "constraint_name")))
                .addAllRows(
                    foreignKeys.stream()
                        .map(
                            foreignKey ->
                                ListValue.newBuilder()
                                    .addValues(
                                        Value.newBuilder()
                                            .setStringValue(foreignKey.tableName.table_schema)
                                            .build())
                                    .addValues(
                                        Value.newBuilder()
                                            .setStringValue(foreignKey.tableName.table_name)
                                            .build())
                                    .addValues(
                                        Value.newBuilder()
                                            .setStringValue(foreignKey.constraint_name)
                                            .build())
                                    .build())
                        .collect(Collectors.toList()))
                .build()));
  }
}
