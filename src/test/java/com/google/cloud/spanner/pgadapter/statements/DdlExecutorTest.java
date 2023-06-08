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

package com.google.cloud.spanner.pgadapter.statements;

import static com.google.cloud.spanner.pgadapter.statements.DdlExecutor.unquoteIdentifier;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DdlExecutorTest {
  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  @Test
  public void testUnquoteIdentifier() {
    assertEquals("foo", unquoteIdentifier("foo"));
    assertEquals("foo", unquoteIdentifier("FOO"));
    assertEquals("foo", unquoteIdentifier("Foo"));

    assertEquals("foo", unquoteIdentifier("\"foo\""));
    assertEquals("FOO", unquoteIdentifier("\"FOO\""));
    assertEquals("Foo", unquoteIdentifier("\"Foo\""));

    assertEquals("", unquoteIdentifier(""));
    assertEquals("a", unquoteIdentifier("a"));
    assertEquals("", unquoteIdentifier("\"\""));
  }

  private Statement translate(String sql, DdlExecutor executor) {
    Statement statement = Statement.of(sql);
    return executor.translate(PARSER.parse(statement), statement);
  }

  @Test
  public void testMaybeRemovePrimaryKeyConstraintName() {
    DdlExecutor ddlExecutor = new DdlExecutor(mock(BackendConnection.class));

    assertEquals(
        Statement.of("create table foo (id bigint primary key, value text)"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of("create table foo (id bigint primary key, value text)")));
    assertEquals(
        Statement.of("create table foo (id bigint, value text, primary key (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of("create table foo (id bigint, value text, primary key (id))")));
    assertEquals(
        Statement.of(
            "create table foo (value1 varchar, value2 text, primary key (value1, value2))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table foo (value1 varchar, value2 text, primary key (value1, value2))")));
    assertEquals(
        Statement.of(
            "create table foo (id bigint primary key, value text, constraint chk_bar check (length(value) < 100))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table foo (id bigint primary key, value text, constraint chk_bar check (length(value) < 100))")));

    // Only primary key constraints that are literally named 'pk_<table-name>' are replaced.
    assertEquals(
        Statement.of(
            "create table foo (id bigint, value text, constraint pk_a1b2 primary key (id) )"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table foo (id bigint, value text, constraint pk_a1b2 primary key (id) )")));

    assertEquals(
        Statement.of("create table foo (id bigint, value text,  primary key (id) )"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table foo (id bigint, value text, constraint pk_foo primary key (id) )")));
    assertEquals(
        Statement.of(
            "create table foo (id bigint, value text,  primary key (id), constraint fk_bar foreign key (value) references bar (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table foo (id bigint, value text, constraint pk_foo primary key (id), constraint fk_bar foreign key (value) references bar (id))")));
    assertEquals(
        Statement.of("create table public.foo (id bigint, value text,  primary key (id) )"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table public.foo (id bigint, value text, constraint pk_foo primary key (id) )")));

    assertEquals(
        Statement.of(
            "CREATE TABLE \"user\" (\"id\" integer NOT NULL, \"firstName\" character varying NOT NULL, \"lastName\" character varying NOT NULL, \"age\" integer NOT NULL,  PRIMARY KEY (\"id\"))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "CREATE TABLE \"user\" (\"id\" integer NOT NULL, \"firstName\" character varying NOT NULL, \"lastName\" character varying NOT NULL, \"age\" integer NOT NULL, CONSTRAINT \"PK_user\" PRIMARY KEY (\"id\"))")));
  }

  @Test
  public void testGetDependentStatements() {
    BackendConnection connection = mock(BackendConnection.class);
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.isSupportDropCascade()).thenReturn(true);
    when(connection.getSessionState()).thenReturn(sessionState);
    DdlExecutor ddlExecutor =
        new DdlExecutor(connection) {
          @Override
          ImmutableList<Statement> getDropDependentIndexesStatements(TableOrIndexName tableName) {
            return ImmutableList.of(Statement.of("drop dependent indexes of " + tableName));
          }

          @Override
          ImmutableList<Statement> getDropDependentForeignKeyConstraintsStatements(
              TableOrIndexName tableName) {
            return ImmutableList.of(Statement.of("drop dependent foreign keys of " + tableName));
          }

          @Override
          ImmutableList<Statement> getDropSchemaTablesStatements(TableOrIndexName schemaName) {
            return ImmutableList.of(Statement.of("drop all tables in schema " + schemaName));
          }

          @Override
          ImmutableList<Statement> getDropSchemaForeignKeysStatements(TableOrIndexName schemaName) {
            return ImmutableList.of(Statement.of("drop all foreign keys in schema " + schemaName));
          }

          @Override
          ImmutableList<Statement> getDropSchemaIndexesStatements(TableOrIndexName schemaName) {
            return ImmutableList.of(Statement.of("drop all indexes in schema " + schemaName));
          }

          @Override
          ImmutableList<Statement> getDropSchemaViewsStatements(TableOrIndexName schemaName) {
            return ImmutableList.of(Statement.of("drop all views in schema " + schemaName));
          }
        };

    assertGetDependentStatementsReturnsSame(
        ddlExecutor, Statement.of("create table foo (id bigint primary key)"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop view foo"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop table"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop table foo invalid"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop schema"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop schema foo invalid"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop schema foo"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop schema foo restrict"));

    assertEquals(
        ImmutableList.of(
            Statement.of("drop dependent indexes of foo"), Statement.of("drop table foo")),
        ddlExecutor.getDependentStatements(Statement.of("drop table foo")));
    assertEquals(
        ImmutableList.of(
            Statement.of("drop dependent indexes of foo"),
            Statement.of("drop dependent foreign keys of foo"),
            Statement.of("drop table foo")),
        ddlExecutor.getDependentStatements(Statement.of("drop table foo cascade")));
    assertEquals(
        ImmutableList.of(
            Statement.of("drop all indexes in schema foo"),
            Statement.of("drop all foreign keys in schema foo"),
            Statement.of("drop all views in schema foo"),
            Statement.of("drop all tables in schema foo")),
        ddlExecutor.getDependentStatements(Statement.of("drop schema foo cascade")));
  }

  private void assertGetDependentStatementsReturnsSame(
      DdlExecutor ddlExecutor, Statement statement) {
    ImmutableList<Statement> dependentStatements = ddlExecutor.getDependentStatements(statement);
    assertEquals(1, dependentStatements.size());
    assertSame(dependentStatements.get(0), statement);
  }

  @Test
  public void testGetDependentStatements_DropTable() {
    BackendConnection connection = mock(BackendConnection.class);
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.isSupportDropCascade()).thenReturn(true);
    when(connection.getSessionState()).thenReturn(sessionState);
    DdlExecutor ddlExecutor = new DdlExecutor(connection);

    assertGetDependentStatementsReturnsSame(
        ddlExecutor, Statement.of("create table foo (id bigint primary key)"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop view foo"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop table"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop table foo invalid"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop schema"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop schema foo invalid"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop schema foo"));
    assertGetDependentStatementsReturnsSame(ddlExecutor, Statement.of("drop schema foo restrict"));
  }
}
