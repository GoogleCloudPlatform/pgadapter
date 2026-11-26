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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.Tuple;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.DdlExecutor.Table;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.utils.RegexQueryPartReplacer;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.util.regex.Pattern;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DdlExecutorTest {
  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  @Test
  public void testUnquoteIdentifier() {
    assertNull(unquoteIdentifier(null));
    assertEquals("foo", unquoteIdentifier("foo"));
    assertEquals("foo", unquoteIdentifier("FOO"));
    assertEquals("foo", unquoteIdentifier("Foo"));

    assertEquals("foo", unquoteIdentifier("\"foo\""));
    assertEquals("FOO", unquoteIdentifier("\"FOO\""));
    assertEquals("Foo", unquoteIdentifier("\"Foo\""));

    assertEquals("", unquoteIdentifier(""));
    assertEquals("a", unquoteIdentifier("a"));
    assertEquals("a", unquoteIdentifier("A"));
    assertEquals("\"", unquoteIdentifier("\""));
    assertEquals("\"foo", unquoteIdentifier("\"foo"));
    assertEquals("foo\"", unquoteIdentifier("foo\""));
    assertEquals("", unquoteIdentifier("\"\""));
  }

  private Statement translate(String sql, DdlExecutor executor) {
    Statement statement = Statement.of(sql);
    return executor.translate(PARSER.parse(statement), statement);
  }

  @Test
  public void testMaybeRemovePrimaryKeyConstraintName() {
    DdlExecutor ddlExecutor =
        new DdlExecutor(mock(BackendConnection.class), Suppliers.ofInstance(ImmutableList.of()));

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
        Statement.of(
            "create table foo (id bigint, value text, constraint chk_bar check (length(value) < 100),  primary key (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table foo (id bigint, value text, constraint chk_bar check (length(value) < 100), constraint pk_foo primary key (id))")));
    assertEquals(
        Statement.of(
            "create table foo (a int, b int, constraint fk_other foreign key (a, b) references other (a, b),  primary key (a, b))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table foo (a int, b int, constraint fk_other foreign key (a, b) references other (a, b), constraint pk_foo primary key (a, b))")));
    assertEquals(
        Statement.of("create table public.foo (id bigint, value text,  primary key (id) )"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table public.foo (id bigint, value text, constraint pk_foo primary key (id) )")));
    assertEquals(
        Statement.of("CREATE TABLE \"public\".\"Foo\" (id int,  PRIMARY KEY (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "CREATE TABLE \"public\".\"Foo\" (id int, CONSTRAINT \"pk_Foo\" PRIMARY KEY (id))")));
    assertEquals(
        Statement.of("CREATE TABLE foo(id int,PRIMARY KEY(id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of("CREATE TABLE foo(id int,CONSTRAINT\"pk_foo\"PRIMARY KEY(id))")));
    assertEquals(
        Statement.of("/* c */ CREATE TABLE foo (id int,  PRIMARY KEY (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of("/* c */ CREATE TABLE foo (id int, CONSTRAINT pk_foo PRIMARY KEY (id))")));
    assertEquals(
        Statement.of("-- c\nCREATE TABLE foo (id int,  PRIMARY KEY (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of("-- c\nCREATE TABLE foo (id int, CONSTRAINT pk_foo PRIMARY KEY (id))")));

    assertEquals(
        Statement.of(
            "CREATE TABLE \"user\" (\"id\" integer NOT NULL, \"firstName\" character varying NOT NULL, \"lastName\" character varying NOT NULL, \"age\" integer NOT NULL,  PRIMARY KEY (\"id\"))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "CREATE TABLE \"user\" (\"id\" integer NOT NULL, \"firstName\" character varying NOT NULL, \"lastName\" character varying NOT NULL, \"age\" integer NOT NULL, CONSTRAINT \"PK_user\" PRIMARY KEY (\"id\"))")));
    assertEquals(
        Statement.of("CREATE TABLE \"T\" (id int,  PRIMARY KEY (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of("CREATE TABLE \"T\" (id int, CONSTRAINT pk_t PRIMARY KEY (id))")));
    assertEquals(
        Statement.of("CREATE TABLE t (id int,  PRIMARY KEY (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of("CREATE TABLE t (id int, CONSTRAINT \"pk_t\" PRIMARY KEY (id))")));
    assertEquals(
        Statement.of("CREATE TABLE \"a\"\"b\" (id int,  PRIMARY KEY (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "CREATE TABLE \"a\"\"b\" (id int, CONSTRAINT \"pk_a\"\"b\" PRIMARY KEY (id))")));
    assertEquals(
        Statement.of("CREATE TABLE t (\"constraint\" text,  PRIMARY KEY (\"constraint\"))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "CREATE TABLE t (\"constraint\" text, CONSTRAINT pk_t PRIMARY KEY (\"constraint\"))")));
    assertEquals(
        Statement.of("CREATE TABLE t (id int,  /* c */ PRIMARY /* c */ KEY (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "CREATE TABLE t (id int, CONSTRAINT /* c */ pk_t /* c */ PRIMARY /* c */ KEY (id))")));
    assertEquals(
        Statement.of("CREATE TABLE t (id int, CONSTRAINT \"\" PRIMARY KEY (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of("CREATE TABLE t (id int, CONSTRAINT \"\" PRIMARY KEY (id))")));
    assertEquals(
        Statement.of("create table if not exists foo (id bigint,  primary key (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table if not exists foo (id bigint, constraint pk_foo primary key (id))")));
    assertEquals(
        Statement.of("CREATE TABLE \"a b\" (id int,  PRIMARY KEY (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of("CREATE TABLE \"a b\" (id int, CONSTRAINT \"pk_a b\" PRIMARY KEY (id))")));
    assertEquals(
        Statement.of("CREATE TABLE \"😀\" (id int,  PRIMARY KEY (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of("CREATE TABLE \"😀\" (id int, CONSTRAINT \"pk_😀\" PRIMARY KEY (id))")));
    assertEquals(
        Statement.of(
            "create table t (id bigint, s text default 'a''b), constraint pk_t primary key',  primary key (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table t (id bigint, s text default 'a''b), constraint pk_t primary key', constraint pk_t primary key (id))")));
    assertEquals(
        Statement.of("create table t (id bigint, s text default e'a\\' ),',  primary key (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table t (id bigint, s text default e'a\\' ),', constraint pk_t primary key (id))")));
    assertEquals(
        Statement.of("create table t (id bigint, s text default $$a,b)c$$,  primary key (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table t (id bigint, s text default $$a,b)c$$, constraint pk_t primary key (id))")));
    assertEquals(
        Statement.of("create table t (id bigint /* ) /* nested ) */ */, -- )\n  primary key (id))"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(
            Statement.of(
                "create table t (id bigint /* ) /* nested ) */ */, -- )\n constraint pk_t primary key (id))")));
    assertEquals(
        Statement.of("CREATE TABLE t ()"),
        ddlExecutor.maybeRemovePrimaryKeyConstraintName(Statement.of("CREATE TABLE t ()")));
  }

  @Test(timeout = 5000L)
  public void testMaybeRemovePrimaryKeyConstraintName_MalformedCreateTable() {
    DdlExecutor ddlExecutor =
        new DdlExecutor(mock(BackendConnection.class), Suppliers.ofInstance(ImmutableList.of()));

    ImmutableList<String> malformedStatements =
        ImmutableList.of(
            "CREATE TABLE",
            "CREATE TABLE (",
            "CREATE TABLE \"unclosed",
            "CREATE TABLE 123 (id int)",
            "CREATE TABLE t",
            "CREATE TABLE IF NOT EXISTS t",
            "CREATE TABLE t AS SELECT 1",
            "CREATE TABLE t (",
            "CREATE TABLE t ((a int))",
            "CREATE TABLE t ((a int",
            "CREATE TABLE t (a int",
            "CREATE TABLE t (a int\0",
            "CREATE TABLE t (a int   ",
            "CREATE TABLE t (a int CHECK (a > 0)",
            "CREATE TABLE t (a text DEFAULT 'unclosed)",
            "CREATE TABLE t (a text DEFAULT e'unclosed)",
            "CREATE TABLE t (a text DEFAULT $tag$unclosed $other$)",
            "CREATE TABLE t (a text DEFAULT $$unclosed)",
            "CREATE TABLE t (\"unclosed)",
            "CREATE TABLE t (a int /* unclosed)",
            "CREATE TABLE t (a int -- )",
            "CREATE TABLE t ($a $ $)",
            "CREATE TABLE t (CONSTRAINT",
            "CREATE TABLE t (CONSTRAINT ",
            "CREATE TABLE t (CONSTRAINT foo",
            "CREATE TABLE t (CONSTRAINT foo (",
            "CREATE TABLE t (id int, CONSTRAINT chk CHECK (a > 0",
            "CREATE TABLE t (id int, CONSTRAINT chk CHECK (s = 'unclosed",
            "CREATE TABLE t (id int, CONSTRAINT chk CHECK (/* unclosed)",
            "CREATE TABLE t (CONSTRAINT PRIMARY KEY (id))",
            "CREATE TABLE t (CONSTRAINT (id))",
            "CREATE TABLE t (CONSTRAINT 123 PRIMARY KEY (id))",
            "CREATE TABLE t (CONSTRAINT \"unclosed",
            "CREATE TABLE t (id int, CONSTRAINT foo PRIMARY, CONSTRAINT pk_t PRIMARY KEY (id))",
            "CREATE TABLE t (id int, CONSTRAINT pk_t",
            "CREATE TABLE t (id int, CONSTRAINT pk_t PRIMARY (id))",
            "CREATE TABLE t (id int, CONSTRAINT pk_t PRIMARY");

    for (String sql : malformedStatements) {
      Statement statement = Statement.of(sql);
      assertSame(sql, statement, ddlExecutor.maybeRemovePrimaryKeyConstraintName(statement));
      assertEquals(sql, statement, translate(sql, ddlExecutor));
    }
  }

  @Test
  public void testGetDependentStatements() {
    BackendConnection connection = mock(BackendConnection.class);
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.isSupportDropCascade()).thenReturn(true);
    when(connection.getSessionState()).thenReturn(sessionState);
    DdlExecutor ddlExecutor =
        new DdlExecutor(connection, Suppliers.ofInstance(ImmutableList.of())) {
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
            Statement.of("drop table if exists foo")),
        ddlExecutor.getDependentStatements(Statement.of("drop table if exists foo")));
    assertEquals(
        ImmutableList.of(
            Statement.of("drop dependent indexes of foo"),
            Statement.of("drop dependent foreign keys of foo"),
            Statement.of("drop table foo")),
        ddlExecutor.getDependentStatements(Statement.of("drop table foo cascade")));
    assertEquals(
        ImmutableList.of(
            Statement.of("drop dependent indexes of foo"),
            Statement.of("drop dependent foreign keys of foo"),
            Statement.of("drop table if exists foo")),
        ddlExecutor.getDependentStatements(Statement.of("drop table if exists foo cascade")));
    assertEquals(
        ImmutableList.of(
            Statement.of("drop all indexes in schema foo"),
            Statement.of("drop all foreign keys in schema foo"),
            Statement.of("drop all views in schema foo"),
            Statement.of("drop all tables in schema foo")),
        ddlExecutor.getDependentStatements(Statement.of("drop schema foo cascade")));
    assertEquals(
        ImmutableList.of(
            Statement.of("drop all indexes in schema foo"),
            Statement.of("drop all foreign keys in schema foo"),
            Statement.of("drop all views in schema foo"),
            Statement.of("drop all tables in schema foo")),
        ddlExecutor.getDependentStatements(Statement.of("drop schema if exists foo cascade")));
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
    DdlExecutor ddlExecutor = new DdlExecutor(connection, Suppliers.ofInstance(ImmutableList.of()));

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

  @Test
  public void testDdlReplacements() {
    DdlExecutor ddlExecutorWithoutReplacements =
        new DdlExecutor(mock(BackendConnection.class), Suppliers.ofInstance(ImmutableList.of()));
    String sql = "create table my_table (id bigint primary key, value varchar)";
    assertSame(sql, ddlExecutorWithoutReplacements.applyReplacers(sql));

    DdlExecutor ddlExecutorWithReplacements =
        new DdlExecutor(
            mock(BackendConnection.class),
            Suppliers.ofInstance(
                ImmutableList.of(
                    RegexQueryPartReplacer.replace(Pattern.compile("timestamp"), "timestamptz"),
                    RegexQueryPartReplacer.replaceAndStop(
                        Pattern.compile(
                            "create table databasechangelog \\(id bigint primary key\\)"),
                        "create table databasechangelog_replaced (id bigint primary key)"),
                    RegexQueryPartReplacer.replace(
                        Pattern.compile("create table databasechangelog_replaced"),
                        "create table databasechangelog_reverted"))));
    assertSame(sql, ddlExecutorWithReplacements.applyReplacers(sql));
    assertEquals(
        "create table my_table (id bigint primary key, value timestamptz)",
        applyReplacers(
            ddlExecutorWithReplacements,
            "create table my_table (id bigint primary key, value timestamp)"));
    assertEquals(
        "create table databasechangelog_replaced (id bigint primary key)",
        applyReplacers(
            ddlExecutorWithReplacements, "create table databasechangelog (id bigint primary key)"));
    assertEquals(
        "create table databasechangelog_replaced (id bigint primary key); create table databasechangelog_replaced (id bigint primary key)",
        applyReplacers(
            ddlExecutorWithReplacements,
            "create table databasechangelog (id bigint primary key); create table databasechangelog (id bigint primary key)"));
    assertEquals(
        "create table databasechangelog_reverted (id bigint primary key)",
        applyReplacers(
            ddlExecutorWithReplacements,
            "create table databasechangelog_replaced (id bigint primary key)"));
  }

  @Test
  public void testTableComparison() {
    assertEquals(1, new Table("public", "foo", "bar").compareTo(new Table("public", "baz", "foo")));
    assertEquals(-1, new Table("public", "foo", "bar").compareTo(new Table("public", "bar", null)));
  }

  private String applyReplacers(DdlExecutor ddlExecutor, String sql) {
    Statement statement = Statement.of(sql);
    ParsedStatement parsedStatement = PARSER.parse(Statement.of(sql));
    Tuple<ParsedStatement, Statement> result = ddlExecutor.replace(parsedStatement, statement);
    return result.y().getSql();
  }
}
