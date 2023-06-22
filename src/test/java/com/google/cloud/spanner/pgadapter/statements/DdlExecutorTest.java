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

import static com.google.cloud.spanner.pgadapter.statements.DdlExecutor.replaceTimestampWithTimestamptz;
import static com.google.cloud.spanner.pgadapter.statements.DdlExecutor.unquoteIdentifier;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
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
  public void testReplaceTimestampWithTimestamptz() {
    assertSameAfterReplace(Statement.of("create index foo on bar (col1, col2)"));
    assertSameAfterReplace(Statement.of("create table foo (col1 bigint, col2 varchar)"));
    assertSameAfterReplace(
        Statement.of(
            "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 varchar)"));
    assertSameAfterReplace(
        Statement.of(
            "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamptz)"));
    assertSameAfterReplace(
        Statement.of(
            "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamp with time zone)"));
    assertSameAfterReplace(
        Statement.of(
            "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamptz not null)"));
    assertSameAfterReplace(
        Statement.of(
            "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamp with time zone not null)"));

    assertEquals(
        Statement.of("create table foo (id bigint, value timestamptz)"),
        replaceTimestampWithTimestamptz(
            Statement.of("create table foo (id bigint, value timestamptz)")));
    assertEquals(
        Statement.of(
            "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamptz )"),
        replaceTimestampWithTimestamptz(
            Statement.of(
                "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamp)")));
    assertEquals(
        Statement.of(
            "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamp with time zone )"),
        replaceTimestampWithTimestamptz(
            Statement.of(
                "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamp without time zone)")));
    assertEquals(
        Statement.of(
            "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamptz not null)"),
        replaceTimestampWithTimestamptz(
            Statement.of(
                "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamp not null)")));
    assertEquals(
        Statement.of(
            "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamp with time zone  not null)"),
        replaceTimestampWithTimestamptz(
            Statement.of(
                "create table foo (col1 bigint, constraint chk_col1 check (col1 < 100), col2 timestamp without time zone not null)")));
    assertEquals(
        Statement.of(
            "create table foo (\n"
                + "  col1 bigint,\n"
                + "  constraint chk_col1 check (col1 < 100),\n"
                + "  col2 timestamp with time zone  not null\n"
                + ")"),
        replaceTimestampWithTimestamptz(
            Statement.of(
                "create table foo (\n"
                    + "  col1 bigint,\n"
                    + "  constraint chk_col1 check (col1 < 100),\n"
                    + "  col2 timestamp without time zone not null\n"
                    + ")")));
    assertEquals(
        Statement.of(
            "create table foo (\n"
                + "  col1 bigint,\n"
                + "  constraint chk_col1 check (col1 < 100),\n"
                + "  col2 timestamp with time zone  not null\n"
                + ")"),
        replaceTimestampWithTimestamptz(
            Statement.of(
                "create table foo (\n"
                    + "  col1 bigint,\n"
                    + "  constraint chk_col1 check (col1 < 100),\n"
                    + "  col2 timestamp /* we don't want a time zone */ without time zone not null\n"
                    + ")")));
    assertEquals(
        Statement.of(
            "create table foo (\n"
                + "  col1 bigint,\n"
                + "  primary key (col1),\n"
                + "  constraint chk_col1 check (col1 < 100),\n"
                + "  col2 timestamp with time zone  not null,\n"
                + "  col3 timestamptz default null,\n"
                + "  col4 varchar(100) not null,\n"
                + "  col5 timestamp with time zone ,\n"
                + "  col6 timestamp with time zone \n"
                + ")"),
        replaceTimestampWithTimestamptz(
            Statement.of(
                "create table foo (\n"
                    + "  col1 bigint,\n"
                    + "  primary key (col1),\n"
                    + "  constraint chk_col1 check (col1 < 100),\n"
                    + "  col2 timestamp without time zone not null,\n"
                    + "  col3 timestamp default null,\n"
                    + "  col4 varchar(100) not null,\n"
                    + "  col5 timestamp without    time zone,\n"
                    + "  col6       timestamp     without    time  zone\n"
                    + ")")));
  }

  private void assertSameAfterReplace(Statement statement) {
    assertSame(statement, replaceTimestampWithTimestamptz(statement));
  }
}
