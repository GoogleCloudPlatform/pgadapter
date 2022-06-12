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

import static com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement.transformDeleteToSelectParams;
import static com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement.transformInsertToSelectParams;
import static com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement.transformUpdateToSelectParams;
import static com.google.cloud.spanner.pgadapter.statements.SimpleQueryStatement.splitStatements;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.PostgreSQLStatementParser;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ResultType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Comparator;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class IntermediateStatementTest {
  @Rule public MockitoRule rule = MockitoJUnit.rule();

  @Mock private ConnectionHandler connectionHandler;

  private static final PostgreSQLStatementParser PARSER =
      (PostgreSQLStatementParser) AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  private static ParsedStatement parse(String sql) {
    return PARSER.parse(Statement.of(sql));
  }

  @Mock private Connection connection;

  @Test
  public void testSplitStatements() {
    assertEquals(ImmutableList.of(""), splitStatements(""));
    assertEquals(ImmutableList.of("select 1"), splitStatements("select 1"));
    assertEquals(ImmutableList.of("select 1"), splitStatements("select 1;"));
    assertEquals(ImmutableList.of("select 1", "select 2"), splitStatements("select 1; select 2;"));
    assertEquals(
        ImmutableList.of("select '1;2'", "select 2"), splitStatements("select '1;2'; select 2;"));
    assertEquals(
        ImmutableList.of("select \"bobby;drop table\"", "select 2"),
        splitStatements("select \"bobby;drop table\"; select 2;"));
    assertEquals(
        ImmutableList.of("select '1'';\"2'", "select 2"),
        splitStatements("select '1'';\"2'; select 2;"));
    assertEquals(
        ImmutableList.of("select \"a\"\";b\"", "select 2"),
        splitStatements("select \"a\"\";b\"; select 2;"));
    assertEquals(
        ImmutableList.of("select '1'''", "select '2'"),
        splitStatements("select '1'''; select '2'"));
    assertEquals(
        ImmutableList.of("select '1'''", "select '2'''"),
        splitStatements("select '1'''; select '2'''"));
    assertEquals(
        ImmutableList.of("select 1", "select '2'';'"), splitStatements("select 1; select '2'';'"));
  }

  @Test
  public void testUpdateResultCount_ResultSet() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);

    IntermediateStatement statement =
        new IntermediateStatement(
            mock(OptionsMetadata.class), parse("select foo from bar"), connectionHandler);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true, false);
    StatementResult result = mock(StatementResult.class);
    when(result.getResultType()).thenReturn(ResultType.RESULT_SET);
    when(result.getResultSet()).thenReturn(resultSet);

    statement.setStatementResult(result);

    assertTrue(statement.hasMoreData);
    assertEquals(-1, statement.getUpdateCount());
    assertSame(resultSet, statement.getStatementResult().getResultSet());
  }

  @Test
  public void testUpdateResultCount_UpdateCount() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediateStatement statement =
        new IntermediateStatement(
            mock(OptionsMetadata.class), parse("update bar set foo=1"), connectionHandler);
    StatementResult result = mock(StatementResult.class);
    when(result.getResultType()).thenReturn(ResultType.UPDATE_COUNT);
    when(result.getUpdateCount()).thenReturn(100L);

    statement.setStatementResult(result);

    assertFalse(statement.hasMoreData);
    assertEquals(100L, statement.getUpdateCount());
    assertNotNull(statement.getStatementResult());
    assertEquals(ResultType.UPDATE_COUNT, statement.getStatementResult().getResultType());
  }

  @Test
  public void testUpdateResultCount_NoResult() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediateStatement statement =
        new IntermediateStatement(
            mock(OptionsMetadata.class),
            parse("create table bar (foo bigint primary key)"),
            connectionHandler);
    StatementResult result = mock(StatementResult.class);
    when(result.getResultType()).thenReturn(ResultType.NO_RESULT);

    statement.setStatementResult(result);

    assertFalse(statement.hasMoreData);
    assertEquals(0, statement.getUpdateCount());
    assertNotNull(statement.getStatementResult());
    assertEquals(ResultType.NO_RESULT, statement.getStatementResult().getResultType());
  }

  @Test
  public void testTransformInsertValuesToSelectParams() {
    assertEquals(
        "select $1, $2 from (select col1=$1, col2=$2 from foo) p",
        transformInsert("insert into foo (col1, col2) values ($1, $2)").getSql());
    assertEquals(
        "select $1, $2 from (select col1=$1, col2=$2 from foo) p",
        transformInsert("insert foo (col1, col2) values ($1, $2)").getSql());
    assertEquals(
        "select $1, $2, $3, $4 from (select col1=$1, col2=$2, col1=$3, col2=$4 from foo) p",
        transformInsert("insert into foo (col1, col2) values ($1, $2), ($3, $4)").getSql());
    assertEquals(
        "select $1, $2 from (select col1=$1::varchar, col2=$2::bigint from foo) p",
        transformInsert("insert into foo (col1, col2) values ($1::varchar, $2::bigint)").getSql());
    assertEquals(
        "select $1, $2, $3, $4 from (select col1=($1 + $2), col2=$3 || to_char($4) from foo) p",
        transformInsert("insert into foo (col1, col2) values (($1 + $2), $3 || to_char($4))")
            .getSql());
    assertEquals(
        "select $1, $2, $3, $4 from (select col1=($1 + $2), col2=$3 || to_char($4) from foo) p",
        transformInsert("insert into foo (col1, col2) values (($1 + $2), $3 || to_char($4))")
            .getSql());
    assertEquals(
        "select $1, $2, $3, $4, $5 from (select col1=$1 + $2 + 5, col2=$3 || to_char($4) || coalesce($5, '') from foo) p",
        transformInsert(
                "insert\ninto\nfoo\n(col1,\ncol2  ) values ($1 + $2 + 5, $3 || to_char($4) || coalesce($5, ''))")
            .getSql());
  }

  @Test
  public void testTransformInsertSelectToSelectParams() {
    assertEquals(
        "select $1 from (select * from bar where some_col=$1) p",
        transformInsert("insert into foo select * from bar where some_col=$1").getSql());
    assertEquals(
        "select $1 from (select * from bar where some_col=$1) p",
        transformInsert("insert foo select * from bar where some_col=$1").getSql());
    assertEquals(
        "select $1 from (select * from bar where some_col=$1) p",
        transformInsert("insert into foo (col1, col2) select * from bar where some_col=$1")
            .getSql());
    assertEquals(
        "select $1 from (select * from bar where some_col=$1) p",
        transformInsert("insert foo (col1, col2, col3) select * from bar where some_col=$1")
            .getSql());
    assertEquals(
        "select $1, $2 from (select * from bar where some_col=$1 limit $2) p",
        transformInsert(
                "insert foo (col1, col2, col3) select * from bar where some_col=$1 limit $2")
            .getSql());
  }

  @Test
  public void testTransformUpdateToSelectParams() {
    assertEquals(
        "select $1, $2, $3 from (select col1=$1, col2=$2 from foo where id=$3) p",
        transformUpdate("update foo set col1=$1, col2=$2  where id=$3").getSql());
    assertEquals(
        "select $1, $2, $3 from (select col1=col2 + $1 , "
            + "col2=coalesce($1, $2, $3, to_char(current_timestamp())), "
            + "col3 = 15 "
            + "from foo where id=$3 and value>100) p",
        transformUpdate(
                "update foo set col1=col2 + $1 , "
                    + "col2=coalesce($1, $2, $3, to_char(current_timestamp())), "
                    + "col3 = 15 "
                    + "where id=$3 and value>100")
            .getSql());
    assertEquals(
        "select $1 from (select col1=$1 from foo) p",
        transformUpdate("update foo set col1=$1").getSql());

    assertNull(transformUpdate("update foo col1=1"));
    assertNull(transformUpdate("update foo col1=1 hwere col1=2"));
    assertNull(transformUpdate("udpate foo col1=1 where col1=2"));
  }

  @Test
  public void testTransformDeleteToSelectParams() {
    assertEquals(
        "select $1 from (select 1 from foo where id=$1) p",
        transformDelete("delete from foo where id=$1").getSql());
    assertEquals(
        "select $1, $2 from (select 1 from foo where id=$1 and bar > $2) p",
        transformDelete("delete foo\nwhere id=$1 and bar > $2").getSql());
    assertEquals(
        "select $1, $2, $3, $4, $5, $6, $7, $8, $9 "
            + "from (select 1 from all_types "
            + "where col_bigint=$1 "
            + "and col_bool=$2 "
            + "and col_bytea=$3 "
            + "and col_float8=$4 "
            + "and col_int=$5 "
            + "and col_numeric=$6 "
            + "and col_timestamptz=$7 "
            + "and col_date=$8 "
            + "and col_varchar=$9"
            + ") p",
        transformDelete(
                "delete "
                    + "from all_types "
                    + "where col_bigint=$1 "
                    + "and col_bool=$2 "
                    + "and col_bytea=$3 "
                    + "and col_float8=$4 "
                    + "and col_int=$5 "
                    + "and col_numeric=$6 "
                    + "and col_timestamptz=$7 "
                    + "and col_date=$8 "
                    + "and col_varchar=$9")
            .getSql());

    assertNull(transformDelete("delete from foo"));
    assertNull(transformDelete("dlete from foo where id=$1"));
    assertNull(transformDelete("delete from foo hwere col1=2"));
  }

  private static Statement transformInsert(String sql) {
    Set<String> parameters =
        ImmutableSortedSet.<String>orderedBy(Comparator.comparing(o -> o.substring(1)))
            .addAll(PARSER.getQueryParameters(sql))
            .build();
    return transformInsertToSelectParams(mock(Connection.class), sql, parameters);
  }

  private static Statement transformUpdate(String sql) {
    Set<String> parameters =
        ImmutableSortedSet.<String>orderedBy(Comparator.comparing(o -> o.substring(1)))
            .addAll(PARSER.getQueryParameters(sql))
            .build();
    return transformUpdateToSelectParams(sql, parameters);
  }

  private static Statement transformDelete(String sql) {
    Set<String> parameters =
        ImmutableSortedSet.<String>orderedBy(Comparator.comparing(o -> o.substring(1)))
            .addAll(PARSER.getQueryParameters(sql))
            .build();
    return transformDeleteToSelectParams(sql, parameters);
  }
}
