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

package com.google.cloud.spanner.pgadapter.python.sqlalchemy;

import static com.google.cloud.spanner.pgadapter.python.sqlalchemy.SqlAlchemyBasicsTest.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.RollbackRequest;
import java.io.IOException;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(PythonTest.class)
public class SqlAlchemyOrmTest extends AbstractMockServerTest {

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {""});
  }

  @BeforeClass
  public static void setupBaseResults() {
    SqlAlchemyBasicsTest.setupBaseResults();
  }

  @Test
  public void testInsertAllTypes() throws IOException, InterruptedException {
    String sql =
        "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "VALUES (1, true, '\\x74657374206279746573'::bytea, 3.14, 100, 6.626, '2011-11-04T00:05:23.123456+00:00'::timestamptz, '2011-11-04'::date, 'test string', '{\"key1\": \"value1\", \"key2\": \"value2\"}')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 1L));

    String actualOutput = execute("orm_insert.py", host, pgServer.getLocalPort());
    String expectedOutput = "Inserted 1 row(s)\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testSelectAllTypes() throws IOException, InterruptedException {
    String sql =
        "SELECT all_types.col_bigint, all_types.col_bool, all_types.col_bytea, all_types.col_float8, all_types.col_int, all_types.col_numeric, all_types.col_timestamptz, all_types.col_date, all_types.col_varchar, all_types.col_jsonb \n"
            + "FROM all_types";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("", true)));

    String actualOutput = execute("orm_select_first.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14col_int=        100col_numeric=    Decimal('6.626')col_timestamptz=datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=datetime.timezone.utc)col_date=       datetime.date(2022, 3, 29)col_varchar=    'test'col_jsonb=      {'key': 'value'})\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(2, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testGetAllTypes() throws IOException, InterruptedException {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("", true)));

    String actualOutput = execute("orm_get.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14col_int=        100col_numeric=    Decimal('6.626')col_timestamptz=datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=datetime.timezone.utc)col_date=       datetime.date(2022, 3, 29)col_varchar=    'test'col_jsonb=      {'key': 'value'})\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(2, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testUpdateAllTypes() throws IOException, InterruptedException {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("", true)));
    String updateSql =
        "UPDATE all_types SET col_varchar='updated string' WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSql), 1L));

    String actualOutput = execute("orm_update.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14col_int=        100col_numeric=    Decimal('6.626')col_timestamptz=datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=datetime.timezone.utc)col_date=       datetime.date(2022, 3, 29)col_varchar=    'updated string'col_jsonb=      {'key': 'value'})\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest selectRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, selectRequest.getSql());
    assertTrue(selectRequest.getTransaction().hasBegin());
    assertTrue(selectRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest updateRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(updateSql, updateRequest.getSql());
    assertTrue(updateRequest.getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testDeleteAllTypes() throws IOException, InterruptedException {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("", true)));
    String deleteSql = "DELETE FROM all_types WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(deleteSql), 1L));

    String actualOutput = execute("orm_delete.py", host, pgServer.getLocalPort());
    String expectedOutput = "deleted row\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest selectRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, selectRequest.getSql());
    assertTrue(selectRequest.getTransaction().hasBegin());
    assertTrue(selectRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest deleteRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(deleteSql, deleteRequest.getSql());
    assertTrue(deleteRequest.getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testRollback() throws IOException, InterruptedException {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("", true)));
    String updateSql =
        "UPDATE all_types SET col_varchar='updated string' WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSql), 1L));

    String actualOutput = execute("orm_rollback.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "Before rollback: AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14col_int=        100col_numeric=    Decimal('6.626')col_timestamptz=datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=datetime.timezone.utc)col_date=       datetime.date(2022, 3, 29)col_varchar=    'updated string'col_jsonb=      {'key': 'value'})\n"
            + "After rollback: AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14col_int=        100col_numeric=    Decimal('6.626')col_timestamptz=datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=datetime.timezone.utc)col_date=       datetime.date(2022, 3, 29)col_varchar=    'test'col_jsonb=      {'key': 'value'})\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest selectRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, selectRequest.getSql());
    assertTrue(selectRequest.getTransaction().hasBegin());
    assertTrue(selectRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest updateRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(updateSql, updateRequest.getSql());
    assertTrue(updateRequest.getTransaction().hasId());

    ExecuteSqlRequest refreshRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(sql, refreshRequest.getSql());
    assertTrue(refreshRequest.getTransaction().hasBegin());
    assertTrue(refreshRequest.getTransaction().getBegin().hasReadWrite());

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(3, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }
}
