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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Duration;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
  public static void setupBaseResults() throws Exception {
    SqlAlchemyBasicsTest.setupBaseResults();
  }

  @Test
  public void testInsertAllTypes() throws Exception {
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
  public void testInsertAllTypes_NullValues() throws Exception {
    // Note that the JSONB column is 'null' instead of NULL. That means that SQLAlchemy is inserting
    // a JSON null values instead of a SQL NULL value into the column. This can be changed by
    // creating the columns as Column(JSONB(none_as_null=True)) in the SQLAlchemy model.
    String sql =
        "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "VALUES (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'null')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 1L));

    String actualOutput = execute("orm_insert_null_values.py", host, pgServer.getLocalPort());
    String expectedOutput = "Inserted 1 row(s)\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Ignore("requires support for literals like '2022-12-16 10:11:12+01:00'::timestamptz")
  @Test
  public void testInsertAllTypesWithPreparedStatement() throws Exception {
    String sql =
        "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));

    String actualOutput =
        execute("orm_insert_with_prepared_statement.py", host, pgServer.getLocalPort());
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
  public void testSelectAllTypes() throws Exception {
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
  public void testGetAllTypes() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("all_types_", true)));

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
  public void testGetAllTypes_NullValues() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesNullResultSet("all_types_", 1L)));

    String actualOutput = execute("orm_get.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "AllTypes(col_bigint=     1,col_bool=       None,col_bytea=      Nonecol_float8=     Nonecol_int=        Nonecol_numeric=    Nonecol_timestamptz=Nonecol_date=       Nonecol_varchar=    Nonecol_jsonb=      None)\n";
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
  public void testGetAllTypesWithPreparedStatement() throws Exception {
    String sql = "select * from all_types where col_bigint=$1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createAllTypesResultSetMetadata("")
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64))
                                .getUndeclaredParameters()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(),
            createAllTypesResultSet("", true)));

    String actualOutput =
        execute("orm_get_with_prepared_statement.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14col_int=        100col_numeric=    Decimal('6.626')col_timestamptz=datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=datetime.timezone.utc)col_date=       datetime.date(2022, 3, 29)col_varchar=    'test'col_jsonb=      {'key': 'value'})\n";
    assertEquals(expectedOutput, actualOutput);

    // We receive 3 ExecuteSqlRequests:
    // 1. Internal metadata query from SQLAlchemy (ignored in this test).
    // 2. The SQL statement in PLAN mode to prepare the statement.
    // 3. The SQL statement in NORMAL mode with 1 as the parameter value.
    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest planRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    assertEquals(sql, planRequest.getSql());
    assertTrue(planRequest.getTransaction().hasBegin());
    assertTrue(planRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(sql, executeRequest.getSql());
    assertTrue(executeRequest.getTransaction().hasId());

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(2, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testOrmReadOnlyTransaction() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("all_types_", true)));

    String actualOutput = execute("orm_read_only_transaction.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14col_int=        100col_numeric=    Decimal('6.626')col_timestamptz=datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=datetime.timezone.utc)col_date=       datetime.date(2022, 3, 29)col_varchar=    'test'col_jsonb=      {'key': 'value'})\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, request.getSql());
    assertFalse(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    assertEquals(
        1,
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).stream()
            .filter(req -> req.getOptions().hasReadOnly())
            .count());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    // This rollback request comes from a system query before the actual data query.
    // The read-only transaction is not committed or rolled back.
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testStaleRead() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = %d";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(String.format(sql, 1)), createAllTypesResultSet("1", "all_types_", true)));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(String.format(sql, 2)), createAllTypesResultSet("2", "all_types_", true)));

    String actualOutput = execute("orm_stale_read.py", host, pgServer.getLocalPort());
    assertTrue(actualOutput, actualOutput.contains("AllTypes(col_bigint=     1"));
    assertTrue(actualOutput, actualOutput.contains("AllTypes(col_bigint=     2"));

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    for (int index = 1; index < 3; index++) {
      ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(index);
      assertTrue(request.getTransaction().hasSingleUse());
      assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
      assertTrue(request.getTransaction().getSingleUse().getReadOnly().hasMaxStaleness());
      assertEquals(
          Duration.newBuilder().setSeconds(10L).build(),
          request.getTransaction().getSingleUse().getReadOnly().getMaxStaleness());
    }
    // This rollback request comes from a system query before the actual data query.
    // The read-only transaction is not committed or rolled back.
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testUpdateAllTypes() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("all_types_", true)));
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
  public void testDeleteAllTypes() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("all_types_", true)));
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
  public void testRollback() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("all_types_", true)));
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

  @Test
  public void testCreateRelationships() throws Exception {
    String insertUserSql =
        "INSERT INTO user_account (name, fullname) "
            + "VALUES ('pkrabs', 'Pearl Krabs') "
            + "RETURNING user_account.id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertUserSql),
            SELECT1_RESULTSET
                .toBuilder()
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));
    String insertAddressesSql =
        "INSERT INTO address (email_address, user_id) "
            + "VALUES ('pearl.krabs@gmail.com', 1),('pearl@aol.com', 1) "
            + "RETURNING address.id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertAddressesSql),
            ResultSet.newBuilder()
                .setMetadata(SELECT1_RESULTSET.getMetadata())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(2L).build())
                .addRows(SELECT1_RESULTSET.getRows(0))
                .addRows(SELECT2_RESULTSET.getRows(0))
                .build()));

    String actualOutput = execute("orm_create_relationships.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "[]\n"
            + "[Address(id=None, email_address='pearl.krabs@gmail.com')]\n"
            + "User(id=None, name='pkrabs', fullname='Pearl Krabs')\n"
            + "[Address(id=None, email_address='pearl.krabs@gmail.com'), Address(id=None, email_address='pearl@aol.com')]\n"
            + "True\n"
            + "True\n"
            + "True\n"
            + "None\n"
            + "None\n"
            + "None\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest insertUserRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(insertUserSql, insertUserRequest.getSql());
    assertTrue(insertUserRequest.getTransaction().hasBegin());
    assertTrue(insertUserRequest.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest insertAddressesRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(insertAddressesSql, insertAddressesRequest.getSql());
    assertTrue(insertAddressesRequest.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testLoadRelationships() throws Exception {
    String selectUsersSql =
        "SELECT user_account.id, user_account.name, user_account.fullname \n"
            + "FROM user_account ORDER BY user_account.id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectUsersSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("spongebob"))
                        .addValues(Value.newBuilder().setStringValue("spongebob squarepants"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .addValues(Value.newBuilder().setStringValue("sandy"))
                        .addValues(Value.newBuilder().setStringValue("sandy oyster"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("3").build())
                        .addValues(Value.newBuilder().setStringValue("patrick"))
                        .addValues(Value.newBuilder().setStringValue("patrick sea"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("4").build())
                        .addValues(Value.newBuilder().setStringValue("squidward"))
                        .addValues(Value.newBuilder().setStringValue("squidward manyarms"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("5").build())
                        .addValues(Value.newBuilder().setStringValue("ehkrabs"))
                        .addValues(Value.newBuilder().setStringValue("ehkrabs hibernate"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("6").build())
                        .addValues(Value.newBuilder().setStringValue("pkrabs"))
                        .addValues(Value.newBuilder().setStringValue("pkrabs primary"))
                        .build())
                .build()));
    String selectAddressesSql =
        "SELECT address.user_id AS address_user_id, address.id AS address_id, address.email_address AS address_email_address \n"
            + "FROM address \n"
            + "WHERE address.user_id IN (1, 2, 3, 4, 5, 6)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectAddressesSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.INT64, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("1"))
                        .addValues(Value.newBuilder().setStringValue("spongebob@sqlalchemy.org"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .addValues(Value.newBuilder().setStringValue("2"))
                        .addValues(Value.newBuilder().setStringValue("sandy@sqlalchemy.org"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .addValues(Value.newBuilder().setStringValue("3"))
                        .addValues(Value.newBuilder().setStringValue("sandy@squirrelpower.org"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("6").build())
                        .addValues(Value.newBuilder().setStringValue("4"))
                        .addValues(Value.newBuilder().setStringValue("pearl.krabs@gmail.com"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("6").build())
                        .addValues(Value.newBuilder().setStringValue("5"))
                        .addValues(Value.newBuilder().setStringValue("pearl@aol.com"))
                        .build())
                .build()));

    String actualOutput = execute("orm_load_relationships.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "spongebob  (spongebob@sqlalchemy.org) \n"
            + "sandy  (sandy@sqlalchemy.org, sandy@squirrelpower.org) \n"
            + "patrick  () \n"
            + "squidward  () \n"
            + "ehkrabs  () \n"
            + "pkrabs  (pearl.krabs@gmail.com, pearl@aol.com) \n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest selectUsersRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(selectUsersSql, selectUsersRequest.getSql());
    assertTrue(selectUsersRequest.getTransaction().hasBegin());
    assertTrue(selectUsersRequest.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest selectAddressesRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(selectAddressesSql, selectAddressesRequest.getSql());
    assertTrue(selectAddressesRequest.getTransaction().hasId());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(2, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testErrorInReadWriteTransaction() throws Exception {
    String sql =
        "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "VALUES (1, true, '\\x74657374206279746573'::bytea, 3.14, 100, 6.626, '2011-11-04T00:05:23.123456+00:00'::timestamptz, '2011-11-04'::date, 'test string', '{\"key1\": \"value1\", \"key2\": \"value2\"}')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.ALREADY_EXISTS
                .withDescription("Row with id 1 already exists")
                .asRuntimeException()));

    String actualOutput =
        execute("orm_error_in_read_write_transaction.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "Insert failed: (psycopg2.errors.RaiseException) com.google.api.gax.rpc.AlreadyExistsException: io.grpc.StatusRuntimeException: ALREADY_EXISTS: Row with id 1 already exists";
    assertTrue(actualOutput, actualOutput.startsWith(expectedOutput));
    assertFalse(actualOutput, actualOutput.contains("Getting the row after an error succeeded"));

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testErrorInReadWriteTransactionContinue() throws Exception {
    String sql =
        "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "VALUES (1, true, '\\x74657374206279746573'::bytea, 3.14, 100, 6.626, '2011-11-04T00:05:23.123456+00:00'::timestamptz, '2011-11-04'::date, 'test string', '{\"key1\": \"value1\", \"key2\": \"value2\"}')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.ALREADY_EXISTS
                .withDescription("Row with id 1 already exists")
                .asRuntimeException()));

    String actualOutput =
        execute("orm_error_in_read_write_transaction_continue.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "Getting the row failed: This Session's transaction has been rolled back due to a previous exception during flush. "
            + "To begin a new transaction with this Session, first issue Session.rollback(). "
            + "Original exception was: (psycopg2.errors.RaiseException) com.google.api.gax.rpc.AlreadyExistsException: io.grpc.StatusRuntimeException: ALREADY_EXISTS: Row with id 1 already exists";
    assertTrue(actualOutput, actualOutput.startsWith(expectedOutput));

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }
}
