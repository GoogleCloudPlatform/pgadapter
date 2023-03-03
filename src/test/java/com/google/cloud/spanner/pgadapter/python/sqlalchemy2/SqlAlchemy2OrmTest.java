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

package com.google.cloud.spanner.pgadapter.python.sqlalchemy2;

import static com.google.cloud.spanner.pgadapter.python.sqlalchemy2.SqlAlchemy2BasicsTest.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
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
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import java.util.Arrays;
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
public class SqlAlchemy2OrmTest extends AbstractMockServerTest {
  private static final com.google.cloud.spanner.Value UNTYPED_NULL_VALUE = null;

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {""});
  }

  @BeforeClass
  public static void setupBaseResults() throws Exception {
    SqlAlchemy2BasicsTest.setupBaseResults();
  }

  @Test
  public void testInsertAllTypes() throws Exception {
    String sql = getInsertAllTypesSql();
    addDescribeInsertAllTypesResult();
    Statement statement =
        Statement.newBuilder(sql)
            .bind("p1")
            .to(1L)
            .bind("p2")
            .to(true)
            .bind("p3")
            .to(ByteArray.copyFrom("test bytes"))
            .bind("p4")
            .to(3.14d)
            .bind("p5")
            .to(100)
            .bind("p6")
            .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
            .bind("p7")
            .to(Timestamp.parseTimestamp("2011-11-04T00:05:23.123456000Z"))
            .bind("p8")
            .to(Date.parseDate("2011-11-04"))
            .bind("p9")
            .to("test string")
            .bind("p10")
            .to(
                com.google.cloud.spanner.Value.pgJsonb(
                    "{\"key1\": \"value1\", \"key2\": \"value2\"}"))
            .bind("p11")
            .toInt64Array(Arrays.asList(1L, null, 2L))
            .bind("p12")
            .toBoolArray(Arrays.asList(true, null, false))
            .bind("p13")
            .toBytesArray(
                Arrays.asList(ByteArray.copyFrom("bytes1"), null, ByteArray.copyFrom("bytes2")))
            .bind("p14")
            .toFloat64Array(Arrays.asList(-3.14d, null, 99.99d))
            .bind("p15")
            .toInt64Array(Arrays.asList(-100L, null, -200L))
            .bind("p16")
            .toPgNumericArray(Arrays.asList("-6.626", null, "99.99"))
            .bind("p17")
            .toTimestampArray(
                Arrays.asList(
                    Timestamp.parseTimestamp("2010-11-08T17:33:12Z"),
                    null,
                    Timestamp.parseTimestamp("2012-05-04T23:05:23.123Z")))
            .bind("p18")
            .toDateArray(
                Arrays.asList(Date.parseDate("2010-11-08"), null, Date.parseDate("2012-05-05")))
            .bind("p19")
            .toStringArray(Arrays.asList("string1", null, "string2"))
            .bind("p20")
            .toPgJsonbArray(
                Arrays.asList(
                    "{\"key1\": \"value1\", \"key2\": \"value2\"}",
                    "null",
                    "{\"key1\": \"value3\", \"key2\": \"value4\"}"))
            .build();
    mockSpanner.putStatementResult(StatementResult.update(statement, 1L));

    String actualOutput = execute("orm_insert.py", host, pgServer.getLocalPort());
    String expectedOutput = "Inserted 1 row(s)\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest describeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertAllTypes_NullValues() throws Exception {
    // Note that the JSONB column is 'null' instead of NULL. That means that SQLAlchemy is inserting
    // a JSON null values instead of a SQL NULL value into the column. This can be changed by
    // creating the columns as Column(JSONB(none_as_null=True)) in the SQLAlchemy model.
    String sql = getInsertAllTypesSql();
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(UNTYPED_NULL_VALUE)
                .bind("p3")
                .to(UNTYPED_NULL_VALUE)
                .bind("p4")
                .to(UNTYPED_NULL_VALUE)
                .bind("p5")
                .to(UNTYPED_NULL_VALUE)
                .bind("p6")
                .to(UNTYPED_NULL_VALUE)
                .bind("p7")
                .to(UNTYPED_NULL_VALUE)
                .bind("p8")
                .to(UNTYPED_NULL_VALUE)
                .bind("p9")
                .to(UNTYPED_NULL_VALUE)
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("null"))
                .bind("p11")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p12")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p13")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p14")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p15")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p16")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p17")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p18")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p19")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p20")
                .to((com.google.cloud.spanner.Value) null)
                .build(),
            1L));

    String actualOutput = execute("orm_insert_null_values.py", host, pgServer.getLocalPort());
    String expectedOutput = "Inserted 1 row(s)\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testSelectAllTypes() throws Exception {
    String sql =
        "SELECT all_types.col_bigint, all_types.col_bool, all_types.col_bytea, all_types.col_float8, all_types.col_int, all_types.col_numeric, all_types.col_timestamptz, all_types.col_date, all_types.col_varchar, all_types.col_jsonb, all_types.col_array_bigint, all_types.col_array_bool, all_types.col_array_bytea, all_types.col_array_float8, all_types.col_array_int, all_types.col_array_numeric, all_types.col_array_timestamptz, all_types.col_array_date, all_types.col_array_varchar, all_types.col_array_jsonb \n"
            + "FROM all_types";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("", true)));

    String actualOutput = execute("orm_select_first.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'"
            + "col_float8=     3.14col_int=        100col_numeric=    Decimal('6.626')"
            + "col_timestamptz='2022-02-16T13:18:02.123456+00:00'"
            + "col_date=       datetime.date(2022, 3, 29)"
            + "col_varchar=    'test'col_jsonb=      {'key': 'value'}"
            + "col_array_bigint=     [1, None, 2]"
            + "col_array_bool=       [True, None, False]"
            + "col_array_bytea=      [b'bytes1', None, b'bytes2']"
            + "col_array_float8=     [3.14, None, -99.99]"
            + "col_array_int=        [-100, None, -200]"
            + "col_array_numeric=    [Decimal('6.626'), None, Decimal('-3.14')]"
            + "col_array_timestamptz=[datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]"
            + "col_array_date=       [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]"
            + "col_array_varchar=    ['string1', None, 'string2']"
            + "col_array_jsonb=      [{'key': 'value1'}, None, {'key': 'value2'}])\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(2, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testGetAllTypes() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb, all_types.col_array_bigint AS all_types_col_array_bigint, all_types.col_array_bool AS all_types_col_array_bool, all_types.col_array_bytea AS all_types_col_array_bytea, all_types.col_array_float8 AS all_types_col_array_float8, all_types.col_array_int AS all_types_col_array_int, all_types.col_array_numeric AS all_types_col_array_numeric, all_types.col_array_timestamptz AS all_types_col_array_timestamptz, all_types.col_array_date AS all_types_col_array_date, all_types.col_array_varchar AS all_types_col_array_varchar, all_types.col_array_jsonb AS all_types_col_array_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = $1::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(),
            createAllTypesResultSet("", true)));

    String actualOutput = execute("orm_get.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14"
            + "col_int=        100col_numeric=    Decimal('6.626')"
            + "col_timestamptz='2022-02-16T13:18:02.123456+00:00'"
            + "col_date=       datetime.date(2022, 3, 29)"
            + "col_varchar=    'test'col_jsonb=      {'key': 'value'}"
            + "col_array_bigint=     [1, None, 2]"
            + "col_array_bool=       [True, None, False]"
            + "col_array_bytea=      [b'bytes1', None, b'bytes2']"
            + "col_array_float8=     [3.14, None, -99.99]"
            + "col_array_int=        [-100, None, -200]"
            + "col_array_numeric=    [Decimal('6.626'), None, Decimal('-3.14')]"
            + "col_array_timestamptz=[datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]"
            + "col_array_date=       [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]"
            + "col_array_varchar=    ['string1', None, 'string2']"
            + "col_array_jsonb=      [{'key': 'value1'}, None, {'key': 'value2'}])\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(2, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testGetAllTypes_NullValues() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb, all_types.col_array_bigint AS all_types_col_array_bigint, all_types.col_array_bool AS all_types_col_array_bool, all_types.col_array_bytea AS all_types_col_array_bytea, all_types.col_array_float8 AS all_types_col_array_float8, all_types.col_array_int AS all_types_col_array_int, all_types.col_array_numeric AS all_types_col_array_numeric, all_types.col_array_timestamptz AS all_types_col_array_timestamptz, all_types.col_array_date AS all_types_col_array_date, all_types.col_array_varchar AS all_types_col_array_varchar, all_types.col_array_jsonb AS all_types_col_array_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = $1::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(),
            createAllTypesNullResultSet("", 1L)));

    String actualOutput = execute("orm_get.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "AllTypes(col_bigint=     1,col_bool=       None,col_bytea=      Nonecol_float8=     Nonecol_int=        Nonecol_numeric=    Nonecol_timestamptz=Nonecol_date=       Nonecol_varchar=    Nonecol_jsonb=      Nonecol_array_bigint=     Nonecol_array_bool=       Nonecol_array_bytea=      Nonecol_array_float8=     Nonecol_array_int=        Nonecol_array_numeric=    Nonecol_array_timestamptz=Nonecol_array_date=       Nonecol_array_varchar=    Nonecol_array_jsonb=      None)\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(2, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testOrmReadOnlyTransaction() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb, all_types.col_array_bigint AS all_types_col_array_bigint, all_types.col_array_bool AS all_types_col_array_bool, all_types.col_array_bytea AS all_types_col_array_bytea, all_types.col_array_float8 AS all_types_col_array_float8, all_types.col_array_int AS all_types_col_array_int, all_types.col_array_numeric AS all_types_col_array_numeric, all_types.col_array_timestamptz AS all_types_col_array_timestamptz, all_types.col_array_date AS all_types_col_array_date, all_types.col_array_varchar AS all_types_col_array_varchar, all_types.col_array_jsonb AS all_types_col_array_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = $1::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(),
            createAllTypesResultSet("", true)));

    String actualOutput = execute("orm_read_only_transaction.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14"
            + "col_int=        100col_numeric=    Decimal('6.626')"
            + "col_timestamptz='2022-02-16T13:18:02.123456+00:00'"
            + "col_date=       datetime.date(2022, 3, 29)col_varchar=    'test'"
            + "col_jsonb=      {'key': 'value'}"
            + "col_array_bigint=     [1, None, 2]"
            + "col_array_bool=       [True, None, False]"
            + "col_array_bytea=      [b'bytes1', None, b'bytes2']"
            + "col_array_float8=     [3.14, None, -99.99]"
            + "col_array_int=        [-100, None, -200]"
            + "col_array_numeric=    [Decimal('6.626'), None, Decimal('-3.14')]"
            + "col_array_timestamptz=[datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]"
            + "col_array_date=       [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]"
            + "col_array_varchar=    ['string1', None, 'string2']"
            + "col_array_jsonb=      [{'key': 'value1'}, None, {'key': 'value2'}])\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
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
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb, all_types.col_array_bigint AS all_types_col_array_bigint, all_types.col_array_bool AS all_types_col_array_bool, all_types.col_array_bytea AS all_types_col_array_bytea, all_types.col_array_float8 AS all_types_col_array_float8, all_types.col_array_int AS all_types_col_array_int, all_types.col_array_numeric AS all_types_col_array_numeric, all_types.col_array_timestamptz AS all_types_col_array_timestamptz, all_types.col_array_date AS all_types_col_array_date, all_types.col_array_varchar AS all_types_col_array_varchar, all_types.col_array_jsonb AS all_types_col_array_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = $1::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(),
            createAllTypesResultSet("1", "", true)));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(2L).build(),
            createAllTypesResultSet("2", "", true)));

    String actualOutput = execute("orm_stale_read.py", host, pgServer.getLocalPort());
    assertTrue(actualOutput, actualOutput.contains("AllTypes(col_bigint=     1"));
    assertTrue(actualOutput, actualOutput.contains("AllTypes(col_bigint=     2"));

    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    for (int index = 2; index < 4; index++) {
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
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb, all_types.col_array_bigint AS all_types_col_array_bigint, all_types.col_array_bool AS all_types_col_array_bool, all_types.col_array_bytea AS all_types_col_array_bytea, all_types.col_array_float8 AS all_types_col_array_float8, all_types.col_array_int AS all_types_col_array_int, all_types.col_array_numeric AS all_types_col_array_numeric, all_types.col_array_timestamptz AS all_types_col_array_timestamptz, all_types.col_array_date AS all_types_col_array_date, all_types.col_array_varchar AS all_types_col_array_varchar, all_types.col_array_jsonb AS all_types_col_array_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = $1::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(),
            createAllTypesResultSet("", true)));
    String updateSql =
        "UPDATE all_types SET col_varchar=$1::VARCHAR WHERE all_types.col_bigint = $2::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(updateSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(updateSql)
                .bind("p1")
                .to("updated string")
                .bind("p2")
                .to(1L)
                .build(),
            1L));

    String actualOutput = execute("orm_update.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14"
            + "col_int=        100col_numeric=    Decimal('6.626')"
            + "col_timestamptz='2022-02-16T13:18:02.123456+00:00'"
            + "col_date=       datetime.date(2022, 3, 29)col_varchar=    'updated string'"
            + "col_jsonb=      {'key': 'value'}"
            + "col_array_bigint=     [1, None, 2]"
            + "col_array_bool=       [True, None, False]"
            + "col_array_bytea=      [b'bytes1', None, b'bytes2']"
            + "col_array_float8=     [3.14, None, -99.99]"
            + "col_array_int=        [-100, None, -200]"
            + "col_array_numeric=    [Decimal('6.626'), None, Decimal('-3.14')]"
            + "col_array_timestamptz=[datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]"
            + "col_array_date=       [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]"
            + "col_array_varchar=    ['string1', None, 'string2']"
            + "col_array_jsonb=      [{'key': 'value1'}, None, {'key': 'value2'}])\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(5, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest selectRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql, selectRequest.getSql());
    assertTrue(selectRequest.getTransaction().hasBegin());
    assertTrue(selectRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest describeUpdateRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(QueryMode.PLAN, describeUpdateRequest.getQueryMode());
    assertEquals(updateSql, describeUpdateRequest.getSql());
    assertTrue(describeUpdateRequest.getTransaction().hasId());

    ExecuteSqlRequest executeUpdateRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(4);
    assertEquals(QueryMode.NORMAL, executeUpdateRequest.getQueryMode());
    assertEquals(updateSql, executeUpdateRequest.getSql());
    assertTrue(executeUpdateRequest.getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testDeleteAllTypes() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb, all_types.col_array_bigint AS all_types_col_array_bigint, all_types.col_array_bool AS all_types_col_array_bool, all_types.col_array_bytea AS all_types_col_array_bytea, all_types.col_array_float8 AS all_types_col_array_float8, all_types.col_array_int AS all_types_col_array_int, all_types.col_array_numeric AS all_types_col_array_numeric, all_types.col_array_timestamptz AS all_types_col_array_timestamptz, all_types.col_array_date AS all_types_col_array_date, all_types.col_array_varchar AS all_types_col_array_varchar, all_types.col_array_jsonb AS all_types_col_array_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = $1::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(),
            createAllTypesResultSet("", true)));
    String deleteSql = "DELETE FROM all_types WHERE all_types.col_bigint = $1::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(deleteSql).bind("p1").to(1L).build(), 1L));

    String actualOutput = execute("orm_delete.py", host, pgServer.getLocalPort());
    String expectedOutput = "deleted row\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest selectRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql, selectRequest.getSql());
    assertTrue(selectRequest.getTransaction().hasBegin());
    assertTrue(selectRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest deleteRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(deleteSql, deleteRequest.getSql());
    assertTrue(deleteRequest.getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testRollback() throws Exception {
    String sql =
        "SELECT all_types.col_bigint AS all_types_col_bigint, all_types.col_bool AS all_types_col_bool, all_types.col_bytea AS all_types_col_bytea, all_types.col_float8 AS all_types_col_float8, all_types.col_int AS all_types_col_int, all_types.col_numeric AS all_types_col_numeric, all_types.col_timestamptz AS all_types_col_timestamptz, all_types.col_date AS all_types_col_date, all_types.col_varchar AS all_types_col_varchar, all_types.col_jsonb AS all_types_col_jsonb, all_types.col_array_bigint AS all_types_col_array_bigint, all_types.col_array_bool AS all_types_col_array_bool, all_types.col_array_bytea AS all_types_col_array_bytea, all_types.col_array_float8 AS all_types_col_array_float8, all_types.col_array_int AS all_types_col_array_int, all_types.col_array_numeric AS all_types_col_array_numeric, all_types.col_array_timestamptz AS all_types_col_array_timestamptz, all_types.col_array_date AS all_types_col_array_date, all_types.col_array_varchar AS all_types_col_array_varchar, all_types.col_array_jsonb AS all_types_col_array_jsonb \n"
            + "FROM all_types \n"
            + "WHERE all_types.col_bigint = $1::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(),
            createAllTypesResultSet("", true)));
    String updateSql =
        "UPDATE all_types SET col_varchar=$1::VARCHAR WHERE all_types.col_bigint = $2::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(updateSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(updateSql)
                .bind("p1")
                .to("updated string")
                .bind("p2")
                .to(1L)
                .build(),
            1L));

    String actualOutput = execute("orm_rollback.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "Before rollback: AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14col_int=        100col_numeric=    Decimal('6.626')col_timestamptz='2022-02-16T13:18:02.123456+00:00'col_date=       datetime.date(2022, 3, 29)col_varchar=    'updated string'col_jsonb=      {'key': 'value'}col_array_bigint=     [1, None, 2]col_array_bool=       [True, None, False]col_array_bytea=      [b'bytes1', None, b'bytes2']col_array_float8=     [3.14, None, -99.99]col_array_int=        [-100, None, -200]col_array_numeric=    [Decimal('6.626'), None, Decimal('-3.14')]col_array_timestamptz=[datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]col_array_date=       [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]col_array_varchar=    ['string1', None, 'string2']col_array_jsonb=      [{'key': 'value1'}, None, {'key': 'value2'}])\n"
            + "After rollback: AllTypes(col_bigint=     1,col_bool=       True,col_bytea=      b'test'col_float8=     3.14col_int=        100col_numeric=    Decimal('6.626')col_timestamptz='2022-02-16T13:18:02.123456+00:00'col_date=       datetime.date(2022, 3, 29)col_varchar=    'test'col_jsonb=      {'key': 'value'}col_array_bigint=     [1, None, 2]col_array_bool=       [True, None, False]col_array_bytea=      [b'bytes1', None, b'bytes2']col_array_float8=     [3.14, None, -99.99]col_array_int=        [-100, None, -200]col_array_numeric=    [Decimal('6.626'), None, Decimal('-3.14')]col_array_timestamptz=[datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]col_array_date=       [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]col_array_varchar=    ['string1', None, 'string2']col_array_jsonb=      [{'key': 'value1'}, None, {'key': 'value2'}])\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest selectRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql, selectRequest.getSql());
    assertTrue(selectRequest.getTransaction().hasBegin());
    assertTrue(selectRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest describeUpdateRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(QueryMode.PLAN, describeUpdateRequest.getQueryMode());
    assertEquals(updateSql, describeUpdateRequest.getSql());
    assertTrue(describeUpdateRequest.getTransaction().hasId());

    ExecuteSqlRequest executeUpdateRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(4);
    assertEquals(QueryMode.NORMAL, executeUpdateRequest.getQueryMode());
    assertEquals(updateSql, executeUpdateRequest.getSql());
    assertTrue(executeUpdateRequest.getTransaction().hasId());

    ExecuteSqlRequest refreshRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(5);
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
            + "VALUES ($1::VARCHAR(30), $2::VARCHAR) "
            + "RETURNING user_account.id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertUserSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(ImmutableList.of(TypeCode.INT64))
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(TypeCode.STRING, TypeCode.STRING))
                                .getUndeclaredParameters()))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertUserSql)
                .bind("p1")
                .to("pkrabs")
                .bind("p2")
                .to("Pearl Krabs")
                .build(),
            SELECT1_RESULTSET
                .toBuilder()
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));
    String insertAddressesSql =
        "INSERT INTO address (email_address, user_id) "
            + "VALUES ($1::VARCHAR, $2::INTEGER), "
            + "($3::VARCHAR, $4::INTEGER) "
            + "RETURNING address.id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertAddressesSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(ImmutableList.of(TypeCode.INT64))
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.STRING,
                                        TypeCode.INT64,
                                        TypeCode.STRING,
                                        TypeCode.INT64))
                                .getUndeclaredParameters()))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertAddressesSql)
                .bind("p1")
                .to("pearl.krabs@gmail.com")
                .bind("p2")
                .to(1L)
                .bind("p3")
                .to("pearl@aol.com")
                .bind("p4")
                .to(1L)
                .build(),
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

    assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest describeInsertUserRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(QueryMode.PLAN, describeInsertUserRequest.getQueryMode());
    assertEquals(insertUserSql, describeInsertUserRequest.getSql());
    assertTrue(describeInsertUserRequest.getTransaction().hasBegin());
    assertTrue(describeInsertUserRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeInsertUserRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(QueryMode.NORMAL, executeInsertUserRequest.getQueryMode());
    assertEquals(insertUserSql, executeInsertUserRequest.getSql());
    assertTrue(executeInsertUserRequest.getTransaction().hasId());

    ExecuteSqlRequest describeInsertAddressesRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(4);
    assertEquals(QueryMode.PLAN, describeInsertAddressesRequest.getQueryMode());
    assertEquals(insertAddressesSql, describeInsertAddressesRequest.getSql());
    assertTrue(describeInsertAddressesRequest.getTransaction().hasId());

    ExecuteSqlRequest executeInsertAddressesRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(5);
    assertEquals(QueryMode.NORMAL, executeInsertAddressesRequest.getQueryMode());
    assertEquals(insertAddressesSql, executeInsertAddressesRequest.getSql());
    assertTrue(executeInsertAddressesRequest.getTransaction().hasId());

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
            + "WHERE address.user_id IN ($1::INTEGER, $2::INTEGER, $3::INTEGER, $4::INTEGER, $5::INTEGER, $6::INTEGER)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectAddressesSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(2L)
                .bind("p3")
                .to(3L)
                .bind("p4")
                .to(4L)
                .bind("p5")
                .to(5L)
                .bind("p6")
                .to(6L)
                .build(),
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

    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest selectUsersRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(selectUsersSql, selectUsersRequest.getSql());
    assertTrue(selectUsersRequest.getTransaction().hasBegin());
    assertTrue(selectUsersRequest.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest selectAddressesRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(selectAddressesSql, selectAddressesRequest.getSql());
    assertTrue(selectAddressesRequest.getTransaction().hasId());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(2, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testErrorInReadWriteTransaction() throws Exception {
    String sql = getInsertAllTypesSql();
    addDescribeInsertAllTypesResult();
    Statement statement =
        Statement.newBuilder(sql)
            .bind("p1")
            .to(1L)
            .bind("p2")
            .to(true)
            .bind("p3")
            .to(ByteArray.copyFrom("test bytes"))
            .bind("p4")
            .to(3.14d)
            .bind("p5")
            .to(100)
            .bind("p6")
            .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
            .bind("p7")
            .to(Timestamp.parseTimestamp("2011-11-04T00:05:23.123456000Z"))
            .bind("p8")
            .to(Date.parseDate("2011-11-04"))
            .bind("p9")
            .to("test string")
            .bind("p10")
            .to(
                com.google.cloud.spanner.Value.pgJsonb(
                    "{\"key1\": \"value1\", \"key2\": \"value2\"}"))
            .bind("p11")
            .toInt64Array((long[]) null)
            .bind("p12")
            .toBoolArray((boolean[]) null)
            .bind("p13")
            .toBytesArray(null)
            .bind("p14")
            .toFloat64Array((double[]) null)
            .bind("p15")
            .toInt64Array((long[]) null)
            .bind("p16")
            .toPgNumericArray(null)
            .bind("p17")
            .toTimestampArray(null)
            .bind("p18")
            .toDateArray(null)
            .bind("p19")
            .toStringArray(null)
            .bind("p20")
            .toPgJsonbArray(null)
            .build();
    mockSpanner.putStatementResult(
        StatementResult.exception(
            statement,
            Status.ALREADY_EXISTS
                .withDescription("Row with id 1 already exists")
                .asRuntimeException()));

    String actualOutput =
        execute("orm_error_in_read_write_transaction.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "Insert failed: (psycopg.errors.RaiseException) com.google.api.gax.rpc.AlreadyExistsException: io.grpc.StatusRuntimeException: ALREADY_EXISTS: Row with id 1 already exists";
    assertTrue(actualOutput, actualOutput.startsWith(expectedOutput));
    assertFalse(actualOutput, actualOutput.contains("Getting the row after an error succeeded"));

    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest describeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(sql, executeRequest.getSql());
    assertTrue(executeRequest.getTransaction().hasId());

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testErrorInReadWriteTransactionContinue() throws Exception {
    String sql = getInsertAllTypesSql();
    addDescribeInsertAllTypesResult();
    Statement statement =
        Statement.newBuilder(sql)
            .bind("p1")
            .to(1L)
            .bind("p2")
            .to(true)
            .bind("p3")
            .to(ByteArray.copyFrom("test bytes"))
            .bind("p4")
            .to(3.14d)
            .bind("p5")
            .to(100)
            .bind("p6")
            .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
            .bind("p7")
            .to(Timestamp.parseTimestamp("2011-11-04T00:05:23.123456000Z"))
            .bind("p8")
            .to(Date.parseDate("2011-11-04"))
            .bind("p9")
            .to("test string")
            .bind("p10")
            .to(
                com.google.cloud.spanner.Value.pgJsonb(
                    "{\"key1\": \"value1\", \"key2\": \"value2\"}"))
            .bind("p11")
            .toInt64Array((long[]) null)
            .bind("p12")
            .toBoolArray((boolean[]) null)
            .bind("p13")
            .toBytesArray(null)
            .bind("p14")
            .toFloat64Array((double[]) null)
            .bind("p15")
            .toInt64Array((long[]) null)
            .bind("p16")
            .toPgNumericArray(null)
            .bind("p17")
            .toTimestampArray(null)
            .bind("p18")
            .toDateArray(null)
            .bind("p19")
            .toStringArray(null)
            .bind("p20")
            .toPgJsonbArray(null)
            .build();
    mockSpanner.putStatementResult(
        StatementResult.exception(
            statement,
            Status.ALREADY_EXISTS
                .withDescription("Row with id 1 already exists")
                .asRuntimeException()));

    String actualOutput =
        execute("orm_error_in_read_write_transaction_continue.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "Getting the row failed: This Session's transaction has been rolled back due to a previous exception during flush. "
            + "To begin a new transaction with this Session, first issue Session.rollback(). "
            + "Original exception was: (psycopg.errors.RaiseException) com.google.api.gax.rpc.AlreadyExistsException: io.grpc.StatusRuntimeException: ALREADY_EXISTS: Row with id 1 already exists";
    assertTrue(actualOutput, actualOutput.startsWith(expectedOutput));

    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest describeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(sql, executeRequest.getSql());
    assertTrue(executeRequest.getTransaction().hasId());

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertAll() throws Exception {
    String sql = getInsertAllTypesSql();
    addDescribeInsertAllTypesResult();
    mockSpanner.putStatementResult(StatementResult.update(createInsertAllTypesNullValues(1L), 1L));
    mockSpanner.putStatementResult(StatementResult.update(createInsertAllTypesNullValues(2L), 1L));
    mockSpanner.putStatementResult(StatementResult.update(createInsertAllTypesNullValues(3L), 1L));

    String actualOutput = execute("orm_insert_all.py", host, pgServer.getLocalPort());
    String expectedOutput = "Inserted 3 row(s)\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(3, executeRequest.getStatementsCount());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  private static String getInsertAllTypesSql() {
    return "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, col_array_bigint, col_array_bool, col_array_bytea, col_array_float8, col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb) "
        + "VALUES ($1::INTEGER, $2, $3, $4, $5::INTEGER, $6, $7::TIMESTAMP WITH TIME ZONE, $8::DATE, $9::VARCHAR, $10::JSONB, $11::INTEGER[], $12::BOOLEAN[], $13::BYTEA[], $14::FLOAT[], $15::INTEGER[], $16::NUMERIC[], $17::TIMESTAMP WITH TIME ZONE[], $18::DATE[], $19::VARCHAR[], $20::JSONB[])";
  }

  private Statement createInsertAllTypesNullValues(long id) {
    return Statement.newBuilder(getInsertAllTypesSql())
        .bind("p1")
        .to(id)
        .bind("p2")
        .to(UNTYPED_NULL_VALUE)
        .bind("p3")
        .to(UNTYPED_NULL_VALUE)
        .bind("p4")
        .to(UNTYPED_NULL_VALUE)
        .bind("p5")
        .to(UNTYPED_NULL_VALUE)
        .bind("p6")
        .to(UNTYPED_NULL_VALUE)
        .bind("p7")
        .to(UNTYPED_NULL_VALUE)
        .bind("p8")
        .to(UNTYPED_NULL_VALUE)
        .bind("p9")
        .to(UNTYPED_NULL_VALUE)
        .bind("p10")
        .to(com.google.cloud.spanner.Value.pgJsonb("null"))
        .bind("p11")
        .to(UNTYPED_NULL_VALUE)
        .bind("p12")
        .to(UNTYPED_NULL_VALUE)
        .bind("p13")
        .to(UNTYPED_NULL_VALUE)
        .bind("p14")
        .to(UNTYPED_NULL_VALUE)
        .bind("p15")
        .to(UNTYPED_NULL_VALUE)
        .bind("p16")
        .to(UNTYPED_NULL_VALUE)
        .bind("p17")
        .to(UNTYPED_NULL_VALUE)
        .bind("p18")
        .to(UNTYPED_NULL_VALUE)
        .bind("p19")
        .to(UNTYPED_NULL_VALUE)
        .bind("p20")
        .to(UNTYPED_NULL_VALUE)
        .build();
  }

  private void addDescribeInsertAllTypesResult() {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(getInsertAllTypesSql()),
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
                            TypeCode.JSON),
                        true))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
  }
}
