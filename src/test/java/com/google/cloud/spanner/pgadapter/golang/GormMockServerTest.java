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

package com.google.cloud.spanner.pgadapter.golang;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import java.io.IOException;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests PGAdapter using gorm. The Go code can be found in
 * src/test/golang/pgadapter_gorm_tests/gorm.go.
 */
@Category(GolangTest.class)
@RunWith(Parameterized.class)
public class GormMockServerTest extends AbstractMockServerTest {
  private static GormTest gormTest;

  @Parameter public boolean useDomainSocket;

  @Parameters(name = "useDomainSocket = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled() ? new Object[] {true, false} : new Object[] {false};
  }

  @BeforeClass
  public static void compile() throws IOException, InterruptedException {
    gormTest = GolangTest.compile("pgadapter_gorm_tests/gorm.go", GormTest.class);
  }

  private GoString createConnString() {
    if (useDomainSocket) {
      return new GoString(String.format("host=/tmp port=%d", pgServer.getLocalPort()));
    }
    return new GoString(
        String.format("postgres://uid:pwd@localhost:%d/?sslmode=disable", pgServer.getLocalPort()));
  }

  @Test
  public void testFirst() {
    String sql = "SELECT * FROM \"users\" ORDER BY \"users\".\"id\" LIMIT 1";

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("ID")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("Name")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("Email")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("Age")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("Birthday")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("MemberNumber")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("ActivatedAt")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("CreatedAt")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("UpdatedAt")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("Some Name").build())
                        .addValues(Value.newBuilder().setStringValue("user@example.com").build())
                        .addValues(Value.newBuilder().setStringValue("62").build())
                        .addValues(
                            Value.newBuilder()
                                .setStringValue("1960-06-27T16:44:10.123456789Z")
                                .build())
                        .addValues(Value.newBuilder().setStringValue("MN9999").build())
                        .addValues(
                            Value.newBuilder().setStringValue("2021-01-04T10:00:00Z").build())
                        .addValues(
                            Value.newBuilder().setStringValue("2000-01-01T00:00:00Z").build())
                        .addValues(
                            Value.newBuilder().setStringValue("2022-05-22T12:13:14.123Z").build())
                        .build())
                .build()));

    String res = gormTest.TestFirst(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // gorm uses pgx as its default driver for PostgreSQL.
    // pgx by default always uses prepared statements. That means that each request is sent twice
    // to the backend.
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.hasTransaction());
    assertTrue(executeRequest.getTransaction().hasSingleUse());
    assertTrue(executeRequest.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testCreateBlogAndUser() {
    String insertUserSql =
        "INSERT INTO \"users\" (\"id\",\"name\",\"email\",\"age\",\"birthday\",\"member_number\",\"activated_at\",\"created_at\",\"updated_at\") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING \"name_and_number\"";
    String insertBlogSql =
        "INSERT INTO \"blogs\" (\"id\",\"name\",\"description\",\"user_id\",\"created_at\",\"updated_at\") VALUES ($1,$2,$3,$4,$5,$6)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertUserSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to("User Name")
                .bind("p3")
                .to((String) null)
                .bind("p4")
                .to(20L)
                .bind("p5")
                .to((Date) null)
                .bind("p6")
                .to((String) null)
                .bind("p7")
                .to((Timestamp) null)
                .bind("p8")
                .to(Timestamp.parseTimestamp("2022-09-09T12:00:00+01:00"))
                .bind("p9")
                .to(Timestamp.parseTimestamp("2022-09-09T12:00:00+01:00"))
                .build(),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .setName("name_and_number")
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("User Name null").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertUserSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                            ImmutableList.of(
                                TypeCode.INT64,
                                TypeCode.STRING,
                                TypeCode.STRING,
                                TypeCode.INT64,
                                TypeCode.DATE,
                                TypeCode.STRING,
                                TypeCode.TIMESTAMP,
                                TypeCode.TIMESTAMP,
                                TypeCode.TIMESTAMP))
                        .toBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .setName("name_and_number")
                                        .build())
                                .build()))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(insertBlogSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to("My Blog")
                .bind("p3")
                .to((String) null)
                .bind("p4")
                .to(1L)
                .bind("p5")
                .to(Timestamp.parseTimestamp("2022-09-09T12:00:00+01:00"))
                .bind("p6")
                .to(Timestamp.parseTimestamp("2022-09-09T12:00:00+01:00"))
                .build(),
            1L));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertBlogSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.STRING,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.TIMESTAMP,
                            TypeCode.TIMESTAMP)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));

    String res = gormTest.TestCreateBlogAndUser(createConnString());

    assertNull(res);
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);

    assertEquals(insertUserSql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
    assertEquals(insertUserSql, requests.get(1).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(1).getQueryMode());

    assertEquals(insertBlogSql, requests.get(2).getSql());
    assertEquals(QueryMode.PLAN, requests.get(2).getQueryMode());
    assertEquals(insertBlogSql, requests.get(3).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(3).getQueryMode());
  }

  @Test
  public void testQueryAllDataTypes() {
    String sql = "SELECT * FROM \"all_types\" ORDER BY \"all_types\".\"col_varchar\" LIMIT 1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), ALL_TYPES_RESULTSET));

    String res = gormTest.TestQueryAllDataTypes(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements.
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.hasTransaction());
    assertTrue(executeRequest.getTransaction().hasSingleUse());
    assertTrue(executeRequest.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testQueryNullsAllDataTypes() {
    String sql = "SELECT * FROM \"all_types\" ORDER BY \"all_types\".\"col_varchar\" LIMIT 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), ALL_TYPES_NULLS_RESULTSET));

    String res = gormTest.TestQueryNullsAllDataTypes(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements.
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.hasTransaction());
    assertTrue(executeRequest.getTransaction().hasSingleUse());
    assertTrue(executeRequest.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testInsertAllDataTypes() {
    String sql =
        "INSERT INTO \"all_types\" "
            + "(\"col_bigint\",\"col_bool\",\"col_bytea\",\"col_float8\",\"col_int\",\"col_numeric\",\"col_timestamptz\",\"col_date\",\"col_varchar\") "
            + "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)";
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
                            TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(100L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test_bytes"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(1L)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-04-02"))
                .bind("p9")
                .to("test_string")
                .build(),
            1L));

    String res = gormTest.TestInsertAllDataTypes(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent two
    // times to the backend the first time it is executed:
    // 1. DescribeStatement
    // 2. Execute
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.hasTransaction());
    assertTrue(executeRequest.getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(executeRequest.getTransaction().getId(), commitRequest.getTransactionId());
  }

  @Test
  public void testInsertNullsAllDataTypes() {
    String sql =
        "INSERT INTO \"all_types\" "
            + "(\"col_bigint\",\"col_bool\",\"col_bytea\",\"col_float8\",\"col_int\",\"col_numeric\",\"col_timestamptz\",\"col_date\",\"col_varchar\") "
            + "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)";
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
                            TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(100L)
                .bind("p2")
                .to((Boolean) null)
                .bind("p3")
                .to((ByteArray) null)
                .bind("p4")
                .to((Double) null)
                .bind("p5")
                .to((Long) null)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric(null))
                .bind("p7")
                .to((Timestamp) null)
                .bind("p8")
                .to((Date) null)
                .bind("p9")
                .to((String) null)
                .build(),
            1L));

    String res = gormTest.TestInsertNullsAllDataTypes(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent two
    // times to the backend the first time it is executed:
    // 1. DescribeStatement
    // 2. Execute
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testUpdateAllDataTypes() {
    String sql =
        "UPDATE \"all_types\" SET \"col_bigint\"=$1,\"col_bool\"=$2,\"col_bytea\"=$3,\"col_float8\"=$4,\"col_int\"=$5,\"col_numeric\"=$6,\"col_timestamptz\"=$7,\"col_date\"=$8,\"col_varchar\"=$9 WHERE \"col_varchar\" = $10";
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
                            TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(100L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test_bytes"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(1L)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-04-02"))
                .bind("p9")
                .to("test_string")
                .bind("p10")
                .to("test_string")
                .build(),
            1L));

    String res = gormTest.TestUpdateAllDataTypes(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent two
    // times to the backend the first time it is executed:
    // 1. DescribeStatement
    // 2. Execute
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testDelete() {
    String sql = "DELETE FROM \"all_types\" WHERE \"all_types\".\"col_varchar\" = $1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("test_string").build(), 1L));

    String res = gormTest.TestDelete(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent three
    // times to the backend the first time it is executed:
    // 1. DescribeStatement
    // 2. Execute
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testCreateInBatches() {
    String sql =
        "INSERT INTO \"all_types\" (\"col_bigint\",\"col_bool\",\"col_bytea\",\"col_float8\",\"col_int\",\"col_numeric\",\"col_timestamptz\",\"col_date\",\"col_varchar\") "
            + "VALUES "
            + "($1,$2,$3,$4,$5,$6,$7,$8,$9),"
            + "($10,$11,$12,$13,$14,$15,$16,$17,$18),"
            + "($19,$20,$21,$22,$23,$24,$25,$26,$27)";
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
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to((Long) null)
                .bind("p2")
                .to((Boolean) null)
                .bind("p3")
                .to((ByteArray) null)
                .bind("p4")
                .to((Double) null)
                .bind("p5")
                .to((Long) null)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric(null))
                .bind("p7")
                .to((Timestamp) null)
                .bind("p8")
                .to((Date) null)
                .bind("p9")
                .to("1")
                .bind("p10")
                .to((Long) null)
                .bind("p11")
                .to((Boolean) null)
                .bind("p12")
                .to((ByteArray) null)
                .bind("p13")
                .to((Double) null)
                .bind("p14")
                .to((Long) null)
                .bind("p15")
                .to(com.google.cloud.spanner.Value.pgNumeric(null))
                .bind("p16")
                .to((Timestamp) null)
                .bind("p17")
                .to((Date) null)
                .bind("p18")
                .to("2")
                .bind("p19")
                .to((Long) null)
                .bind("p20")
                .to((Boolean) null)
                .bind("p21")
                .to((ByteArray) null)
                .bind("p22")
                .to((Double) null)
                .bind("p23")
                .to((Long) null)
                .bind("p24")
                .to(com.google.cloud.spanner.Value.pgNumeric(null))
                .bind("p25")
                .to((Timestamp) null)
                .bind("p26")
                .to((Date) null)
                .bind("p27")
                .to("3")
                .build(),
            3L));

    String res = gormTest.TestCreateInBatches(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent two
    // times to the backend the first time it is executed:
    // 1. DescribeStatement
    // 2. Execute
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testTransaction() {
    String sql = "INSERT INTO \"all_types\" (\"col_varchar\") VALUES ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("1").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("2").build(), 1L));

    String res = gormTest.TestTransaction(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(3, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest1 = requests.get(1);
    assertEquals(sql, executeRequest1.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest1.getQueryMode());
    assertTrue(executeRequest1.hasTransaction());
    assertTrue(executeRequest1.getTransaction().hasId());

    ExecuteSqlRequest executeRequest2 = requests.get(2);
    assertEquals(sql, executeRequest2.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest2.getQueryMode());
    assertTrue(executeRequest2.hasTransaction());
    assertTrue(executeRequest2.getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(executeRequest2.getTransaction().getId(), commitRequest.getTransactionId());
  }

  @Test
  public void testNestedTransaction() {
    String sql = "INSERT INTO \"all_types\" (\"col_varchar\") VALUES ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("1").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("2").build(), 1L));

    String res = gormTest.TestNestedTransaction(createConnString());
    assertNull(res);
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testErrorInTransaction() {
    String insertSql = "INSERT INTO \"all_types\" (\"col_varchar\") VALUES ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.newBuilder(insertSql).bind("p1").to("1").build(),
            Status.ALREADY_EXISTS.withDescription("Row [1] already exists").asRuntimeException()));
    String updateSql = "UPDATE \"all_types\" SET \"col_int\"=$1 WHERE \"col_varchar\" = $2";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(updateSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(updateSql).bind("p1").to(100L).bind("p2").to("1").build(), 1L));

    String res = gormTest.TestErrorInTransaction(createConnString());
    assertEquals(
        "failed to execute transaction: ERROR: current transaction is aborted, commands ignored until end of transaction block (SQLSTATE 25P02)",
        res);
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testReadOnlyTransaction() {
    String sql = "SELECT * FROM \"all_types\" WHERE \"all_types\".\"col_varchar\" = $1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    ALL_TYPES_METADATA.toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to("1").build(), ALL_TYPES_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to("2").build(), ALL_TYPES_RESULTSET));

    String res = gormTest.TestReadOnlyTransaction(createConnString());

    assertNull(res);
    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    BeginTransactionRequest beginRequest =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).get(0);
    assertTrue(beginRequest.getOptions().hasReadOnly());

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(3, requests.size());

    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertTrue(describeRequest.getTransaction().hasId());

    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(describeRequest.getTransaction().getId(), executeRequest.getTransaction().getId());

    // The read-only transaction is 'committed', but that does not cause a CommitRequest to be sent
    // to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }
}
