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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.RandomResultSetGenerator;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.cloud.spanner.pgadapter.python.PythonTestUtil;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.TypeCode;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
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
public class SqlAlchemy2BasicsTest extends AbstractMockServerTest {
  static final String DIRECTORY_NAME = "./src/test/python/sqlalchemy2";

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {""});
  }

  static String execute(String script, String host, int port) throws Exception {
    return execute(DIRECTORY_NAME, script, host, port, null);
  }

  static String execute(String directoryName, String script, String host, int port)
      throws Exception {
    return execute(directoryName, script, host, port, null);
  }

  static String execute(String directoryName, String script, String host, int port, String database)
      throws Exception {
    File directory = new File(directoryName);
    return PythonTestUtil.run(
        new String[] {
          directory.getAbsolutePath() + "/venv/bin/python",
          script,
          host,
          Integer.toString(port),
          database == null ? "d" : database
        },
        directoryName);
  }

  @BeforeClass
  public static void setupBaseResults() throws Exception {
    setupBaseResults(DIRECTORY_NAME);
  }

  public static void setupBaseResults(String directory) throws Exception {
    PythonTestUtil.createVirtualEnv(directory);
    String selectHstoreType =
        "with "
            + PG_TYPE_PREFIX
            + "\nSELECT\n"
            + "    typname AS name, oid, typarray AS array_oid,\n"
            + "    '' as regtype, typdelim AS delimiter\n"
            + "FROM pg_type t\n"
            + "WHERE t.typname = $1\n"
            + "ORDER BY t.oid\n";
    ResultSet hstoreResultSet =
        ResultSet.newBuilder()
            .setMetadata(
                createMetadata(
                        ImmutableList.of(
                            TypeCode.STRING, TypeCode.INT64, TypeCode.STRING, TypeCode.STRING))
                    .toBuilder()
                    .setUndeclaredParameters(
                        createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                            .getUndeclaredParameters()))
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(selectHstoreType), hstoreResultSet));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectHstoreType).bind("p1").to("hstore").build(),
            hstoreResultSet));
  }

  @Test
  public void testHelloWorld() throws Exception {
    String sql = "select 'hello world'";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("hello world").build())
                        .build())
                .build()));

    String actualOutput = execute("hello_world.py", host, pgServer.getLocalPort());
    String expectedOutput = "[('hello world',)]\n[('sqlalchemy2',)]\n";
    assertEquals(expectedOutput, actualOutput);

    // The 'get hstore type' statement is auto-described and then executed, hence 3 requests.
    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testSimpleInsert() throws Exception {
    String sql = "INSERT INTO test VALUES ($1, $2)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql).bind("p1").to(1L).bind("p2").to("One").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql).bind("p1").to(2L).bind("p2").to("Two").build(), 1L));

    String actualOutput = execute("simple_insert.py", host, pgServer.getLocalPort());
    String expectedOutput = "Row count: 2\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest describeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertTrue(request.getTransaction().hasId());
    assertEquals(sql, request.getStatements(0).getSql());
    assertEquals(sql, request.getStatements(1).getSql());
    assertEquals(
        "1", request.getStatements(0).getParams().getFieldsMap().get("p1").getStringValue());
    assertEquals(
        "One", request.getStatements(0).getParams().getFieldsMap().get("p2").getStringValue());
    assertEquals(
        "2", request.getStatements(1).getParams().getFieldsMap().get("p1").getStringValue());
    assertEquals(
        "Two", request.getStatements(1).getParams().getFieldsMap().get("p2").getStringValue());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testEngineBegin() throws Exception {
    String sql = "INSERT INTO test VALUES ($1, $2)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql).bind("p1").to(3L).bind("p2").to("Three").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql).bind("p1").to(4L).bind("p2").to("Four").build(), 1L));

    String actualOutput = execute("engine_begin.py", host, pgServer.getLocalPort());
    String expectedOutput = "Row count: 2\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest describeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertTrue(request.getTransaction().hasId());
    assertEquals(sql, request.getStatements(0).getSql());
    assertEquals(sql, request.getStatements(1).getSql());
    assertEquals(
        "3", request.getStatements(0).getParams().getFieldsMap().get("p1").getStringValue());
    assertEquals(
        "Three", request.getStatements(0).getParams().getFieldsMap().get("p2").getStringValue());
    assertEquals(
        "4", request.getStatements(1).getParams().getFieldsMap().get("p1").getStringValue());
    assertEquals(
        "Four", request.getStatements(1).getParams().getFieldsMap().get("p2").getStringValue());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testSessionExecute() throws Exception {
    String sql = "UPDATE test SET value=$1 WHERE id=$2";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql).bind("p1").to("one").bind("p2").to(1L).build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql).bind("p1").to("two").bind("p2").to(2L).build(), 1L));

    String actualOutput = execute("session_execute.py", host, pgServer.getLocalPort());
    String expectedOutput = "Row count: 2\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest describeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertTrue(request.getTransaction().hasId());
    assertEquals(sql, request.getStatements(0).getSql());
    assertEquals(sql, request.getStatements(1).getSql());
    assertEquals(
        "one", request.getStatements(0).getParams().getFieldsMap().get("p1").getStringValue());
    assertEquals(
        "1", request.getStatements(0).getParams().getFieldsMap().get("p2").getStringValue());
    assertEquals(
        "two", request.getStatements(1).getParams().getFieldsMap().get("p1").getStringValue());
    assertEquals(
        "2", request.getStatements(1).getParams().getFieldsMap().get("p2").getStringValue());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Ignore("Requires pg_catalog re-writes")
  @Test
  public void testSimpleMetadata() throws Exception {
    String checkTableExistsSql =
        "with "
            + EMULATED_PG_CLASS_PREFIX
            + ",\n"
            + "pg_namespace as (\n"
            + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
            + "        schema_name as nspname, null as nspowner, null as nspacl\n"
            + "  from information_schema.schemata\n"
            + ")\n"
            + "select relname from pg_class c join pg_namespace n on n.oid=c.relnamespace where true and relname='%s'";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(String.format(checkTableExistsSql, "user_account")),
            ResultSet.newBuilder().setMetadata(SELECT1_RESULTSET.getMetadata()).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(String.format(checkTableExistsSql, "address")),
            ResultSet.newBuilder().setMetadata(SELECT1_RESULTSET.getMetadata()).build()));
    addDdlResponseToSpannerAdmin();

    String actualOutput = execute("simple_metadata.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "user_account.name\n"
            + "['id', 'name', 'fullname']\n"
            + "PrimaryKeyConstraint(Column('id', Integer(), table=<user_account>, primary_key=True, nullable=False))\n"
            + "user_account\n"
            + "address\n";
    assertEquals(expectedOutput, actualOutput);

    List<UpdateDatabaseDdlRequest> requests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(req -> req instanceof UpdateDatabaseDdlRequest)
            .map(req -> (UpdateDatabaseDdlRequest) req)
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    assertEquals(2, requests.get(0).getStatementsCount());
    assertEquals(
        "CREATE TABLE user_account (\n"
            + "\tid SERIAL NOT NULL, \n"
            + "\tname VARCHAR(30), \n"
            + "\tfullname VARCHAR, \n"
            + "\tPRIMARY KEY (id)\n"
            + ")",
        requests.get(0).getStatements(0));
    assertEquals(
        "CREATE TABLE address (\n"
            + "\tid SERIAL NOT NULL, \n"
            + "\temail_address VARCHAR NOT NULL, \n"
            + "\tuser_id INTEGER, \n"
            + "\tPRIMARY KEY (id), \n"
            + "\tFOREIGN KEY(user_id) REFERENCES user_account (id)\n"
            + ")",
        requests.get(0).getStatements(1));
  }

  @Test
  public void testCoreInsert() throws Exception {
    String sql =
        "INSERT INTO user_account (name, fullname) "
            + "VALUES ($1::VARCHAR(30), $2::VARCHAR) RETURNING user_account.id";
    ResultSet metadataResultSet =
        ResultSet.newBuilder()
            .setMetadata(
                SELECT1_RESULTSET.getMetadata().toBuilder()
                    .setUndeclaredParameters(
                        createParameterTypesMetadata(
                                ImmutableList.of(TypeCode.STRING, TypeCode.STRING))
                            .getUndeclaredParameters())
                    .build())
            .setStats(ResultSetStats.newBuilder().build())
            .build();
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), metadataResultSet));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql)
                .bind("p1")
                .to("spongebob")
                .bind("p2")
                .to("Spongebob Squarepants")
                .build(),
            metadataResultSet.toBuilder()
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addAllRows(SELECT1_RESULTSET.getRowsList())
                .build()));
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    String sqlMultiple =
        "INSERT INTO user_account (name, fullname) VALUES ($1::VARCHAR(30), $2::VARCHAR)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sqlMultiple),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(TypeCode.STRING, TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sqlMultiple)
                .bind("p1")
                .to("sandy")
                .bind("p2")
                .to("Sandy Cheeks")
                .build(),
            1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sqlMultiple)
                .bind("p1")
                .to("patrick")
                .bind("p2")
                .to("Patrick Star")
                .build(),
            1L));

    String actualOutput = execute("core_insert.py", host, pgServer.getLocalPort());
    // TODO: Verify that the returned rowcount does not cause problems with versioning.
    String expectedOutput =
        "INSERT INTO user_account (name, fullname) VALUES (:name, :fullname)\n"
            + "{'name': 'spongebob', 'fullname': 'Spongebob Squarepants'}\n"
            + "Result: []\n"
            + "Row count: -1\n"
            + "Inserted primary key: (1,)\n"
            + "Row count: -1\n";
    assertEquals(expectedOutput, actualOutput);

    // 5 requests:
    // 1. 2 system calls for getting hstore type information.
    // 2. 2 'DESCRIBE' calls.
    // 3. 1 'EXECUTE' call.
    assertEquals(5, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest describeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.getTransaction().hasId());
    ExecuteSqlRequest multipleRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(4);
    assertEquals(QueryMode.PLAN, multipleRequest.getQueryMode());
    assertTrue(multipleRequest.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest batchRequest =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertTrue(batchRequest.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(
        batchRequest.getTransaction().getId(),
        mockSpanner.getRequestsOfType(CommitRequest.class).get(0).getTransactionId());
    assertEquals(
        multipleRequest.getTransaction().getId(),
        mockSpanner.getRequestsOfType(CommitRequest.class).get(0).getTransactionId());
    assertEquals(
        executeRequest.getTransaction().getId(),
        mockSpanner.getRequestsOfType(CommitRequest.class).get(0).getTransactionId());
  }

  @Test
  public void testCoreInsertFromSelect() throws Exception {
    String sql =
        "INSERT INTO address (user_id, email_address) "
            + "SELECT user_account.id, user_account.name || $1::VARCHAR(30) AS anon_1 \n"
            + "FROM user_account RETURNING address.id, address.email_address";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)).toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                                .getUndeclaredParameters()))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to("@aol.com").build(),
            ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().setRowCountExact(2L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("test1@aol.com"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .addValues(Value.newBuilder().setStringValue("test2@aol.com"))
                        .build())
                .build()));

    String actualOutput = execute("core_insert_from_select.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "INSERT INTO address (user_id, email_address) "
            + "SELECT user_account.id, user_account.name || :name_1 AS anon_1 \n"
            + "FROM user_account\n"
            + "Inserted rows: 2\n"
            + "Returned rows: [(1, 'test1@aol.com'), (2, 'test2@aol.com')]\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testCoreSelect() throws Exception {
    String sql =
        "SELECT user_account.id, user_account.name, user_account.fullname \n"
            + "FROM user_account \n"
            + "WHERE user_account.name = $1::VARCHAR(30)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                            ImmutableList.of(TypeCode.INT64, TypeCode.STRING, TypeCode.STRING))
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                                .getUndeclaredParameters()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to("spongebob").build(),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("test1@aol.com"))
                        .addValues(Value.newBuilder().setStringValue("Bob Test1"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .addValues(Value.newBuilder().setStringValue("test2@aol.com"))
                        .addValues(Value.newBuilder().setStringValue("Bob Test2"))
                        .build())
                .build()));

    String actualOutput = execute("core_select.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "SELECT user_account.id, user_account.name, user_account.fullname \n"
            + "FROM user_account \n"
            + "WHERE user_account.name = :name_1\n"
            + "(1, 'test1@aol.com', 'Bob Test1')\n"
            + "(2, 'test2@aol.com', 'Bob Test2')\n";
    assertEquals(expectedOutput, actualOutput);

    // 2 round-trips for getting hstore type information.
    // 2 round-trips for DESCRIBE and EXECUTE.
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    // Both steps use a read/write transaction by default.
    assertEquals(2, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testAutoCommit() throws Exception {
    String sql =
        "SELECT user_account.id, user_account.name, user_account.fullname \n"
            + "FROM user_account \n"
            + "WHERE user_account.name = $1::VARCHAR(30)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                            ImmutableList.of(TypeCode.INT64, TypeCode.STRING, TypeCode.STRING))
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                                .getUndeclaredParameters()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to("spongebob").build(),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("test1@aol.com"))
                        .addValues(Value.newBuilder().setStringValue("Bob Test1"))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .addValues(Value.newBuilder().setStringValue("test2@aol.com"))
                        .addValues(Value.newBuilder().setStringValue("Bob Test2"))
                        .build())
                .build()));
    String insertSql =
        "INSERT INTO user_account (name, fullname) "
            + "VALUES ($1::VARCHAR(30), $2::VARCHAR) RETURNING user_account.id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(
                    SELECT1_RESULTSET.getMetadata().toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(TypeCode.STRING, TypeCode.STRING))
                                .getUndeclaredParameters()))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to("sandy")
                .bind("p2")
                .to("Sandy Cheeks")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(SELECT1_RESULTSET.getMetadata())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(SELECT1_RESULTSET.getRows(0))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to("patrick")
                .bind("p2")
                .to("Patrick Star")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(SELECT2_RESULTSET.getMetadata())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(SELECT2_RESULTSET.getRows(0))
                .build()));

    String actualOutput = execute("autocommit.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "SERIALIZABLE\n"
            + "(1, 'test1@aol.com', 'Bob Test1')\n"
            + "(2, 'test2@aol.com', 'Bob Test2')\n"
            + "Row count: -1\n"
            + "Row count: -1\n";
    assertEquals(expectedOutput, actualOutput);

    // 2 system queries for hstore.
    // 2 for SELECT (DESCRIBE + EXECUTE)
    // 1 for DESCRIBE INSERT.
    // 2 for EXECUTE INSERT.
    assertEquals(7, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest describeSelectRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(QueryMode.PLAN, describeSelectRequest.getQueryMode());
    assertTrue(describeSelectRequest.getTransaction().hasSingleUse());
    ExecuteSqlRequest selectRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(QueryMode.NORMAL, selectRequest.getQueryMode());
    assertTrue(selectRequest.getTransaction().hasSingleUse());

    ExecuteSqlRequest describeInsertRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(4);
    assertEquals(QueryMode.PLAN, describeInsertRequest.getQueryMode());
    assertTrue(describeInsertRequest.getTransaction().hasBegin());
    assertTrue(describeInsertRequest.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest insertRequest1 =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(5);
    assertEquals(QueryMode.NORMAL, insertRequest1.getQueryMode());
    assertTrue(insertRequest1.getTransaction().hasBegin());
    assertTrue(insertRequest1.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest insertRequest2 =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(6);
    assertEquals(QueryMode.NORMAL, insertRequest1.getQueryMode());
    assertTrue(insertRequest2.getTransaction().hasBegin());
    assertTrue(insertRequest2.getTransaction().getBegin().hasReadWrite());

    // Both the DESCRIBE and two EXECUTE requests are committed.
    assertEquals(3, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Ignore("requires DECLARE support, https://github.com/GoogleCloudPlatform/pgadapter/issues/510")
  @Test
  public void testServerSideCursors() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from random"), new RandomResultSetGenerator(100).generate()));

    String actualOutput = execute("server_side_cursor.py", host, pgServer.getLocalPort());
    String expectedOutput = "";
    assertEquals(expectedOutput, actualOutput);
  }
}
