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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.RandomResultSetGenerator;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.TypeCode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;
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
public class SqlAlchemyBasicsTest extends AbstractMockServerTest {

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {""});
  }

  static String execute(String script, String host, int port)
      throws IOException, InterruptedException {
    return execute("./src/test/python/sqlalchemy", script, host, port);
  }

  static String execute(String directory, String script, String host, int port)
      throws IOException, InterruptedException {
    return execute(directory, script, host, port, null);
  }

  static String execute(String directory, String script, String host, int port, String database)
      throws IOException, InterruptedException {
    String[] runCommand =
        new String[] {
          "python3", script, host, Integer.toString(port), database == null ? "d" : database
        };
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(runCommand);
    builder.directory(new File(directory));
    Process process = builder.start();
    Scanner scanner = new Scanner(process.getInputStream());
    Scanner errorScanner = new Scanner(process.getErrorStream());

    StringBuilder output = new StringBuilder();
    while (scanner.hasNextLine()) {
      output.append(scanner.nextLine()).append("\n");
    }
    StringBuilder error = new StringBuilder();
    while (errorScanner.hasNextLine()) {
      error.append(errorScanner.nextLine()).append("\n");
    }
    int result = process.waitFor();
    assertEquals(error.toString(), 0, result);

    return output.toString();
  }

  @BeforeClass
  public static void setupBaseResults() {
    String selectHstoreType =
        "with "
            + PG_TYPE_PREFIX
            + "\nSELECT t.oid, typarray\n"
            + "FROM pg_type t JOIN pg_namespace ns\n"
            + "    ON typnamespace = ns.oid\n"
            + "WHERE typname = 'hstore'";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectHstoreType),
            ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.INT64)))
                .build()));
  }

  @Test
  public void testHelloWorld() throws IOException, InterruptedException {
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
    String expectedOutput = "[('hello world',)]\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testSimpleInsert() throws IOException, InterruptedException {
    String sql1 = "INSERT INTO test VALUES (1, 'One')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql1), 1L));
    String sql2 = "INSERT INTO test VALUES (2, 'Two')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1L));

    String actualOutput = execute("simple_insert.py", host, pgServer.getLocalPort());
    String expectedOutput = "Row count: 2\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request1 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql1, request1.getSql());
    assertTrue(request1.getTransaction().hasBegin());
    assertTrue(request1.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest request2 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql2, request2.getSql());
    assertTrue(request2.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testEngineBegin() throws IOException, InterruptedException {
    String sql1 = "INSERT INTO test VALUES (3, 'Three')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql1), 1L));
    String sql2 = "INSERT INTO test VALUES (4, 'Four')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1L));

    String actualOutput = execute("engine_begin.py", host, pgServer.getLocalPort());
    String expectedOutput = "Row count: 2\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request1 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql1, request1.getSql());
    assertTrue(request1.getTransaction().hasBegin());
    assertTrue(request1.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest request2 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql2, request2.getSql());
    assertTrue(request2.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testSessionExecute() throws IOException, InterruptedException {
    String sql1 = "UPDATE test SET value='one' WHERE id=1";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql1), 1L));
    String sql2 = "UPDATE test SET value='two' WHERE id=2";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1L));

    String actualOutput = execute("session_execute.py", host, pgServer.getLocalPort());
    String expectedOutput = "Row count: 2\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request1 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql1, request1.getSql());
    assertTrue(request1.getTransaction().hasBegin());
    assertTrue(request1.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest request2 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(sql2, request2.getSql());
    assertTrue(request2.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testSimpleMetadata() throws Exception {
    String checkTableExistsSql =
        "with pg_class as (\n"
            + "  select\n"
            + "  -1 as oid,\n"
            + "  table_name as relname,\n"
            + "  case table_schema when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as relnamespace,\n"
            + "  0 as reltype,\n"
            + "  0 as reloftype,\n"
            + "  0 as relowner,\n"
            + "  1 as relam,\n"
            + "  0 as relfilenode,\n"
            + "  0 as reltablespace,\n"
            + "  0 as relpages,\n"
            + "  0.0::float8 as reltuples,\n"
            + "  0 as relallvisible,\n"
            + "  0 as reltoastrelid,\n"
            + "  false as relhasindex,\n"
            + "  false as relisshared,\n"
            + "  'p' as relpersistence,\n"
            + "  'r' as relkind,\n"
            + "  count(*) as relnatts,\n"
            + "  0 as relchecks,\n"
            + "  false as relhasrules,\n"
            + "  false as relhastriggers,\n"
            + "  false as relhassubclass,\n"
            + "  false as relrowsecurity,\n"
            + "  false as relforcerowsecurity,\n"
            + "  true as relispopulated,\n"
            + "  'n' as relreplident,\n"
            + "  false as relispartition,\n"
            + "  0 as relrewrite,\n"
            + "  0 as relfrozenxid,\n"
            + "  0 as relminmxid,\n"
            + "  '{}'::bigint[] as relacl,\n"
            + "  '{}'::text[] as reloptions,\n"
            + "  0 as relpartbound\n"
            + "from information_schema.tables t\n"
            + "inner join information_schema.columns using (table_catalog, table_schema, table_name)\n"
            + "group by t.table_name, t.table_schema\n"
            + "union all\n"
            + "select\n"
            + "    -1 as oid,\n"
            + "    i.index_name as relname,\n"
            + "    case table_schema when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as relnamespace,\n"
            + "    0 as reltype,\n"
            + "    0 as reloftype,\n"
            + "    0 as relowner,\n"
            + "    1 as relam,\n"
            + "    0 as relfilenode,\n"
            + "    0 as reltablespace,\n"
            + "    0 as relpages,\n"
            + "    0.0::float8 as reltuples,\n"
            + "    0 as relallvisible,\n"
            + "    0 as reltoastrelid,\n"
            + "    false as relhasindex,\n"
            + "    false as relisshared,\n"
            + "    'p' as relpersistence,\n"
            + "    'r' as relkind,\n"
            + "    count(*) as relnatts,\n"
            + "    0 as relchecks,\n"
            + "    false as relhasrules,\n"
            + "    false as relhastriggers,\n"
            + "    false as relhassubclass,\n"
            + "    false as relrowsecurity,\n"
            + "    false as relforcerowsecurity,\n"
            + "    true as relispopulated,\n"
            + "    'n' as relreplident,\n"
            + "    false as relispartition,\n"
            + "    0 as relrewrite,\n"
            + "    0 as relfrozenxid,\n"
            + "    0 as relminmxid,\n"
            + "    '{}'::bigint[] as relacl,\n"
            + "    '{}'::text[] as reloptions,\n"
            + "    0 as relpartbound\n"
            + "from information_schema.indexes i\n"
            + "inner join information_schema.index_columns using (table_catalog, table_schema, table_name)\n"
            + "group by i.index_name, i.table_schema\n"
            + "),\n"
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
  public void testCoreInsert() throws IOException, InterruptedException {
    String sql =
        "INSERT INTO user_account (name, fullname) VALUES "
            + "('spongebob', 'Spongebob Squarepants') RETURNING user_account.id";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));
    String sqlMultiple =
        "INSERT INTO user_account (name, fullname) VALUES "
            + "('sandy', 'Sandy Cheeks'),('patrick', 'Patrick Star')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sqlMultiple), 2L));

    String actualOutput = execute("core_insert.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "INSERT INTO user_account (name, fullname) VALUES (:name, :fullname)\n"
            + "{'name': 'spongebob', 'fullname': 'Spongebob Squarepants'}\n"
            + "Result: []\n"
            + "Row count: 1\n"
            + "Inserted primary key: (1,)\n"
            + "Row count: 2\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest multipleRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertTrue(multipleRequest.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testCoreInsertFromSelect() throws IOException, InterruptedException {
    String sql =
        "INSERT INTO address (user_id, email_address) "
            + "SELECT user_account.id, user_account.name || '@aol.com' AS anon_1 \n"
            + "FROM user_account RETURNING address.id, address.email_address";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
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
  public void testCoreSelect() throws IOException, InterruptedException {
    String sql =
        "SELECT user_account.id, user_account.name, user_account.fullname \n"
            + "FROM user_account \n"
            + "WHERE user_account.name = 'spongebob'";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING, TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().setRowCountExact(2L).build())
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

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(2, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testAutoCommit() throws IOException, InterruptedException {
    String sql =
        "SELECT user_account.id, user_account.name, user_account.fullname \n"
            + "FROM user_account \n"
            + "WHERE user_account.name = 'spongebob'";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
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
    String insertSql1 =
        "INSERT INTO user_account (name, fullname) VALUES "
            + "('sandy', 'Sandy Cheeks') RETURNING user_account.id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql1),
            ResultSet.newBuilder()
                .setMetadata(SELECT1_RESULTSET.getMetadata())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(SELECT1_RESULTSET.getRows(0))
                .build()));
    String insertSql2 =
        "INSERT INTO user_account (name, fullname) VALUES "
            + "('patrick', 'Patrick Star') RETURNING user_account.id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql2),
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
            + "Row count: 1\n"
            + "Row count: 1\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest selectRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertTrue(selectRequest.getTransaction().hasSingleUse());
    assertTrue(selectRequest.getTransaction().getSingleUse().hasReadOnly());

    ExecuteSqlRequest insertRequest1 =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertTrue(insertRequest1.getTransaction().hasBegin());
    assertTrue(insertRequest1.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest insertRequest2 =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertTrue(insertRequest2.getTransaction().hasBegin());
    assertTrue(insertRequest2.getTransaction().getBegin().hasReadWrite());

    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Ignore("requires DECLARE support, https://github.com/GoogleCloudPlatform/pgadapter/issues/510")
  @Test
  public void testServerSideCursors() throws IOException, InterruptedException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from random"), new RandomResultSetGenerator(100).generate()));

    String actualOutput = execute("server_side_cursor.py", host, pgServer.getLocalPort());
    String expectedOutput = "";
    assertEquals(expectedOutput, actualOutput);
  }
}
