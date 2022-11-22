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
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.TypeCode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import org.junit.BeforeClass;
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
    return ImmutableList.of(
        new Object[] {"localhost"}
        //        , new Object[] {"/tmp"}
        );
  }

  static String execute(String script, String host, int port)
      throws IOException, InterruptedException {
    String[] runCommand = new String[] {"python3", script, host, Integer.toString(port)};
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(runCommand);
    builder.directory(new File("./src/test/python/sqlalchemy"));
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
        "with pg_namespace as (\n"
            + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
            + "        schema_name as nspname, null as nspowner, null as nspacl\n"
            + "  from information_schema.schemata\n"
            + "),\n"
            + "pg_type as (\n"
            + "  select 16 as oid, 'bool' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 1 as typlen, true as typbyval, 'b' as typtype, 'B' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1000 as typarray, 'boolin' as typinput, 'boolout' as typoutput, 'boolrecv' as typreceive, 'boolsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'c' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 17 as oid, 'bytea' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1001 as typarray, 'byteain' as typinput, 'byteaout' as typoutput, 'bytearecv' as typreceive, 'byteasend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 20 as oid, 'int8' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1016 as typarray, 'int8in' as typinput, 'int8out' as typoutput, 'int8recv' as typreceive, 'int8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 21 as oid, 'int2' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 2 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1005 as typarray, 'int2in' as typinput, 'int2out' as typoutput, 'int2recv' as typreceive, 'int2send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 's' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 23 as oid, 'int4' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1007 as typarray, 'int4in' as typinput, 'int4out' as typoutput, 'int4recv' as typreceive, 'int4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 25 as oid, 'text' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1009 as typarray, 'textin' as typinput, 'textout' as typoutput, 'textrecv' as typreceive, 'textsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 700 as oid, 'float4' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1021 as typarray, 'float4in' as typinput, 'float4out' as typoutput, 'float4recv' as typreceive, 'float4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 701 as oid, 'float8' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1022 as typarray, 'float8in' as typinput, 'float8out' as typoutput, 'float8recv' as typreceive, 'float8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1043 as oid, 'varchar' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1015 as typarray, 'varcharin' as typinput, 'varcharout' as typoutput, 'varcharrecv' as typreceive, 'varcharsend' as typsend, 'varchartypmodin' as typmodin, 'varchartypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1082 as oid, 'date' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1182 as typarray, 'date_in' as typinput, 'date_out' as typoutput, 'date_recv' as typreceive, 'date_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1114 as oid, 'timestamp' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1115 as typarray, 'timestamp_in' as typinput, 'timestamp_out' as typoutput, 'timestamp_recv' as typreceive, 'timestamp_send' as typsend, 'timestamptypmodin' as typmodin, 'timestamptypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1184 as oid, 'timestamptz' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1185 as typarray, 'timestamptz_in' as typinput, 'timestamptz_out' as typoutput, 'timestamptz_recv' as typreceive, 'timestamptz_send' as typsend, 'timestamptztypmodin' as typmodin, 'timestamptztypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1700 as oid, 'numeric' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1231 as typarray, 'numeric_in' as typinput, 'numeric_out' as typoutput, 'numeric_recv' as typreceive, 'numeric_send' as typsend, 'numerictypmodin' as typmodin, 'numerictypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'm' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 3802 as oid, 'jsonb' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 3807 as typarray, 'jsonb_in' as typinput, 'jsonb_out' as typoutput, 'jsonb_recv' as typreceive, 'jsonb_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl\n"
            + ")\n"
            + "SELECT t.oid, typarray\n"
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
}
