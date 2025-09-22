// Copyright 2024 Google LLC
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

package com.google.cloud.spanner.pgadapter.sample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.DriverManager;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;

public class SampleApplicationTest extends AbstractMockServerTest {
  private static final int RANDOM_RESULTS_ROW_COUNT = 10;
  private static final Statement SELECT_RANDOM = Statement.of("select * from random_table");

  @BeforeClass
  public static void setupServer() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    mockSpanner.setAbortProbability(0.005);
    mockSpanner.putStatementResult(StatementResult.detectDialectResult(Dialect.POSTGRESQL));
    mockSpanner.putStatementResult(StatementResult.query(Statement.of("select 'abort,access,aggregate,also,analyse,analyze,backward,bit,cache,checkpoint,class,cluster,comment,concurrently,connection,conversion,copy,csv,database,delimiter,delimiters,disable,do,enable,encoding,encrypted,exclusive,explain,force,forward,freeze,greatest,handler,header,if,ilike,immutable,implicit,index,indexes,inherit,inherits,instead,isnull,least,limit,listen,load,location,lock,mode,move,nothing,notify,notnull,nowait,off,offset,oids,operator,owned,owner,password,prepared,procedural,quote,reassign,recheck,reindex,rename,replace,reset,restrict,returning,rule,setof,share,show,stable,statistics,stdin,stdout,storage,strict,sysid,tablespace,temp,template,truncate,trusted,unencrypted,unlisten,until,vacuum,valid,validator,verbose,volatile'"), ResultSet.newBuilder()
            .setMetadata(ResultSetMetadata.newBuilder()
                .setRowType(StructType.newBuilder()
                    .addFields(Field.newBuilder().setName("c").setType(Type.newBuilder().setCode(
                        TypeCode.STRING).build()).build())
                    .build())
                .build())
            .addRows(ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("abort,access,aggregate,also,analyse,analyze,backward,bit,cache,checkpoint,class,cluster,comment,concurrently,connection,conversion,copy,csv,database,delimiter,delimiters,disable,do,enable,encoding,encrypted,exclusive,explain,force,forward,freeze,greatest,handler,header,if,ilike,immutable,implicit,index,indexes,inherit,inherits,instead,isnull,least,limit,listen,load,location,lock,mode,move,nothing,notify,notnull,nowait,off,offset,oids,operator,owned,owner,password,prepared,procedural,quote,reassign,recheck,reindex,rename,replace,reset,restrict,returning,rule,setof,share,show,stable,statistics,stdin,stdout,storage,strict,sysid,tablespace,temp,template,truncate,trusted,unencrypted,unlisten,until,vacuum,valid,validator,verbose,volatile").build()).build())
        .build()));
    mockSpanner.putPartialStatementResult(StatementResult.update(Statement.of("insert into singers (active,created_at,first_name,last_name,updated_at,id) values ($1,$2,$3,$4,$5,$6)"), 1L));

    addRandomResultResults();
    setupJsonbResults();
    setupIntervalResults();
    setupSettingsResults();
  }

  @Test
  public void testRunApplication() {
    System.setProperty("spanner.endpoint", String.format("localhost:%d", spannerServer.getPort()));
    mockSpanner.setExecuteBatchDmlExecutionTime(SimulatedExecutionTime.ofMinimumAndRandomTime(100, 200));
    mockSpanner.setCommitExecutionTime(SimulatedExecutionTime.ofMinimumAndRandomTime(50, 100));

//    ByteArrayOutputStream outArrayStream = new ByteArrayOutputStream();
//    PrintStream out = new PrintStream(outArrayStream);
//    PrintStream originalOut = System.out;
//    System.setOut(out);
//    try {
//      SampleApplication.main(new String[] {});
//      String output = outArrayStream.toString();
//      assertTrue(
//          output, output.contains("Created singers"));
//      assertEquals(50, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
//    } finally {
//      System.setOut(originalOut);
//    }

    SampleApplication.main(new String[] {});
  }

  private static void setupSettingsResults() {
    String sql = "with pg_settings_inmem_ as (\n"
        + "select t->>'name' as name, t->>'setting' as setting, t->>'unit' as unit, t->>'category' as category, t->>'short_desc' as short_desc, t->>'extra_desc' as extra_desc, t->>'context' as context, t->>'vartype' as vartype, t->>'min_val' as min_val, t->>'max_val' as max_val, case when t->>'enumvals' is null then null::text[] else spanner.string_array((t->>'enumvals')::jsonb) end as enumvals, t->>'boot_val' as boot_val, t->>'reset_val' as reset_val, t->>'source' as source, (t->>'sourcefile')::varchar as sourcefile, (t->>'sourceline')::bigint as sourceline, (t->>'pending_restart')::boolean as pending_restart\n"
        + "from unnest(array[\n"
        + "'{\"name\":\"DateStyle\",\"setting\":\"ISO\",\"unit\":null,\"category\":\"Client Connection Defaults / Locale and Formatting\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"ISO, MDY\",\"reset_val\":\"ISO\",\"source\":\"configuration file\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"TimeZone\",\"setting\":\"Europe/Oslo\",\"unit\":null,\"category\":\"Client Connection Defaults / Locale and Formatting\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"GMT\",\"reset_val\":\"Europe/Oslo\",\"source\":\"configuration file\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"application_name\",\"setting\":\"PostgreSQL JDBC Driver\",\"unit\":null,\"category\":\"Reporting and Logging / What to Log\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"\",\"reset_val\":null,\"source\":\"client\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"bytea_output\",\"setting\":\"hex\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"enum\",\"min_val\":null,\"max_val\":null,\"enum_vals\":[\"escape\", \"hex\"],\"boot_val\":\"hex\",\"reset_val\":\"hex\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"default_transaction_isolation\",\"setting\":\"serializable\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"enum\",\"min_val\":null,\"max_val\":null,\"enum_vals\":[\"serializable\", \"repeatable read\", \"read committed\", \"read uncommitted\"],\"boot_val\":\"serializable\",\"reset_val\":\"serializable\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"default_transaction_read_only\",\"setting\":\"off\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"off\",\"reset_val\":\"off\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"extra_float_digits\",\"setting\":\"1\",\"unit\":null,\"category\":\"Client Connection Defaults / Locale and Formatting\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"integer\",\"min_val\":\"-15\",\"max_val\":\"3\",\"enum_vals\":null,\"boot_val\":\"1\",\"reset_val\":\"1\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"max_connections\",\"setting\":\"100\",\"unit\":null,\"category\":\"Connections and Authentication / Connection Settings\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"postmaster\",\"vartype\":\"integer\",\"min_val\":\"1\",\"max_val\":\"262143\",\"enum_vals\":null,\"boot_val\":\"100\",\"reset_val\":\"100\",\"source\":\"configuration file\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"max_index_keys\",\"setting\":\"16\",\"unit\":null,\"category\":\"Preset Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"internal\",\"vartype\":\"integer\",\"min_val\":\"16\",\"max_val\":\"16\",\"enum_vals\":null,\"boot_val\":\"16\",\"reset_val\":\"16\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"port\",\"setting\":\"5432\",\"unit\":null,\"category\":\"Connections and Authentication / Connection Settings\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"postmaster\",\"vartype\":\"integer\",\"min_val\":\"1\",\"max_val\":\"65535\",\"enum_vals\":null,\"boot_val\":\"5432\",\"reset_val\":\"5432\",\"source\":\"configuration file\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"search_path\",\"setting\":\"public\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"public\",\"reset_val\":\"public\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"server_version\",\"setting\":\"14.1\",\"unit\":null,\"category\":\"Preset Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"backend\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"14.1\",\"reset_val\":\"14.1\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"server_version_num\",\"setting\":\"140001\",\"unit\":null,\"category\":\"Preset Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"internal\",\"vartype\":\"integer\",\"min_val\":\"140001\",\"max_val\":\"140001\",\"enum_vals\":null,\"boot_val\":\"140001\",\"reset_val\":\"140001\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_batch_size\",\"setting\":\"5000\",\"unit\":null,\"category\":\"COPY / Batch size for non-atomic COPY operations\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"5000\",\"reset_val\":\"5000\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_commit_priority\",\"setting\":\"medium\",\"unit\":null,\"category\":\"COPY / RPC priority for commits for COPY operations\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"enum\",\"min_val\":null,\"max_val\":null,\"enum_vals\":[\"low\", \"medium\", \"high\"],\"boot_val\":\"medium\",\"reset_val\":\"medium\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_commit_size_multiplier_factor\",\"setting\":\"2.0\",\"unit\":null,\"category\":\"COPY / Factor for estimating COPY commit size\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"real\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"2.0\",\"reset_val\":\"2.0\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_commit_timeout\",\"setting\":\"300\",\"unit\":null,\"category\":\"COPY / Timeout in seconds for commits for COPY operations\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"300\",\"reset_val\":\"300\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_max_atomic_commit_size\",\"setting\":\"100000000\",\"unit\":null,\"category\":\"COPY / Max number of bytes in an atomic COPY operation\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"internal\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"100000000\",\"reset_val\":\"100000000\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_max_atomic_mutations\",\"setting\":\"20000\",\"unit\":null,\"category\":\"COPY / Max number of mutations for atomic COPY operations\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"internal\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"20000\",\"reset_val\":\"20000\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_max_non_atomic_commit_size\",\"setting\":\"5000000\",\"unit\":null,\"category\":\"COPY / The max number of bytes per commit in a non-atomic COPY operation\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"5000000\",\"reset_val\":\"5000000\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_max_parallelism\",\"setting\":\"128\",\"unit\":null,\"category\":\"COPY / Max concurrent transactions for a non-atomic COPY operation\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"128\",\"reset_val\":\"128\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_pipe_buffer_size\",\"setting\":\"65536\",\"unit\":null,\"category\":\"COPY / Buffer size for incoming COPY data messages\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"internal\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"100000000\",\"reset_val\":\"100000000\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_upsert\",\"setting\":\"off\",\"unit\":null,\"category\":\"COPY / Use Upsert instead of Insert for COPY\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"off\",\"reset_val\":\"off\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.ddl_transaction_mode\",\"setting\":\"AutocommitExplicitTransaction\",\"unit\":null,\"category\":\"PGAdapter Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"enum\",\"min_val\":null,\"max_val\":null,\"enum_vals\":[\"Single\", \"Batch\", \"AutocommitImplicitTransaction\", \"AutocommitExplicitTransaction\"],\"boot_val\":\"Batch\",\"reset_val\":\"AutocommitExplicitTransaction\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.emulate_pg_class_tables\",\"setting\":\"on\",\"unit\":null,\"category\":\"PGAdapter Options Emulate pg_class and related tables using common table expressions and textual OIDs\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"off\",\"reset_val\":\"off\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.force_autocommit\",\"setting\":\"off\",\"unit\":null,\"category\":\"PGAdapter Options Execute all statements in autocommit mode\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"off\",\"reset_val\":\"off\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.remove_escape_clause\",\"setting\":\"DEFAULT\",\"unit\":null,\"category\":\"PGAdapter Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"enum\",\"min_val\":null,\"max_val\":null,\"enum_vals\":[\"NONE\", \"DEFAULT\", \"ALL\"],\"boot_val\":\"DEFAULT\",\"reset_val\":\"DEFAULT\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.replace_pg_catalog_tables\",\"setting\":\"true\",\"unit\":null,\"category\":\"PGAdapter Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"on\",\"reset_val\":\"on\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.well_known_client\",\"setting\":\"JDBC\",\"unit\":null,\"category\":\"PGAdapter Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"backend\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"UNSPECIFIED\",\"reset_val\":\"JDBC\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"transaction_isolation\",\"setting\":\"serializable\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"enum\",\"min_val\":null,\"max_val\":null,\"enum_vals\":[\"serializable\", \"repeatable read\", \"read committed\", \"read uncommitted\"],\"boot_val\":\"serializable\",\"reset_val\":\"serializable\",\"source\":\"override\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"transaction_read_only\",\"setting\":\"off\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"off\",\"reset_val\":\"off\",\"source\":\"override\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb\n"
        + "]) t\n"
        + "),\n"
        + "pg_settings_names_ as (\n"
        + "select name from pg_settings_inmem_\n"
        + "union\n"
        + "select name from pg_catalog.pg_settings\n"
        + "),\n"
        + "pg_settings as (\n"
        + "select n.name, coalesce(s1.setting, s2.setting) as setting,coalesce(s1.unit, s2.unit) as unit,coalesce(s1.category, s2.category) as category,coalesce(s1.short_desc, s2.short_desc) as short_desc,coalesce(s1.extra_desc, s2.extra_desc) as extra_desc,coalesce(s1.context, s2.context) as context,coalesce(s1.vartype, s2.vartype) as vartype,coalesce(s1.source, s2.source) as source,coalesce(s1.min_val, s2.min_val) as min_val,coalesce(s1.max_val, s2.max_val) as max_val,coalesce(s1.enumvals, s2.enumvals) as enumvals,coalesce(s1.boot_val, s2.boot_val) as boot_val,coalesce(s1.reset_val, s2.reset_val) as reset_val,coalesce(s1.sourcefile, s2.sourcefile) as sourcefile,coalesce(s1.sourceline, s2.sourceline) as sourceline,coalesce(s1.pending_restart, s2.pending_restart) as pending_restart\n"
        + "from pg_settings_names_ n\n"
        + "left join pg_settings_inmem_ s1 using (name)\n"
        + "left join pg_catalog.pg_settings s2 using (name)\n"
        + "order by name\n"
        + ")\n"
        + "\n"
        + "select setting from pg_settings where name = 'edb_redwood_date'";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), ResultSet.newBuilder().setMetadata(
        ResultSetMetadata.newBuilder()
            .setRowType(StructType.newBuilder().addFields(Field.newBuilder()
                .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                    .setName("setting")
                .build()).build())
            .build()).build()));
  }

  private static void addRandomResultResults() {
    RandomResultSetGenerator generator =
        new RandomResultSetGenerator(RANDOM_RESULTS_ROW_COUNT, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(SELECT_RANDOM, generator.generate()));
  }

  static void setupJsonbResults() {
    setupJsonbResults(mockSpanner);
  }

  static void setupIntervalResults() {
    setupIntervalResults(mockSpanner);
  }

  static void setupJsonbResults(MockSpannerServiceImpl mockSpanner) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT t.oid, t.typname   "
                        + "FROM pg_type t  "
                        + "JOIN pg_namespace n ON t.typnamespace = n.oid "
                        + "WHERE t.typelem = (SELECT oid FROM pg_type WHERE typname = $1) AND substring(t.typname, 1, 1) = '_' AND t.typlen = -1 AND (n.nspname = $2 OR $3 AND n.nspname  IN ('pg_catalog', 'public')) "
                        + "ORDER BY t.typelem DESC LIMIT 1")
                .bind("p1")
                .to("jsonb")
                .bind("p2")
                .to((String) null)
                .bind("p3")
                .to(true)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("3807").build())
                        .addValues(Value.newBuilder().setStringValue("_jsonb").build())
                        .build())
                .build()));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT n.nspname  IN ('pg_catalog', 'public'), n.nspname, t.typname "
                        + "FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid "
                        + "WHERE t.oid = $1")
                .bind("p1")
                .to(3802L)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.BOOL, TypeCode.STRING, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                        .addValues(Value.newBuilder().setStringValue("jsonb").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT n.nspname  IN ('pg_catalog', 'public'), n.nspname, t.typname "
                        + "FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid "
                        + "WHERE t.oid = $1")
                .bind("p1")
                .to(3807L)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.BOOL, TypeCode.STRING, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                        .addValues(Value.newBuilder().setStringValue("_jsonb").build())
                        .build())
                .build()));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT e.typdelim FROM pg_type t, pg_type e WHERE t.oid = $1 and t.typelem = e.oid")
                .bind("p1")
                .to(3807L)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(",").build())
                        .build())
                .build()));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT e.oid, n.nspname  IN ('pg_catalog', 'public'), n.nspname, e.typname "
                        + "FROM pg_type t JOIN pg_type e ON t.typelem = e.oid "
                        + "JOIN pg_namespace n ON t.typnamespace = n.oid "
                        + "WHERE t.oid = $1")
                .bind("p1")
                .to(3807L)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(
                            TypeCode.INT64, TypeCode.BOOL, TypeCode.STRING, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("3802").build())
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                        .addValues(Value.newBuilder().setStringValue("jsonb").build())
                        .build())
                .build()));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT t.typarray, arr.typname   "
                        + "FROM pg_type t  "
                        + "JOIN pg_namespace n ON t.typnamespace = n.oid  "
                        + "JOIN pg_type arr ON arr.oid = t.typarray "
                        + "WHERE t.typname = $1 "
                        + "AND (n.nspname = $2 OR $3 AND n.nspname  IN ('pg_catalog', 'public')) "
                        + "ORDER BY t.oid DESC LIMIT 1")
                .bind("p1")
                .to("jsonb")
                .bind("p2")
                .to((String) null)
                .bind("p3")
                .to(true)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("3807").build())
                        .addValues(Value.newBuilder().setStringValue("_jsonb").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT substring(typname, 1, 1)='_' as is_array, typtype, typname, pg_type.oid   "
                        + "FROM pg_type   "
                        + "LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select 1 as r, 'public' as nspname ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  "
                        + "WHERE pg_type.oid = $1  "
                        + "ORDER BY sp.r, pg_type.oid DESC")
                .bind("p1")
                .to(3807L)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("is_array")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("typtype")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("typename")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("oid")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(Value.newBuilder().setStringValue("b").build())
                        .addValues(Value.newBuilder().setStringValue("_jsonb").build())
                        .addValues(Value.newBuilder().setStringValue("3807").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT typinput='pg_catalog.array_in'::regproc as is_array, typtype, typname, pg_type.oid   "
                        + "FROM pg_type   "
                        + "LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  "
                        + "WHERE pg_type.oid = $1  "
                        + "ORDER BY sp.r, pg_type.oid DESC")
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("is_array")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("typtype")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("typename")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("oid")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(Value.newBuilder().setStringValue("b").build())
                        .addValues(Value.newBuilder().setStringValue("_jsonb").build())
                        .addValues(Value.newBuilder().setStringValue("3807").build())
                        .build())
                .build()));
  }

  static void setupIntervalResults(MockSpannerServiceImpl mockSpanner) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT n.nspname  IN ('pg_catalog', 'public'), n.nspname, t.typname "
                        + "FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid "
                        + "WHERE t.oid = $1")
                .bind("p1")
                .to(1186L)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.BOOL, TypeCode.STRING, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                        .addValues(Value.newBuilder().setStringValue("interval").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT n.nspname  IN ('pg_catalog', 'public'), n.nspname, t.typname "
                        + "FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid "
                        + "WHERE t.oid = $1")
                .bind("p1")
                .to(1187L)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.BOOL, TypeCode.STRING, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                        .addValues(Value.newBuilder().setStringValue("_interval").build())
                        .build())
                .build()));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT e.typdelim FROM pg_type t, pg_type e WHERE t.oid = $1 and t.typelem = e.oid")
                .bind("p1")
                .to(Oid.INTERVAL_ARRAY)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(",").build())
                        .build())
                .build()));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT e.oid, n.nspname  IN ('pg_catalog', 'public'), n.nspname, e.typname "
                        + "FROM pg_type t JOIN pg_type e ON t.typelem = e.oid "
                        + "JOIN pg_namespace n ON t.typnamespace = n.oid "
                        + "WHERE t.oid = $1")
                .bind("p1")
                .to(Oid.INTERVAL_ARRAY)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(
                            TypeCode.INT64, TypeCode.BOOL, TypeCode.STRING, TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("3802").build())
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                        .addValues(Value.newBuilder().setStringValue("jsonb").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT substring(typname, 1, 1)='_' as is_array, typtype, typname, pg_type.oid   "
                        + "FROM pg_type   "
                        + "LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select 1 as r, 'public' as nspname ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  "
                        + "WHERE pg_type.oid = $1  "
                        + "ORDER BY sp.r, pg_type.oid DESC")
                .bind("p1")
                .to(1187L)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("is_array")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("typtype")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("typename")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("oid")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(Value.newBuilder().setStringValue("b").build())
                        .addValues(Value.newBuilder().setStringValue("_interval").build())
                        .addValues(Value.newBuilder().setStringValue("1187").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "with "
                        + PG_TYPE_PREFIX
                        + "\nSELECT substring(typname, 1, 1)='_' as is_array, typtype, typname, pg_type.oid   "
                        + "FROM pg_type   "
                        + "LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select 1 as r, 'public' as nspname ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  "
                        + "WHERE pg_type.oid = $1  "
                        + "ORDER BY sp.r, pg_type.oid DESC")
                .bind("p1")
                .to(1186L)
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("is_array")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("typtype")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("typename")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("oid")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setBoolValue(false).build())
                        .addValues(Value.newBuilder().setStringValue("b").build())
                        .addValues(Value.newBuilder().setStringValue("interval").build())
                        .addValues(Value.newBuilder().setStringValue("1186").build())
                        .build())
                .build()));
  }

}
