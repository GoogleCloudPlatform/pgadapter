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

package com.google.cloud.spanner.pgadapter.ruby;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.File;
import java.util.Map;
import java.util.Scanner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;

@Category(RubyTest.class)
@RunWith(JUnit4.class)
public class ActiveRecordMockServerTest extends AbstractRubyMockServerTest {
  private static final String DIRECTORY_NAME = "./src/test/ruby/activerecord";

  @BeforeClass
  public static void installGems() throws Exception {
    createVirtualEnv(DIRECTORY_NAME);
  }

  public static void createVirtualEnv(String directoryName) throws Exception {
    run(new String[] {"bundle", "install"}, directoryName, ImmutableMap.of());
  }

  public static String run(String[] command, String directoryName, Map<String, String> environment)
      throws Exception {
    File directory = new File(directoryName);
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(command);
    builder.directory(directory);
    builder.environment().putAll(environment);
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
    assertEquals(output + "\n\n\n" + error, 0, result);

    return output.toString();
  }

  @Test
  public void testDbMigrate() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_MIGRATIONS_TABLE,
            ResultSet.newBuilder().setMetadata(SELECT_TABLE_METADATA).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_AR_INTERNAL_METADATA_TABLE,
            ResultSet.newBuilder().setMetadata(SELECT_TABLE_METADATA).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_SCHEMA_MIGRATIONS_COLUMNS, SELECT_SCHEMA_MIGRATIONS_COLUMNS_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_AR_INTERNAL_METADATA_COLUMNS, SELECT_AR_INTERNAL_METADATA_COLUMNS_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT \"schema_migrations\".\"version\" "
                    + "FROM \"schema_migrations\" "
                    + "ORDER BY \"schema_migrations\".\"version\" ASC"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("version")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_MIGRATIONS_TABLE_EXTENDED,
            ResultSet.newBuilder()
                .setMetadata(SELECT_TABLE_METADATA)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("schema_migrations").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "INSERT INTO \"schema_migrations\" (\"version\") VALUES ($1) RETURNING \"version\""),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("version")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
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
            Statement.newBuilder(
                    "INSERT INTO \"schema_migrations\" (\"version\") VALUES ($1) RETURNING \"version\"")
                .bind("p1")
                .to("1")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("version")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT \"ar_internal_metadata\".* "
                    + "FROM \"ar_internal_metadata\" "
                    + "WHERE \"ar_internal_metadata\".\"key\" = $1 LIMIT $2"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("key")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("value")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("created_at")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("updated_at")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .build())
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p2")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "SELECT \"ar_internal_metadata\".* "
                        + "FROM \"ar_internal_metadata\" "
                        + "WHERE \"ar_internal_metadata\".\"key\" = $1 LIMIT $2")
                .bind("p1")
                .to("environment")
                .bind("p2")
                .to(1L)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("key")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("value")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("created_at")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("updated_at")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .build())
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p2")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .build()));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_CLASS_PREFIX
                    + ",\n"
                    + "pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + ")\n"
                    + "SELECT c.relname "
                    + "FROM pg_class c "
                    + "LEFT JOIN pg_namespace n ON n.oid = c.relnamespace "
                    + "WHERE n.nspname  IN ('public') "
                    + "AND c.relkind IN ('r','v','m','p','f')"),
            ResultSet.newBuilder()
                .setMetadata(SELECT_TABLE_METADATA)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder().setStringValue("ar_internal_metadata").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("schema_migrations").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("singers").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("albums").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_CLASS_PREFIX
                    + ",\n"
                    + "pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + ")\n"
                    + "SELECT c.relname "
                    + "FROM pg_class c "
                    + "LEFT JOIN pg_namespace n ON n.oid = c.relnamespace "
                    + "WHERE n.nspname  IN ('public') "
                    + "AND c.relkind IN ('r','p')"),
            ResultSet.newBuilder()
                .setMetadata(SELECT_TABLE_METADATA)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder().setStringValue("ar_internal_metadata").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("schema_migrations").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("singers").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("albums").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "INSERT INTO \"ar_internal_metadata\" "
                    + "(\"key\", \"value\", \"created_at\", \"updated_at\") "
                    + "VALUES ($1, $2, $3, $4) RETURNING \"key\""),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("key")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p2")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p3")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p4")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .build())
                        .build())
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    // INSERT INTO "ar_internal_metadata" ("key", "value", "created_at", "updated_at") VALUES ($1,
    // $2, $3, $4) RETURNING "key"
    mockSpanner.putPartialStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "INSERT INTO \"ar_internal_metadata\" "
                        + "(\"key\", \"value\", \"created_at\", \"updated_at\") "
                        + "VALUES ($1, $2, $3, $4) RETURNING \"key\"")
                .bind("p1")
                .to("environment")
                .bind("p2")
                .to("default_env")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("key")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p2")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p3")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p4")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("environment").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_TYPE_PREFIX
                    + "\nSELECT a.attname, format_type(a.atttypid, a.atttypmod),\n"
                    + "       pg_get_expr(d.adbin, d.adrelid), a.attnotnull, a.atttypid, a.atttypmod,\n"
                    + "       c.collname, col_description(a.attrelid, a.attnum) AS comment,\n"
                    + "       attgenerated as attgenerated\n"
                    + "  FROM pg_attribute a\n"
                    + "  LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum\n"
                    + "  LEFT JOIN pg_type t ON a.atttypid = t.oid\n"
                    + "  LEFT JOIN pg_collation c ON a.attcollation = c.oid AND a.attcollation <> t.typcollation\n"
                    + " WHERE a.attrelid = '\"albums\"'::regclass\n"
                    + "   AND a.attnum > 0 AND NOT a.attisdropped\n"
                    + " ORDER BY a.attnum\n"),
            ResultSet.newBuilder()
                .setMetadata(SELECT_COLUMNS_METADATA)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("album_id").build())
                        .addValues(Value.newBuilder().setStringValue("character varying").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(
                            Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                        .addValues(Value.newBuilder().setStringValue("-1").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("title").build())
                        .addValues(Value.newBuilder().setStringValue("character varying").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setBoolValue(false).build())
                        .addValues(
                            Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                        .addValues(Value.newBuilder().setStringValue("-1").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("singer_id").build())
                        .addValues(Value.newBuilder().setStringValue("character varying").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setBoolValue(false).build())
                        .addValues(
                            Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                        .addValues(Value.newBuilder().setStringValue("-1").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT a.attname\n"
                    + "  FROM (\n"
                    + "         SELECT indrelid, indkey, generate_subscripts(indkey, 1) idx\n"
                    + "           FROM pg_index\n"
                    + "          WHERE indrelid = '\"albums\"'::regclass\n"
                    + "            AND indisprimary\n"
                    + "       ) i\n"
                    + "  JOIN pg_attribute a\n"
                    + "    ON a.attrelid = i.indrelid\n"
                    + "   AND a.attnum = i.indkey[i.idx]\n"
                    + " ORDER BY i.idx\n"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("attname")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("album_id").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_CLASS_PREFIX
                    + ",\n"
                    + "pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + ")\n"
                    + "SELECT pg_catalog.obj_description(c.oid, 'pg_class')\n"
                    + "FROM pg_class c\n"
                    + "  LEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n"
                    + "WHERE c.relname = 'albums'\n"
                    + "  AND c.relkind IN ('r','p')\n"
                    + "  AND n.nspname  IN ('public')\n"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("obj_description")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("albums").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_CLASS_PREFIX
                    + ",\n"
                    + "pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + ")\n"
                    + "SELECT distinct i.relname, d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid), t.oid,\n"
                    + "                pg_catalog.obj_description(i.oid, 'pg_class') AS comment\n"
                    + "FROM pg_class t\n"
                    + "INNER JOIN pg_index d ON t.oid = d.indrelid\n"
                    + "INNER JOIN pg_class i ON d.indexrelid = i.oid\n"
                    + "LEFT JOIN pg_namespace n ON n.oid = i.relnamespace\n"
                    + "WHERE i.relkind IN ('i', 'I')\n"
                    + "  AND d.indisprimary = 'f'\n"
                    + "  AND t.relname = 'albums'\n"
                    + "  AND n.nspname  IN ('public')\n"
                    + "ORDER BY i.relname\n"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("relname")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("indisunique")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("indkey")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("pg_get_indexdef")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("oid")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("comment")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_CLASS_PREFIX
                    + ",\n"
                    + "pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + ")\n"
                    + "            SELECT conname, pg_get_constraintdef(c.oid, true) AS constraintdef, c.convalidated AS valid\n"
                    + "            FROM pg_constraint c\n"
                    + "            JOIN pg_class t ON c.conrelid = t.oid\n"
                    + "            JOIN pg_namespace n ON n.oid = c.connamespace\n"
                    + "            WHERE c.contype = 'c'\n"
                    + "              AND t.relname = 'albums'\n"
                    + "              AND n.nspname  IN ('public')\n"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("conname")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("constraintdef")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("valid")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .build())
                        .build())
                .build()));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_TYPE_PREFIX
                    + "\nSELECT a.attname, format_type(a.atttypid, a.atttypmod),\n"
                    + "       pg_get_expr(d.adbin, d.adrelid), a.attnotnull, a.atttypid, a.atttypmod,\n"
                    + "       c.collname, col_description(a.attrelid, a.attnum) AS comment,\n"
                    + "       attgenerated as attgenerated\n"
                    + "  FROM pg_attribute a\n"
                    + "  LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum\n"
                    + "  LEFT JOIN pg_type t ON a.atttypid = t.oid\n"
                    + "  LEFT JOIN pg_collation c ON a.attcollation = c.oid AND a.attcollation <> t.typcollation\n"
                    + " WHERE a.attrelid = '\"singers\"'::regclass\n"
                    + "   AND a.attnum > 0 AND NOT a.attisdropped\n"
                    + " ORDER BY a.attnum\n"),
            ResultSet.newBuilder()
                .setMetadata(SELECT_COLUMNS_METADATA)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("singer_id").build())
                        .addValues(Value.newBuilder().setStringValue("character varying").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(
                            Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                        .addValues(Value.newBuilder().setStringValue("-1").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("first_name").build())
                        .addValues(Value.newBuilder().setStringValue("character varying").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setBoolValue(false).build())
                        .addValues(
                            Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                        .addValues(Value.newBuilder().setStringValue("-1").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("last_name").build())
                        .addValues(Value.newBuilder().setStringValue("character varying").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setBoolValue(false).build())
                        .addValues(
                            Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                        .addValues(Value.newBuilder().setStringValue("-1").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .addValues(Value.newBuilder().setStringValue("").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT a.attname\n"
                    + "  FROM (\n"
                    + "         SELECT indrelid, indkey, generate_subscripts(indkey, 1) idx\n"
                    + "           FROM pg_index\n"
                    + "          WHERE indrelid = '\"singers\"'::regclass\n"
                    + "            AND indisprimary\n"
                    + "       ) i\n"
                    + "  JOIN pg_attribute a\n"
                    + "    ON a.attrelid = i.indrelid\n"
                    + "   AND a.attnum = i.indkey[i.idx]\n"
                    + " ORDER BY i.idx\n"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("attname")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("singer_id").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_CLASS_PREFIX
                    + ",\n"
                    + "pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + ")\n"
                    + "SELECT pg_catalog.obj_description(c.oid, 'pg_class')\n"
                    + "FROM pg_class c\n"
                    + "  LEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n"
                    + "WHERE c.relname = 'singers'\n"
                    + "  AND c.relkind IN ('r','p')\n"
                    + "  AND n.nspname  IN ('public')\n"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("obj_description")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("singers").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_CLASS_PREFIX
                    + ",\n"
                    + "pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + ")\n"
                    + "SELECT distinct i.relname, d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid), t.oid,\n"
                    + "                pg_catalog.obj_description(i.oid, 'pg_class') AS comment\n"
                    + "FROM pg_class t\n"
                    + "INNER JOIN pg_index d ON t.oid = d.indrelid\n"
                    + "INNER JOIN pg_class i ON d.indexrelid = i.oid\n"
                    + "LEFT JOIN pg_namespace n ON n.oid = i.relnamespace\n"
                    + "WHERE i.relkind IN ('i', 'I')\n"
                    + "  AND d.indisprimary = 'f'\n"
                    + "  AND t.relname = 'singers'\n"
                    + "  AND n.nspname  IN ('public')\n"
                    + "ORDER BY i.relname\n"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("relname")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("indisunique")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("indkey")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("pg_get_indexdef")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("oid")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("comment")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_CLASS_PREFIX
                    + ",\n"
                    + "pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + ")\n"
                    + "            SELECT conname, pg_get_constraintdef(c.oid, true) AS constraintdef, c.convalidated AS valid\n"
                    + "            FROM pg_constraint c\n"
                    + "            JOIN pg_class t ON c.conrelid = t.oid\n"
                    + "            JOIN pg_namespace n ON n.oid = c.connamespace\n"
                    + "            WHERE c.contype = 'c'\n"
                    + "              AND t.relname = 'singers'\n"
                    + "              AND n.nspname  IN ('public')\n"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("conname")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("constraintdef")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("valid")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_CLASS_PREFIX
                    + ",\n"
                    + "pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + ")\n"
                    + "SELECT t2.oid::regclass::text AS to_table, a1.attname AS column, a2.attname AS primary_key, c.conname AS name, c.confupdtype AS on_update, c.confdeltype AS on_delete, c.convalidated AS valid, c.condeferrable AS deferrable, c.condeferred AS deferred\n"
                    + "FROM pg_constraint c\n"
                    + "JOIN pg_class t1 ON c.conrelid = t1.oid\n"
                    + "JOIN pg_class t2 ON c.confrelid = t2.oid\n"
                    + "JOIN pg_attribute a1 ON a1.attnum = c.conkey[1] AND a1.attrelid = t1.oid\n"
                    + "JOIN pg_attribute a2 ON a2.attnum = c.confkey[1] AND a2.attrelid = t2.oid\n"
                    + "JOIN pg_namespace t3 ON c.connamespace = t3.oid\n"
                    + "WHERE c.contype = 'f'\n"
                    + "  AND t1.relname = 'albums'\n"
                    + "  AND t3.nspname  IN ('public')\n"
                    + "ORDER BY c.conname\n"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("to_table")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("column")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("name")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("on_update")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("on_delete")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("valid")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("deferrable")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("deferred")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_CLASS_PREFIX
                    + ",\n"
                    + "pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + ")\n"
                    + "SELECT t2.oid::regclass::text AS to_table, a1.attname AS column, a2.attname AS primary_key, c.conname AS name, c.confupdtype AS on_update, c.confdeltype AS on_delete, c.convalidated AS valid, c.condeferrable AS deferrable, c.condeferred AS deferred\n"
                    + "FROM pg_constraint c\n"
                    + "JOIN pg_class t1 ON c.conrelid = t1.oid\n"
                    + "JOIN pg_class t2 ON c.confrelid = t2.oid\n"
                    + "JOIN pg_attribute a1 ON a1.attnum = c.conkey[1] AND a1.attrelid = t1.oid\n"
                    + "JOIN pg_attribute a2 ON a2.attnum = c.confkey[1] AND a2.attrelid = t2.oid\n"
                    + "JOIN pg_namespace t3 ON c.connamespace = t3.oid\n"
                    + "WHERE c.contype = 'f'\n"
                    + "  AND t1.relname = 'singers'\n"
                    + "  AND t3.nspname  IN ('public')\n"
                    + "ORDER BY c.conname\n"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("to_table")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("column")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("name")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("on_update")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("on_delete")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("valid")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("deferrable")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("deferred")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .build())
                        .build())
                .build()));

    // For initializing the DDL executor.
    addDdlResponseToSpannerAdmin();
    // CREATE TABLE schema_migrations
    addDdlResponseToSpannerAdmin();
    // CREATE TABLE ar_internal_metadata
    addDdlResponseToSpannerAdmin();
    // The actual migration.
    addDdlResponseToSpannerAdmin();

    run(
        new String[] {"bundle", "exec", "rake", "db:migrate"},
        DIRECTORY_NAME,
        ImmutableMap.of("PGHOST", "localhost", "PGPORT", String.valueOf(pgServer.getLocalPort())));
  }
}
