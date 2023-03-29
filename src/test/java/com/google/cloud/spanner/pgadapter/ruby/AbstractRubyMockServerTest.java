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
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.EmptyPgEnum;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgCollation;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgConstraint;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgIndex;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgNamespace;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.postgresql.core.Oid;

public class AbstractRubyMockServerTest extends AbstractMockServerTest {
  static String randomUuid() {
    return UUID.randomUUID().toString();
  }

  private static final Statement SELECT_TYPES =
      Statement.of(
          "with "
              + PG_TYPE_PREFIX
              + "\nSELECT t.oid, t.typname\n"
              + "FROM pg_type as t\n"
              + "WHERE t.typname IN ('int2', 'int4', 'int8', 'oid', 'float4', 'float8', 'numeric', 'bool', 'timestamp', 'timestamptz')\n");
  private static final ResultSetMetadata SELECT_TYPES_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("oid")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("typname")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();

  private static final com.google.spanner.v1.ResultSet SELECT_TYPES_RESULTSET =
      ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT2)).build())
                  .addValues(Value.newBuilder().setStringValue("int2").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT4)).build())
                  .addValues(Value.newBuilder().setStringValue("int4").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT8)).build())
                  .addValues(Value.newBuilder().setStringValue("int8").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT4)).build())
                  .addValues(Value.newBuilder().setStringValue("float4").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT8)).build())
                  .addValues(Value.newBuilder().setStringValue("float8").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.NUMERIC)).build())
                  .addValues(Value.newBuilder().setStringValue("numeric").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMP)).build())
                  .addValues(Value.newBuilder().setStringValue("timestamp").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMPTZ)).build())
                  .addValues(Value.newBuilder().setStringValue("timestamptz").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.BOOL)).build())
                  .addValues(Value.newBuilder().setStringValue("bool").build())
                  .build())
          .setMetadata(SELECT_TYPES_METADATA)
          .build();

  private static final Statement SELECT_TYPES_EXTENDED =
      Statement.of(
          "with "
              + PG_TYPE_PREFIX
              + ",\n"
              + "pg_range as (\n"
              + "select * from (select 0::bigint as rngtypid, 0::bigint as rngsubtype, 0::bigint as rngmultitypid, 0::bigint as rngcollation, 0::bigint as rngsubopc, ''::varchar as rngcanonical, ''::varchar as rngsubdiff\n"
              + ") range where false)\n"
              + "SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype, t.typtype, t.typbasetype\n"
              + "FROM pg_type as t\n"
              + "LEFT JOIN pg_range as r ON oid = rngtypid\n"
              + "WHERE\n"
              + "  t.typname IN ('int2', 'int4', 'int8', 'oid', 'float4', 'float8', 'text', 'varchar', 'char', "
              + "'name', 'bpchar', 'bool', 'bit', 'varbit', 'date', 'money', 'bytea', 'point', 'hstore', 'json', "
              + "'jsonb', 'cidr', 'inet', 'uuid', 'xml', 'tsvector', 'macaddr', 'citext', 'ltree', 'line', "
              + "'lseg', 'box', 'path', 'polygon', 'circle', 'time', 'timestamp', 'timestamptz', 'numeric', 'interval')\n");
  private static final ResultSetMetadata SELECT_TYPES_EXTENDED_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("oid")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("typname")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("typelem")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("typdelim")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("typinput")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("rngsubtype")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("typtype")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("typbasetype")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet SELECT_TYPES_EXTENDED_RESULTSET =
      ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT2)).build())
                  .addValues(Value.newBuilder().setStringValue("int2").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("int2in").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT4)).build())
                  .addValues(Value.newBuilder().setStringValue("int4").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("int4in").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT8)).build())
                  .addValues(Value.newBuilder().setStringValue("int8").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("int8in").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT4)).build())
                  .addValues(Value.newBuilder().setStringValue("float4").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("float4in").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT8)).build())
                  .addValues(Value.newBuilder().setStringValue("float8").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("float8in").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.NUMERIC)).build())
                  .addValues(Value.newBuilder().setStringValue("numeric").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("numericin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMP)).build())
                  .addValues(Value.newBuilder().setStringValue("timestamp").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("timestampin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMPTZ)).build())
                  .addValues(Value.newBuilder().setStringValue("timestamptz").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("timestamptzin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.BOOL)).build())
                  .addValues(Value.newBuilder().setStringValue("bool").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("boolin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.TEXT)).build())
                  .addValues(Value.newBuilder().setStringValue("text").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("textin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                  .addValues(Value.newBuilder().setStringValue("varchar").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("varcharin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.JSONB)).build())
                  .addValues(Value.newBuilder().setStringValue("jsonb").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("jsonbin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.BYTEA)).build())
                  .addValues(Value.newBuilder().setStringValue("bytea").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("byteain").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.DATE)).build())
                  .addValues(Value.newBuilder().setStringValue("date").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("datein").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .setMetadata(SELECT_TYPES_EXTENDED_METADATA)
          .build();

  private static final Statement SELECT_RANGE_TYPES =
      Statement.of(
          "with "
              + PG_TYPE_PREFIX
              + ",\n"
              + "pg_range as (\n"
              + "select * from (select 0::bigint as rngtypid, 0::bigint as rngsubtype, 0::bigint as rngmultitypid, 0::bigint as rngcollation, 0::bigint as rngsubopc, ''::varchar as rngcanonical, ''::varchar as rngsubdiff\n"
              + ") range where false)\n"
              + "SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype, t.typtype, t.typbasetype\n"
              + "FROM pg_type as t\n"
              + "LEFT JOIN pg_range as r ON oid = rngtypid\n"
              + "WHERE\n"
              + "  t.typtype IN ('r', 'e', 'd')\n");
  private static final ResultSet SELECT_RANGE_TYPES_RESULTSET =
      ResultSet.newBuilder().setMetadata(SELECT_TYPES_EXTENDED_METADATA).build();

  private static final Statement SELECT_ARRAY_TYPES =
      Statement.of(
          "with "
              + PG_TYPE_PREFIX
              + ",\n"
              + "pg_range as (\n"
              + "select * from (select 0::bigint as rngtypid, 0::bigint as rngsubtype, 0::bigint as rngmultitypid, 0::bigint as rngcollation, 0::bigint as rngsubopc, ''::varchar as rngcanonical, ''::varchar as rngsubdiff\n"
              + ") range where false)\n"
              + "SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype, t.typtype, t.typbasetype\n"
              + "FROM pg_type as t\n"
              + "LEFT JOIN pg_range as r ON oid = rngtypid\n"
              + "WHERE\n"
              + "  t.typelem IN (21, 23, 20, 700, 701, 1700, 1114, 1184, 16, 25, 1043, 3802, 17, 1082)\n");
  private static final com.google.spanner.v1.ResultSet SELECT_ARRAY_TYPES_RESULTSET =
      ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.INT2_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_int2").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT2)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.INT4_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_int4").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT4)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.INT8_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_int8").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT8)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT4_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_float4").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT4)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT8_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_float8").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT8)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.NUMERIC_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_numeric").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.NUMERIC)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder()
                          .setStringValue(String.valueOf(Oid.TIMESTAMP_ARRAY))
                          .build())
                  .addValues(Value.newBuilder().setStringValue("_timestamp").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMP)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder()
                          .setStringValue(String.valueOf(Oid.TIMESTAMPTZ_ARRAY))
                          .build())
                  .addValues(Value.newBuilder().setStringValue("_timestamptz").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMPTZ)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.BOOL_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_bool").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.BOOL)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TEXT_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_text").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.TEXT)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_varchar").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.JSONB_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_jsonb").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.JSONB)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.BYTEA_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_bytea").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.BYTEA)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.DATE_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_date").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.DATE)).build())
                  .addValues(Value.newBuilder().setStringValue(",").build())
                  .addValues(Value.newBuilder().setStringValue("arrayin").build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setStringValue("0").build())
                  .build())
          .setMetadata(SELECT_TYPES_EXTENDED_METADATA)
          .build();

  protected static final Statement SELECT_MIGRATIONS_TABLE =
      getSelectRelNameStatement("schema_migrations");
  protected static final Statement SELECT_MIGRATIONS_TABLE_EXTENDED =
      getSelectRelNameStatementAllRelKinds("schema_migrations");
  protected static final ResultSetMetadata SELECT_RELNAME_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("relname")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();

  protected static final Statement SELECT_AR_INTERNAL_METADATA_TABLE =
      getSelectRelNameStatement("ar_internal_metadata");
  protected static final Statement SELECT_SCHEMA_MIGRATIONS_COLUMNS =
      getSelectAttributesStatement("schema_migrations");
  protected static final ResultSetMetadata SELECT_COLUMNS_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("attname")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("format_type")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("pg_get_expr")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("attnotnull")
                          .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("atttypid")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("atttypmod")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("collname")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("comment")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("attgenerated")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();
  protected static final ResultSet SELECT_SCHEMA_MIGRATIONS_COLUMNS_RESULTSET =
      ResultSet.newBuilder()
          .setMetadata(SELECT_COLUMNS_METADATA)
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("version").build())
                  .addValues(Value.newBuilder().setStringValue("character varying").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setBoolValue(true).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                  .addValues(Value.newBuilder().setStringValue("-1").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .build())
          .build();

  protected static final Statement SELECT_AR_INTERNAL_METADATA_COLUMNS =
      getSelectAttributesStatement("ar_internal_metadata");
  protected static final ResultSet SELECT_AR_INTERNAL_METADATA_COLUMNS_RESULTSET =
      ResultSet.newBuilder()
          .setMetadata(SELECT_COLUMNS_METADATA)
          .addRows(createColumnRow("key", "character varying", true, Oid.VARCHAR))
          .addRows(createColumnRow("value", "character varying", true, Oid.VARCHAR))
          .addRows(createColumnRow("created_at", "timestamptz", true, Oid.TIMESTAMPTZ))
          .addRows(createColumnRow("updated_at", "timestamptz", true, Oid.TIMESTAMPTZ))
          .build();

  private static final Statement SELECT_EXTENSIONS =
      Statement.of(
          "with pg_extension as (\n"
              + "select * from (select 0::bigint as oid, ''::varchar as extname, 0::bigint as extowner, 0::bigint as extnamespace, false::bool as extrelocatable, ''::varchar as extversion, '{}'::bigint[] as extconfig, '{}'::text[] as extcondition\n"
              + ") ext where false)\n"
              + "SELECT extname FROM pg_extension");
  private static final ResultSet SELECT_EXTENSIONS_RESULTSET =
      ResultSet.newBuilder()
          .setMetadata(
              ResultSetMetadata.newBuilder()
                  .setRowType(
                      StructType.newBuilder()
                          .addFields(
                              Field.newBuilder()
                                  .setName("extname")
                                  .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                  .build())
                          .build())
                  .build())
          .build();

  private static final Statement SELECT_ENUMS =
      Statement.of(
          "with "
              + EmptyPgEnum.PG_ENUM_CTE
              + ",\n"
              + PG_TYPE_PREFIX
              + "\nSELECT\n"
              + "  type.typname AS name,\n"
              + "  ''::varchar AS value\n"
              + "FROM pg_enum AS enum\n"
              + "JOIN pg_type AS type\n"
              + "  ON (type.oid = enum.enumtypid)\n"
              + "GROUP BY type.typname;\n");
  private static final ResultSet SELECT_ENUMS_RESULTSET =
      ResultSet.newBuilder()
          .setMetadata(
              ResultSetMetadata.newBuilder()
                  .setRowType(
                      StructType.newBuilder()
                          .addFields(
                              Field.newBuilder()
                                  .setName("name")
                                  .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                  .build())
                          .addFields(
                              Field.newBuilder()
                                  .setName("value")
                                  .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                  .build())
                          .build())
                  .build())
          .build();

  @BeforeClass
  public static void setupResults() {
    mockSpanner.putStatementResult(StatementResult.query(SELECT_TYPES, SELECT_TYPES_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_TYPES_EXTENDED, SELECT_TYPES_EXTENDED_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_RANGE_TYPES, SELECT_RANGE_TYPES_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_ARRAY_TYPES, SELECT_ARRAY_TYPES_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_EXTENSIONS, SELECT_EXTENSIONS_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.query(SELECT_ENUMS, SELECT_ENUMS_RESULTSET));
  }
  //
  //  @BeforeClass
  //  public static void startMockSpannerAndPgAdapterServers() throws Exception {
  //    doStartMockSpannerAndPgAdapterServers(
  //        new MockSpannerServiceImpl(),
  //        "d",
  //        Collections.singleton("-ddl=AutocommitExplicitTransaction"));
  //  }

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

  static void addMigrationsTableResults() {
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_MIGRATIONS_TABLE, createSelectRelNameResultSet("schema_migrations")));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_AR_INTERNAL_METADATA_TABLE,
            createSelectRelNameResultSet("ar_internal_metadata")));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_SCHEMA_MIGRATIONS_COLUMNS, SELECT_SCHEMA_MIGRATIONS_COLUMNS_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_AR_INTERNAL_METADATA_COLUMNS, SELECT_AR_INTERNAL_METADATA_COLUMNS_RESULTSET));

    addMigrationResults("1");
  }

  static Statement getSelectRelNameStatement(String table) {
    return getSelectRelNameStatement(table, "'r','p'");
  }

  static Statement getSelectRelNameStatementAllRelKinds(String table) {
    return getSelectRelNameStatement(table, "'r','v','m','p','f'");
  }

  static Statement getSelectRelNameStatement(String table, String relKinds) {
    return Statement.of(
        String.format(
            "with "
                + EMULATED_PG_CLASS_PREFIX
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
                + "AND c.relname = '%s' "
                + "AND c.relkind IN (%s)",
            table,
            relKinds));
  }

  static Statement getSelectAttributesStatement(String table) {
    return Statement.of(
        String.format(
            "with "
                + EMULATED_PG_ATTRIBUTE_PREFIX
                + ",\n"
                + EMULATED_PG_ATTRDEF_PREFIX
                + ",\n"
                + PG_TYPE_PREFIX
                + ",\n"
                + PgCollation.PG_COLLATION_CTE
                + "\n"
                + "SELECT a.attname, a.spanner_type as format_type,\n"
                + "       d.adbin as pg_get_expr, a.attnotnull, a.atttypid, a.atttypmod,\n"
                + "       c.collname, ''::varchar AS comment,\n"
                + "       attgenerated as attgenerated\n"
                + "  FROM pg_attribute a\n"
                + "  LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum\n"
                + "  LEFT JOIN pg_type t ON a.atttypid = t.oid\n"
                + "  LEFT JOIN pg_collation c ON a.attcollation = c.oid AND a.attcollation <> t.typcollation\n"
                + " WHERE a.attrelid = '''\"public\".\"%s\"'''\n"
                + "   AND a.attnum > 0 AND NOT a.attisdropped\n"
                + " ORDER BY a.attnum\n",
            table));
  }

  static ListValue createColumnRow(String name, String type, boolean notNull, int oid) {
    return ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue(name).build())
        .addValues(Value.newBuilder().setStringValue(type).build())
        .addValues(Value.newBuilder().setStringValue("").build())
        .addValues(Value.newBuilder().setBoolValue(notNull).build())
        .addValues(Value.newBuilder().setStringValue(String.valueOf(oid)).build())
        .addValues(Value.newBuilder().setStringValue("-1").build())
        .addValues(Value.newBuilder().setStringValue("").build())
        .addValues(Value.newBuilder().setStringValue("").build())
        .addValues(Value.newBuilder().setStringValue("").build())
        .build();
  }

  static ResultSet createSelectRelNameResultSet(String table) {
    return ResultSet.newBuilder()
        .setMetadata(SELECT_RELNAME_METADATA)
        .addRows(
            ListValue.newBuilder()
                .addValues(Value.newBuilder().setStringValue(table).build())
                .build())
        .build();
  }

  static void addMigrationResults(String... versions) {
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
                .addAllRows(
                    Arrays.stream(versions)
                        .map(
                            version ->
                                ListValue.newBuilder()
                                    .addValues(Value.newBuilder().setStringValue(version).build())
                                    .build())
                        .collect(Collectors.toList()))
                .build()));
  }

  static void addSelectTablesResult(String... tables) {
    addSelectTablesResultWithRelKinds("'r','v','m','p','f'", tables);
  }

  static void addSelectTablesResultWithRelKinds(String relKinds, String... tables) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                String.format(
                    "with "
                        + EMULATED_PG_CLASS_PREFIX
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
                        + "AND c.relkind IN (%s)",
                    relKinds)),
            ResultSet.newBuilder()
                .setMetadata(SELECT_RELNAME_METADATA)
                .addAllRows(
                    Arrays.stream(tables)
                        .map(
                            table ->
                                ListValue.newBuilder()
                                    .addValues(Value.newBuilder().setStringValue(table).build())
                                    .build())
                        .collect(Collectors.toList()))
                .build()));
  }

  static void addSelectPrimaryKeyResult(String table, String... columns) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                String.format(
                    "SELECT ic.column_name as attname\n"
                        + "FROM information_schema.index_columns ic\n"
                        + "INNER JOIN information_schema.indexes i using (table_catalog, table_schema, table_name, index_name)\n"
                        + "WHERE ic.table_schema='public' and ic.table_name='%s'\n"
                        + "AND i.index_type='PRIMARY_KEY'\n"
                        + "ORDER BY ordinal_position\n",
                    table)),
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
                .addAllRows(
                    Arrays.stream(columns)
                        .map(
                            column ->
                                ListValue.newBuilder()
                                    .addValues(Value.newBuilder().setStringValue(column).build())
                                    .build())
                        .collect(Collectors.toList()))
                .build()));
  }

  static void addSelectColumnsResult(String table, ListValue... columns) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            getSelectAttributesStatement(table),
            ResultSet.newBuilder()
                .setMetadata(SELECT_COLUMNS_METADATA)
                .addAllRows(Arrays.stream(columns).collect(Collectors.toList()))
                .build()));
  }

  static void addInsertMigrationResult(String version) {
    String sql =
        "INSERT INTO \"schema_migrations\" (\"version\") VALUES ($1) RETURNING \"version\"";
    ResultSetMetadata metadata =
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
            .build();

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(version).build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(version).build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));
  }

  static void addSelectArInternalMetadataResult(String key, long limit) {
    String sql =
        "SELECT \"ar_internal_metadata\".* "
            + "FROM \"ar_internal_metadata\" "
            + "WHERE \"ar_internal_metadata\".\"key\" = $1 LIMIT $2";
    ResultSetMetadata metadata =
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
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("updated_at")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
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
            .build();

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ResultSet.newBuilder().setMetadata(metadata).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(key).bind("p2").to(limit).build(),
            ResultSet.newBuilder().setMetadata(metadata).build()));
  }

  static void addInsertArInternalMetadataResult(String key, String value) {
    String sql =
        "INSERT INTO \"ar_internal_metadata\" "
            + "(\"key\", \"value\", \"created_at\", \"updated_at\") "
            + "VALUES ($1, $2, $3, $4) RETURNING \"key\"";
    ResultSetMetadata metadata =
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
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p4")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    // INSERT INTO "ar_internal_metadata" ("key", "value", "created_at", "updated_at")
    // VALUES ($1, $2, $3, $4) RETURNING "key"
    mockSpanner.putPartialStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(key).bind("p2").to(value).build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(key).build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));
  }

  static void addSelectTableDescriptionResult(String table, String description) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                String.format(
                    "with "
                        + EMULATED_PG_CLASS_PREFIX
                        + ",\n"
                        + "pg_namespace as (\n"
                        + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                        + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                        + "  from information_schema.schemata\n"
                        + ")\n"
                        + "SELECT ''::varchar AS obj_description\n"
                        + "FROM pg_class c\n"
                        + "  LEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n"
                        + "WHERE c.relname = '%s'\n"
                        + "  AND c.relkind IN ('r','p')\n"
                        + "  AND n.nspname  IN ('public')\n",
                    table)),
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
                        .addValues(Value.newBuilder().setStringValue(description).build())
                        .build())
                .build()));
  }

  static void addSelectTableIndexesResult(String table) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                String.format(
                    "with "
                        + EMULATED_PG_CLASS_PREFIX
                        + ",\n"
                        + PgIndex.PG_INDEX_CTE
                        + ",\n"
                        + PgNamespace.PG_NAMESPACE_CTE
                        + "\n"
                        + "SELECT i.relname, d.indisunique, d.indkey, 'CREATE INDEX ON USING btree ( )'::varchar AS pg_get_indexdef, t.oid,\n"
                        + "                ''::varchar AS comment\n"
                        + "FROM pg_class t\n"
                        + "INNER JOIN pg_index d ON t.oid = d.indrelid\n"
                        + "INNER JOIN pg_class i ON d.indexrelid = i.oid\n"
                        + "LEFT JOIN pg_namespace n ON n.oid = i.relnamespace\n"
                        + "WHERE i.relkind IN ('i', 'I')\n"
                        + "  AND d.indisprimary = 'f'\n"
                        + "  AND t.relname = '%s'\n"
                        + "  AND n.nspname  IN ('public')\n"
                        + "ORDER BY i.relname\n",
                    table)),
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
  }

  static void addSelectTableConstraintsResult(String table) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                String.format(
                    "with "
                        + PgConstraint.PG_CONSTRAINT_CTE
                        + ",\n"
                        + EMULATED_PG_CLASS_PREFIX
                        + ",\n"
                        + PgNamespace.PG_NAMESPACE_CTE
                        + "\n"
                        + "            SELECT conname, conbin AS constraintdef, c.convalidated AS valid\n"
                        + "            FROM pg_constraint c\n"
                        + "            JOIN pg_class t ON c.conrelid = t.oid\n"
                        + "            JOIN pg_namespace n ON n.oid = c.connamespace\n"
                        + "            WHERE c.contype = 'c'\n"
                        + "              AND t.relname = '%s'\n"
                        + "              AND n.nspname  IN ('public')\n",
                    table)),
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
  }

  static void addSelectTableForeignKeysResult(String table) {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                String.format(
                    "with "
                        + PgConstraint.PG_CONSTRAINT_CTE
                        + ",\n"
                        + EMULATED_PG_CLASS_PREFIX
                        + ",\n"
                        + EMULATED_PG_ATTRIBUTE_PREFIX
                        + ",\n"
                        + PgNamespace.PG_NAMESPACE_CTE
                        + "\nSELECT substr( t2.oid, 12, length( t2.oid) - 13) AS to_table, a1.attname AS column, a2.attname AS primary_key, c.conname AS name, c.confupdtype AS on_update, c.confdeltype AS on_delete, c.convalidated AS valid, c.condeferrable AS deferrable, c.condeferred AS deferred\n"
                        + "FROM pg_constraint c\n"
                        + "JOIN pg_class t1 ON c.conrelid = t1.oid\n"
                        + "JOIN pg_class t2 ON c.confrelid = t2.oid\n"
                        + "JOIN pg_attribute a1 ON a1.attnum = c.conkey[1] AND a1.attrelid = t1.oid\n"
                        + "JOIN pg_attribute a2 ON a2.attnum = c.confkey[1] AND a2.attrelid = t2.oid\n"
                        + "JOIN pg_namespace t3 ON c.connamespace = t3.oid\n"
                        + "WHERE c.contype = 'f'\n"
                        + "  AND t1.relname = '%s'\n"
                        + "  AND t3.nspname  IN ('public')\n"
                        + "ORDER BY c.conname\n",
                    table)),
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
  }
}
