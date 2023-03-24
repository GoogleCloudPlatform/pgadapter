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

import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.util.Collections;
import org.junit.BeforeClass;
import org.postgresql.core.Oid;

public class AbstractRubyMockServerTest extends AbstractMockServerTest {

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
  // 'int2', 'int4', 'int8', 'oid', 'float4', 'float8', 'numeric', 'bool', 'timestamp',
  // 'timestamptz'

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
              + "AND c.relname = 'schema_migrations' "
              + "AND c.relkind IN ('r','p')");
  protected static final Statement SELECT_MIGRATIONS_TABLE_EXTENDED =
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
              + "AND c.relname = 'schema_migrations' "
              + "AND c.relkind IN ('r','v','m','p','f')");
  protected static final ResultSetMetadata SELECT_TABLE_METADATA =
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
              + "AND c.relname = 'ar_internal_metadata' "
              + "AND c.relkind IN ('r','p')");

  protected static final Statement SELECT_SCHEMA_MIGRATIONS_COLUMNS =
      Statement.of(
          "with "
              + PG_TYPE_PREFIX
              + "\n"
              + "SELECT a.attname, format_type(a.atttypid, a.atttypmod),\n"
              + "       pg_get_expr(d.adbin, d.adrelid), a.attnotnull, a.atttypid, a.atttypmod,\n"
              + "       c.collname, col_description(a.attrelid, a.attnum) AS comment,\n"
              + "       attgenerated as attgenerated\n"
              + "  FROM pg_attribute a\n"
              + "  LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum\n"
              + "  LEFT JOIN pg_type t ON a.atttypid = t.oid\n"
              + "  LEFT JOIN pg_collation c ON a.attcollation = c.oid AND a.attcollation <> t.typcollation\n"
              + " WHERE a.attrelid = '\"schema_migrations\"'::regclass\n"
              + "   AND a.attnum > 0 AND NOT a.attisdropped\n"
              + " ORDER BY a.attnum\n");
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
      Statement.of(
          "with "
              + PG_TYPE_PREFIX
              + "\n"
              + "SELECT a.attname, format_type(a.atttypid, a.atttypmod),\n"
              + "       pg_get_expr(d.adbin, d.adrelid), a.attnotnull, a.atttypid, a.atttypmod,\n"
              + "       c.collname, col_description(a.attrelid, a.attnum) AS comment,\n"
              + "       attgenerated as attgenerated\n"
              + "  FROM pg_attribute a\n"
              + "  LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum\n"
              + "  LEFT JOIN pg_type t ON a.atttypid = t.oid\n"
              + "  LEFT JOIN pg_collation c ON a.attcollation = c.oid AND a.attcollation <> t.typcollation\n"
              + " WHERE a.attrelid = '\"ar_internal_metadata\"'::regclass\n"
              + "   AND a.attnum > 0 AND NOT a.attisdropped\n"
              + " ORDER BY a.attnum\n");

  protected static final ResultSet SELECT_AR_INTERNAL_METADATA_COLUMNS_RESULTSET =
      ResultSet.newBuilder()
          .setMetadata(SELECT_COLUMNS_METADATA)
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("key").build())
                  .addValues(Value.newBuilder().setStringValue("character varying").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setBoolValue(true).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                  .addValues(Value.newBuilder().setStringValue("-1").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("value").build())
                  .addValues(Value.newBuilder().setStringValue("character varying").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setBoolValue(true).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                  .addValues(Value.newBuilder().setStringValue("-1").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("created_at").build())
                  .addValues(Value.newBuilder().setStringValue("character varying").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setBoolValue(true).build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMPTZ)).build())
                  .addValues(Value.newBuilder().setStringValue("-1").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("updated_at").build())
                  .addValues(Value.newBuilder().setStringValue("character varying").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setBoolValue(true).build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMPTZ)).build())
                  .addValues(Value.newBuilder().setStringValue("-1").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .addValues(Value.newBuilder().setStringValue("").build())
                  .build())
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
          "with pg_enum as (\n"
              + "select * from (select 0::bigint as oid, 0::bigint as enumtypid, 0.0::float8 as enumsortorder, ''::varchar as enumlabel\n"
              + ") e where false),\n"
              + PG_TYPE_PREFIX
              + "\nSELECT\n"
              + "  type.typname AS name,\n"
              + "  string_agg(enum.enumlabel, ',' ORDER BY enum.enumsortorder) AS value\n"
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

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        new MockSpannerServiceImpl(),
        "d",
        Collections.singleton("-ddl=AutocommitExplicitTransaction"));
  }
}
