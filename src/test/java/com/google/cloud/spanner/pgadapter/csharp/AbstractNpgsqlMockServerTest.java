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

package com.google.cloud.spanner.pgadapter.csharp;

import static org.junit.Assert.assertEquals;

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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;
import org.junit.BeforeClass;
import org.postgresql.core.Oid;

public abstract class AbstractNpgsqlMockServerTest extends AbstractMockServerTest {
  private static final Statement SELECT_VERSION = Statement.of("SELECT version()");
  private static final ResultSetMetadata SELECT_VERSION_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("version")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet SELECT_VERSION_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("PostgreSQL 14.1").build())
                  .build())
          .setMetadata(SELECT_VERSION_METADATA)
          .build();

  private static final Statement SELECT_TYPES_14_1 =
      Statement.of(
          "with pg_range as (\n"
              + "select * from (select 0::bigint as rngtypid, 0::bigint as rngsubtype, 0::bigint as rngmultitypid, 0::bigint as rngcollation, 0::bigint as rngsubopc, ''::varchar as rngcanonical, ''::varchar as rngsubdiff\n"
              + ") range where false),\n"
              + PG_TYPE_PREFIX
              + ",\n"
              + EMULATED_PG_CLASS_PREFIX
              + ",\n"
              + "pg_proc as (\n"
              + "select * from (select 0::bigint as oid, ''::varchar as proname, 0::bigint as pronamespace, 0::bigint as proowner, 0::bigint as prolang, 0.0::float8 as procost, 0.0::float8 as prorows, 0::bigint as provariadic, ''::varchar as prosupport, ''::varchar as prokind, false::bool as prosecdef, false::bool as proleakproof, false::bool as proisstrict, false::bool as proretset, ''::varchar as provolatile, ''::varchar as proparallel, 0::bigint as pronargs, 0::bigint as pronargdefaults, 0::bigint as prorettype, 0::bigint as proargtypes, '{}'::bigint[] as proallargtypes, '{}'::varchar[] as proargmodes, '{}'::text[] as proargnames, ''::varchar as proargdefaults, '{}'::bigint[] as protrftypes, ''::text as prosrc, ''::text as probin, ''::varchar as prosqlbody, '{}'::text[] as proconfig, '{}'::bigint[] as proacl\n"
              + ") proc where false)\n"
              + "SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid\n"
              + "FROM (\n"
              + "    -- Arrays have typtype=b - this subquery identifies them by their typreceive and converts their typtype to a\n"
              + "    -- We first do this for the type (innerest-most subquery), and then for its element type\n"
              + "    -- This also returns the array element, range subtype and domain base type as elemtypoid\n"
              + "    SELECT\n"
              + "        typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,\n"
              + "        elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,\n"
              + "        CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype\n"
              + "    FROM (\n"
              + "        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,\n"
              + "            CASE WHEN substr(typ.typname, 1, 1)='_' THEN 'a' ELSE typ.typtype END AS typtype,\n"
              + "            CASE\n"
              + "                WHEN substr(typ.typname, 1, 1)='_' THEN typ.typelem\n"
              + "                WHEN typ.typtype='r' THEN rngsubtype\n"
              + "                WHEN typ.typtype='m' THEN (SELECT rngtypid FROM pg_range WHERE rngmultitypid = typ.oid)\n"
              + "                WHEN typ.typtype='d' THEN typ.typbasetype\n"
              + "            END AS elemtypoid\n"
              + "        FROM pg_type AS typ\n"
              + "        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)\n"
              + "        LEFT JOIN pg_proc AS proc ON false\n"
              + "        LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)\n"
              + "    ) AS typ\n"
              + "    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid\n"
              + "    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)\n"
              + "    LEFT JOIN pg_proc AS elemproc ON false\n"
              + ") AS t\n"
              + "JOIN pg_namespace AS ns ON (ns.oid = typnamespace)\n"
              + "WHERE\n"
              + "    typtype IN ('b', 'r', 'm', 'e', 'd') OR -- Base, range, multirange, enum, domain\n"
              + "    (typtype = 'c' AND relkind='c') OR -- User-defined free-standing composites (not table composites) by default\n"
              + "    (typtype = 'p' AND typname IN ('record', 'void', 'unknown')) OR -- Some special supported pseudo-types\n"
              + "    (typtype = 'a' AND (  -- Array of...\n"
              + "        elemtyptype IN ('b', 'r', 'm', 'e', 'd') OR -- Array of base, range, multirange, enum, domain\n"
              + "        (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR -- Arrays of special supported pseudo-types\n"
              + "        (elemtyptype = 'c' AND elemrelkind='c') -- Array of user-defined free-standing composites (not table composites) by default\n"
              + "    ))\n"
              + "ORDER BY CASE\n"
              + "       WHEN typtype IN ('b', 'e', 'p') THEN 0           -- First base types, enums, pseudo-types\n"
              + "       WHEN typtype = 'r' THEN 1                        -- Ranges after\n"
              + "       WHEN typtype = 'm' THEN 2                        -- Multiranges after\n"
              + "       WHEN typtype = 'c' THEN 3                        -- Composites after\n"
              + "       WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 4 -- Domains over non-arrays after\n"
              + "       WHEN typtype = 'a' THEN 5                        -- Arrays after\n"
              + "       WHEN typtype = 'd' AND elemtyptype = 'a' THEN 6  -- Domains over arrays last\n"
              + "END");
  // ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
  private static final ResultSetMetadata SELECT_TYPES_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("nspname")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
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
                          .setName("typtype")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("typnotnull")
                          .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("elemtypoid")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .build())
          .build();

  // npgsql assumes that the following types are *always* present.
  //  _int16Handler = new Int16Handler(PgType("smallint"));
  //  _int32Handler = new Int32Handler(PgType("integer"));
  //  _int64Handler = new Int64Handler(PgType("bigint"));
  //  _doubleHandler = new DoubleHandler(PgType("double precision"));
  //  _numericHandler = new NumericHandler(PgType("numeric"));
  //  _textHandler ??= new TextHandler(PgType("text"), _connector.TextEncoding);
  //  _timestampHandler ??= new TimestampHandler(PgType("timestamp without time zone"));
  //  _timestampTzHandler ??= new TimestampTzHandler(PgType("timestamp with time zone"));
  //  _dateHandler ??= new DateHandler(PgType("date"));
  //  _boolHandler ??= new BoolHandler(PgType("boolean"));
  //
  // pg_catalog	21	int2	b	false
  private static final com.google.spanner.v1.ResultSet SELECT_TYPES_RESULTSET =
      ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT2)).build())
                  .addValues(Value.newBuilder().setStringValue("int2").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT4)).build())
                  .addValues(Value.newBuilder().setStringValue("int4").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT8)).build())
                  .addValues(Value.newBuilder().setStringValue("int8").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT4)).build())
                  .addValues(Value.newBuilder().setStringValue("float4").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT8)).build())
                  .addValues(Value.newBuilder().setStringValue("float8").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.NUMERIC)).build())
                  .addValues(Value.newBuilder().setStringValue("numeric").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.TEXT)).build())
                  .addValues(Value.newBuilder().setStringValue("text").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                  .addValues(Value.newBuilder().setStringValue("varchar").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.JSONB)).build())
                  .addValues(Value.newBuilder().setStringValue("jsonb").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMP)).build())
                  .addValues(Value.newBuilder().setStringValue("timestamp").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMPTZ)).build())
                  .addValues(Value.newBuilder().setStringValue("timestamptz").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.DATE)).build())
                  .addValues(Value.newBuilder().setStringValue("date").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.BOOL)).build())
                  .addValues(Value.newBuilder().setStringValue("bool").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.BYTEA)).build())
                  .addValues(Value.newBuilder().setStringValue("bytea").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.INT2_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_int2").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT2)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.INT4_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_int4").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT4)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.INT8_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_int8").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.INT8)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT4_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_float4").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT4)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT8_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_float8").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.FLOAT8)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.NUMERIC_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_numeric").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.NUMERIC)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TEXT_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_text").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.TEXT)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_varchar").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.VARCHAR)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.JSONB_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_jsonb").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.JSONB)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder()
                          .setStringValue(String.valueOf(Oid.TIMESTAMP_ARRAY))
                          .build())
                  .addValues(Value.newBuilder().setStringValue("_timestamp").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMP)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder()
                          .setStringValue(String.valueOf(Oid.TIMESTAMPTZ_ARRAY))
                          .build())
                  .addValues(Value.newBuilder().setStringValue("_timestamptz").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.TIMESTAMPTZ)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.DATE_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_date").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.DATE)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.BOOL_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_bool").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.BOOL)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(
                      Value.newBuilder().setStringValue(String.valueOf(Oid.BYTEA_ARRAY)).build())
                  .addValues(Value.newBuilder().setStringValue("_bytea").build())
                  .addValues(Value.newBuilder().setStringValue("a").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue(String.valueOf(Oid.BYTEA)).build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("pg_catalog").build())
                  .addValues(Value.newBuilder().setStringValue("705").build())
                  .addValues(Value.newBuilder().setStringValue("unknown").build())
                  .addValues(Value.newBuilder().setStringValue("b").build())
                  .addValues(Value.newBuilder().setBoolValue(false).build())
                  .addValues(Value.newBuilder().setStringValue("705").build())
                  .build())
          .setMetadata(SELECT_TYPES_METADATA)
          .build();
  private static final Statement SELECT_ATTRIBUTES =
      Statement.of(
          "with "
              + PG_TYPE_PREFIX
              + ",\n"
              + EMULATED_PG_CLASS_PREFIX
              + ",\n"
              + EMULATED_PG_ATTRIBUTE_PREFIX
              + "\n"
              + "-- Load field definitions for (free-standing) composite types\n"
              + "SELECT typ.oid, att.attname, att.atttypid\n"
              + "FROM pg_type AS typ\n"
              + "JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)\n"
              + "JOIN pg_class AS cls ON (cls.oid = typ.typrelid)\n"
              + "JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)\n"
              + "WHERE\n"
              + "  (typ.typtype = 'c' AND cls.relkind='c') AND\n"
              + "  attnum > 0 AND     -- Don't load system attributes\n"
              + "  NOT attisdropped\n"
              + "ORDER BY typ.oid, att.attnum");
  private static final ResultSetMetadata SELECT_ATTRIBUTES_METADATA =
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
                          .setName("attname")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("atttypid")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet SELECT_ATTRIBUTES_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder().setMetadata(SELECT_ATTRIBUTES_METADATA).build();

  private static final Statement SELECT_ENUM_LABELS_STATEMENT =
      Statement.of(
          "with pg_enum as (\n"
              + "select * from (select 0::bigint as oid, 0::bigint as enumtypid, 0.0::float8 as enumsortorder, ''::varchar as enumlabel\n"
              + ") e where false),\n"
              + PG_TYPE_PREFIX
              + "\n-- Load enum fields\n"
              + "SELECT pg_type.oid, enumlabel\n"
              + "FROM pg_enum\n"
              + "JOIN pg_type ON pg_type.oid=enumtypid\n"
              + "ORDER BY oid, enumsortorder");
  private static final ResultSetMetadata SELECT_ENUM_LABELS_METADATA =
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
                          .setName("enumlabel")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet SELECT_ENUM_LABELS_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder().setMetadata(SELECT_ENUM_LABELS_METADATA).build();

  @BeforeClass
  public static void setupResults() {
    mockSpanner.putStatementResult(StatementResult.query(SELECT_VERSION, SELECT_VERSION_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_TYPES_14_1, SELECT_TYPES_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_ATTRIBUTES, SELECT_ATTRIBUTES_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_ENUM_LABELS_STATEMENT, SELECT_ENUM_LABELS_RESULTSET));
  }

  static String execute(String test, String connectionString)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    String[] runCommand = new String[] {"dotnet", "run", test, connectionString};
    builder.command(runCommand);
    builder.directory(new File("./src/test/csharp/pgadapter_npgsql_tests/npgsql_tests"));
    Process process = builder.start();
    String output = readAll(process.getInputStream());
    String errors = readAll(process.getErrorStream());
    int result = process.waitFor();
    assertEquals(errors, 0, result);

    return output;
  }

  static String readAll(InputStream inputStream) {
    StringBuilder result = new StringBuilder();
    try (Scanner scanner = new Scanner(new InputStreamReader(inputStream))) {
      while (scanner.hasNextLine()) {
        result.append(scanner.nextLine()).append("\n");
      }
    }
    return result.toString();
  }
}
