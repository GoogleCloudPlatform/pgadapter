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

import static com.google.cloud.spanner.pgadapter.PgAdapterTestEnv.useFloat4InTests;
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
  static final String lf = System.lineSeparator();
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
  
  private static final String WINDOWS_SQL = "with pg_range as (\n"
      + "select * from (select 0::bigint as rngtypid, 0::bigint as rngsubtype, 0::bigint as rngmultitypid, 0::bigint as rngcollation, 0::bigint as rngsubopc, ''::varchar as rngcanonical, ''::varchar as rngsubdiff\n"
      + ") range where false),\n"
      + "pg_namespace as (\n"
      + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
      + "        schema_name as nspname, null as nspowner, null as nspacl\n"
      + "  from information_schema.schemata\n"
      + "),\n"
      + "pg_type as (\n"
      + "  select 16 as oid, 'bool' as typname, 11 as typnamespace, null as typowner, 1 as typlen, true as typbyval, 'b' as typtype, 'B' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1000 as typarray, 'boolin' as typinput, 'boolout' as typoutput, 'boolrecv' as typreceive, 'boolsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'c' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'boolean' as spanner_type union all\n"
      + "  select 17 as oid, 'bytea' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1001 as typarray, 'byteain' as typinput, 'byteaout' as typoutput, 'bytearecv' as typreceive, 'byteasend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'bytea' as spanner_type union all\n"
      + "  select 20 as oid, 'int8' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1016 as typarray, 'int8in' as typinput, 'int8out' as typoutput, 'int8recv' as typreceive, 'int8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'bigint' as spanner_type union all\n"
      + "  select 21 as oid, 'int2' as typname, 11 as typnamespace, null as typowner, 2 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1005 as typarray, 'int2in' as typinput, 'int2out' as typoutput, 'int2recv' as typreceive, 'int2send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 's' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
      + "  select 23 as oid, 'int4' as typname, 11 as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1007 as typarray, 'int4in' as typinput, 'int4out' as typoutput, 'int4recv' as typreceive, 'int4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
      + "  select 25 as oid, 'text' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1009 as typarray, 'textin' as typinput, 'textout' as typoutput, 'textrecv' as typreceive, 'textsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
      + "  select 700 as oid, 'float4' as typname, 11 as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1021 as typarray, 'float4in' as typinput, 'float4out' as typoutput, 'float4recv' as typreceive, 'float4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'real' as spanner_type union all\n"
      + "  select 701 as oid, 'float8' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1022 as typarray, 'float8in' as typinput, 'float8out' as typoutput, 'float8recv' as typreceive, 'float8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'double precision' as spanner_type union all\n"
      + "  select 1043 as oid, 'varchar' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1015 as typarray, 'varcharin' as typinput, 'varcharout' as typoutput, 'varcharrecv' as typreceive, 'varcharsend' as typsend, 'varchartypmodin' as typmodin, 'varchartypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'character varying' as spanner_type union all\n"
      + "  select 1082 as oid, 'date' as typname, 11 as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1182 as typarray, 'date_in' as typinput, 'date_out' as typoutput, 'date_recv' as typreceive, 'date_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'date' as spanner_type union all\n"
      + "  select 1114 as oid, 'timestamp' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1115 as typarray, 'timestamp_in' as typinput, 'timestamp_out' as typoutput, 'timestamp_recv' as typreceive, 'timestamp_send' as typsend, 'timestamptypmodin' as typmodin, 'timestamptypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
      + "  select 1184 as oid, 'timestamptz' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1185 as typarray, 'timestamptz_in' as typinput, 'timestamptz_out' as typoutput, 'timestamptz_recv' as typreceive, 'timestamptz_send' as typsend, 'timestamptztypmodin' as typmodin, 'timestamptztypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'timestamp with time zone' as spanner_type union all\n"
      + "  select 1700 as oid, 'numeric' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1231 as typarray, 'numeric_in' as typinput, 'numeric_out' as typoutput, 'numeric_recv' as typreceive, 'numeric_send' as typsend, 'numerictypmodin' as typmodin, 'numerictypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'm' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'numeric' as spanner_type union all\n"
      + "  select 3802 as oid, 'jsonb' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 3807 as typarray, 'jsonb_in' as typinput, 'jsonb_out' as typoutput, 'jsonb_recv' as typreceive, 'jsonb_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'jsonb' as spanner_type union all\n"
      + "  select 1000 as oid, '_bool' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 16 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'boolean[]' as spanner_type union all\n"
      + "  select 1001 as oid, '_bytea' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 17 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'bytea[]' as spanner_type union all\n"
      + "  select 1016 as oid, '_int8' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 20 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'bigint[]' as spanner_type union all\n"
      + "  select 1005 as oid, '_int2' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 21 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
      + "  select 1007 as oid, '_int4' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 23 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
      + "  select 1009 as oid, '_text' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 25 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
      + "  select 1021 as oid, '_float4' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 700 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'real[]' as spanner_type union all\n"
      + "  select 1022 as oid, '_float8' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 701 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'double precision[]' as spanner_type union all\n"
      + "  select 1015 as oid, '_varchar' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 1043 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'character varying[]' as spanner_type union all\n"
      + "  select 1182 as oid, '_date' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 1082 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'date[]' as spanner_type union all\n"
      + "  select 1115 as oid, '_timestamp' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 1114 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
      + "  select 1185 as oid, '_timestamptz' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 1184 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'timestamp with time zone[]' as spanner_type union all\n"
      + "  select 1231 as oid, '_numeric' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 1700 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'numeric[]' as spanner_type union all\n"
      + "  select 3807 as oid, '_jsonb' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 3802 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'jsonb[]' as spanner_type union all\n"
      + "  select 705 as oid, 'unknown' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'p' as typtype, 'X' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 0 as typarray, 'unknownin' as typinput, 'unknownout' as typoutput, 'unknownrecv' as typreceive, 'unknownsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'c' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type\n"
      + "),\n"
      + "pg_class as (\n"
      + "  select\n"
      + "  '''\"' || t.table_schema || '\".\"' || t.table_name || '\"''' as oid,\n"
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
      + "  CASE table_type WHEN 'BASE TABLE' THEN 'r' WHEN 'VIEW' THEN 'v' ELSE '' END as relkind,\n"
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
      + "group by t.table_name, t.table_schema, t.table_type\n"
      + "union all\n"
      + "select\n"
      + "    '''\"' || i.table_schema || '\".\"' || i.table_name || '\".\"' || i.index_name || '\"''' as oid,\n"
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
      + "    'i' as relkind,\n"
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
      + "inner join information_schema.index_columns using (table_catalog, table_schema, table_name, index_name)\n"
      + "group by i.index_name, i.table_name, i.table_schema\n"
      + "),\n"
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
      + "            CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,\n"
      + "            CASE\n"
      + "                WHEN proc.proname='array_recv' THEN typ.typelem\n"
      + "                WHEN typ.typtype='r' THEN rngsubtype\n"
      + "                WHEN typ.typtype='m' THEN (SELECT rngtypid FROM pg_range WHERE rngmultitypid = typ.oid)\n"
      + "                WHEN typ.typtype='d' THEN typ.typbasetype\n"
      + "            END AS elemtypoid\n"
      + "        FROM pg_type AS typ\n"
      + "        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)\n"
      + "        LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive\n"
      + "        LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)\n"
      + "    ) AS typ\n"
      + "    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid\n"
      + "    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)\n"
      + "    LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive\n"
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
      + "END";

  private static final Statement SELECT_TYPES_14_1 =
      Statement.of(
          "with pg_range as ("
              + lf
              + "select * from (select 0::bigint as rngtypid, 0::bigint as rngsubtype, 0::bigint as rngmultitypid, 0::bigint as rngcollation, 0::bigint as rngsubopc, ''::varchar as rngcanonical, ''::varchar as rngsubdiff"
              + lf
              + ") range where false),"
              + lf
              + PG_TYPE_PREFIX
              + ","
              + lf
              + EMULATED_PG_CLASS_PREFIX
              + ","
              + lf
              + "pg_proc as ("
              + lf
              + "select * from (select 0::bigint as oid, ''::varchar as proname, 0::bigint as pronamespace, 0::bigint as proowner, 0::bigint as prolang, 0.0::float8 as procost, 0.0::float8 as prorows, 0::bigint as provariadic, ''::varchar as prosupport, ''::varchar as prokind, false::bool as prosecdef, false::bool as proleakproof, false::bool as proisstrict, false::bool as proretset, ''::varchar as provolatile, ''::varchar as proparallel, 0::bigint as pronargs, 0::bigint as pronargdefaults, 0::bigint as prorettype, 0::bigint as proargtypes, '{}'::bigint[] as proallargtypes, '{}'::varchar[] as proargmodes, '{}'::text[] as proargnames, ''::varchar as proargdefaults, '{}'::bigint[] as protrftypes, ''::text as prosrc, ''::text as probin, ''::varchar as prosqlbody, '{}'::text[] as proconfig, '{}'::bigint[] as proacl"
              + lf
              + ") proc where false)"
              + lf
              + "SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid"
              + lf
              + "FROM ("
              + lf
              + "    -- Arrays have typtype=b - this subquery identifies them by their typreceive and converts their typtype to a"
              + lf
              + "    -- We first do this for the type (innerest-most subquery), and then for its element type"
              + lf
              + "    -- This also returns the array element, range subtype and domain base type as elemtypoid"
              + lf
              + "    SELECT"
              + lf
              + "        typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,"
              + lf
              + "        elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,"
              + lf
              + "        CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype"
              + lf
              + "    FROM ("
              + lf
              + "        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,"
              + lf
              + "            CASE WHEN substr(typ.typname, 1, 1)='_' THEN 'a' ELSE typ.typtype END AS typtype,"
              + lf
              + "            CASE"
              + lf
              + "                WHEN substr(typ.typname, 1, 1)='_' THEN typ.typelem"
              + lf
              + "                WHEN typ.typtype='r' THEN rngsubtype"
              + lf
              + "                WHEN typ.typtype='m' THEN (SELECT rngtypid FROM pg_range WHERE rngmultitypid = typ.oid)"
              + lf
              + "                WHEN typ.typtype='d' THEN typ.typbasetype"
              + lf
              + "            END AS elemtypoid"
              + lf
              + "        FROM pg_type AS typ"
              + lf
              + "        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)"
              + lf
              + "        LEFT JOIN pg_proc AS proc ON false"
              + lf
              + "        LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)"
              + lf
              + "    ) AS typ"
              + lf
              + "    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid"
              + lf
              + "    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)"
              + lf
              + "    LEFT JOIN pg_proc AS elemproc ON false"
              + lf
              + ") AS t"
              + lf
              + "JOIN pg_namespace AS ns ON (ns.oid = typnamespace)"
              + lf
              + "WHERE"
              + lf
              + "    typtype IN ('b', 'r', 'm', 'e', 'd') OR -- Base, range, multirange, enum, domain"
              + lf
              + "    (typtype = 'c' AND relkind='c') OR -- User-defined free-standing composites (not table composites) by default"
              + lf
              + "    (typtype = 'p' AND typname IN ('record', 'void', 'unknown')) OR -- Some special supported pseudo-types"
              + lf
              + "    (typtype = 'a' AND (  -- Array of..."
              + lf
              + "        elemtyptype IN ('b', 'r', 'm', 'e', 'd') OR -- Array of base, range, multirange, enum, domain"
              + lf
              + "        (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR -- Arrays of special supported pseudo-types"
              + lf
              + "        (elemtyptype = 'c' AND elemrelkind='c') -- Array of user-defined free-standing composites (not table composites) by default"
              + lf
              + "    ))"
              + lf
              + "ORDER BY CASE"
              + lf
              + "       WHEN typtype IN ('b', 'e', 'p') THEN 0           -- First base types, enums, pseudo-types"
              + lf
              + "       WHEN typtype = 'r' THEN 1                        -- Ranges after"
              + lf
              + "       WHEN typtype = 'm' THEN 2                        -- Multiranges after"
              + lf
              + "       WHEN typtype = 'c' THEN 3                        -- Composites after"
              + lf
              + "       WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 4 -- Domains over non-arrays after"
              + lf
              + "       WHEN typtype = 'a' THEN 5                        -- Arrays after"
              + lf
              + "       WHEN typtype = 'd' AND elemtyptype = 'a' THEN 6  -- Domains over arrays last"
              + lf
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
              + ","
              + lf
              + EMULATED_PG_CLASS_PREFIX
              + ","
              + lf
              + EMULATED_PG_ATTRIBUTE_PREFIX
              + ""
              + lf
              + "-- Load field definitions for (free-standing) composite types"
              + lf
              + "SELECT typ.oid, att.attname, att.atttypid"
              + lf
              + "FROM pg_type AS typ"
              + lf
              + "JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)"
              + lf
              + "JOIN pg_class AS cls ON (cls.oid = typ.typrelid)"
              + lf
              + "JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)"
              + lf
              + "WHERE"
              + lf
              + "  (typ.typtype = 'c' AND cls.relkind='c') AND"
              + lf
              + "  attnum > 0 AND     -- Don't load system attributes"
              + lf
              + "  NOT attisdropped"
              + lf
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
          "with pg_enum as ("
              + lf
              + "select * from (select 0::bigint as oid, 0::bigint as enumtypid, 0.0::float8 as enumsortorder, ''::varchar as enumlabel"
              + lf
              + ") e where false),"
              + lf
              + PG_TYPE_PREFIX
              + "\n-- Load enum fields"
              + lf
              + "SELECT pg_type.oid, enumlabel"
              + lf
              + "FROM pg_enum"
              + lf
              + "JOIN pg_type ON pg_type.oid=enumtypid"
              + lf
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
    builder
        .environment()
        .put("PGADAPTER_FLOAT4_OID", String.valueOf(useFloat4InTests() ? Oid.FLOAT4 : Oid.FLOAT8));
    Process process = builder.start();
    System.out.println("Executing " + test);
    System.out.println("Waiting for input");
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
        String line = scanner.nextLine();
        result.append(line).append(lf);
        System.out.println(line);
      }
    }
    return result.toString();
  }
}
