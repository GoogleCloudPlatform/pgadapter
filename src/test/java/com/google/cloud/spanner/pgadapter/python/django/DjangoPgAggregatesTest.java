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

package com.google.cloud.spanner.pgadapter.python.django;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(PythonTest.class)
public class DjangoPgAggregatesTest extends DjangoTestSetup {

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {"/tmp"});
  }

  private static ResultSet createResultSet(List<String> rows, int numCols) {
    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    StructType.Builder builder = StructType.newBuilder();

    for (int i = 0; i < 2 * numCols; i += 2) {
      String colType = rows.get(i);
      if (colType.equals("int")) {
        builder.addFields(
            Field.newBuilder()
                .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                .setName(rows.get(i + 1))
                .build());

      } else if (colType.equals("string")) {
        builder.addFields(
            Field.newBuilder()
                .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                .setName(rows.get(i + 1))
                .build());
      } else if (colType.equals("array")) {
        builder.addFields(
            Field.newBuilder()
                .setType(
                    Type.newBuilder()
                        .setCode(TypeCode.ARRAY)
                        .setArrayElementType(Type.newBuilder().setCode(TypeCode.STRING))
                        .build())
                .setName(rows.get(i + 1))
                .build());
      }
    }
    resultSetBuilder.setMetadata(
        ResultSetMetadata.newBuilder().setRowType(builder.build()).build());
    for (int i = 2 * numCols; i < rows.size(); i += numCols) {
      ListValue.Builder rowBuilder = ListValue.newBuilder();
      for (int j = i; j < i + numCols; ++j) {
        if (rows.get(j).charAt(0) == '[') {
          String[] array = rows.get(j).substring(1, rows.get(j).length() - 1).split(",");

          ListValue.Builder listValueBuilder = ListValue.newBuilder();
          for (String arrElement : array) {
            listValueBuilder.addValues(Value.newBuilder().setStringValue(arrElement).build());
          }
          Value value = Value.newBuilder().setListValue(listValueBuilder.build()).build();
          rowBuilder.addValues(value);
        } else rowBuilder.addValues(Value.newBuilder().setStringValue(rows.get(j)).build());
      }
      resultSetBuilder.addRows(rowBuilder.build());
    }
    return resultSetBuilder.build();
  }

  @BeforeClass
  public static void mockUnnecessarySql() {
    String unnecessarySql =
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
            + "SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore'";

    String unnecessarySql2 =
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
            + "SELECT typarray FROM pg_type WHERE typname = 'citext'";

    List<String> result1 = new ArrayList<>();
    result1.add("string");
    result1.add("some_column");

    ResultSet rs1 = createResultSet(result1, 1);

    mockSpanner.putStatementResult(StatementResult.query(Statement.of(unnecessarySql), rs1));
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(unnecessarySql2), rs1));
  }

  @Test
  public void testStrAgg() throws IOException, InterruptedException {

    String sql =
        "SELECT \"singers\".\"firstname\", STRING_AGG(\"singers\".\"firstname\", '|' ) AS \"str\" FROM \"singers\" GROUP BY \"singers\".\"firstname\" LIMIT 21";

    List<String> result = new ArrayList<>();

    result.add("string");
    result.add("firstname");

    result.add("string");
    result.add("str");

    result.add("hello");
    result.add("hello|world");

    result.add("world");
    result.add("world|hello");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 2)));

    List<String> options = new ArrayList<>();
    options.add("string_agg");

    String actualOutput = executePgAggregateTests(pgServer.getLocalPort(), host, options);
    String expectedOutput =
        "<QuerySet [{'firstname': 'hello', 'str': 'hello|world'}, {'firstname': 'world', 'str': 'world|hello'}]>\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2).getSql());
  }

  @Test
  public void testArrAgg() throws IOException, InterruptedException {

    String sql = "SELECT ARRAY_AGG(\"singers\".\"firstname\" ) AS \"arr\" FROM \"singers\"";

    List<String> result = new ArrayList<>();

    result.add("array");
    result.add("arr");

    result.add("[hello,beautiful,world,]");
    ResultSet rs = createResultSet(result, 2);

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 1)));

    List<String> options = new ArrayList<>();
    options.add("arr_agg");

    String actualOutput = executePgAggregateTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "{'arr': ['hello', 'beautiful', 'world']}\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2).getSql());
  }
}
