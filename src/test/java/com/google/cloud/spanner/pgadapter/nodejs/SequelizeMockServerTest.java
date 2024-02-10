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

package com.google.cloud.spanner.pgadapter.nodejs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Duration;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(NodeJSTest.class)
@RunWith(JUnit4.class)
public class SequelizeMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void installDependencies() throws IOException, InterruptedException {
    NodeJSTest.installDependencies("sequelize-tests");

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with pg_range as (\n"
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
                    + "  select 700 as oid, 'float4' as typname, 11 as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 1021 as typarray, 'float4in' as typinput, 'float4out' as typoutput, 'float4recv' as typreceive, 'float4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
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
                    + "  select 1021 as oid, '_float4' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 700 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
                    + "  select 1022 as oid, '_float8' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 701 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'double precision[]' as spanner_type union all\n"
                    + "  select 1015 as oid, '_varchar' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 1043 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'character varying[]' as spanner_type union all\n"
                    + "  select 1182 as oid, '_date' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 1082 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'date[]' as spanner_type union all\n"
                    + "  select 1115 as oid, '_timestamp' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 1114 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
                    + "  select 1185 as oid, '_timestamptz' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 1184 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'timestamp with time zone[]' as spanner_type union all\n"
                    + "  select 1231 as oid, '_numeric' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 1700 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'numeric[]' as spanner_type union all\n"
                    + "  select 3807 as oid, '_jsonb' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 3802 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'jsonb[]' as spanner_type union all\n"
                    + "  select 705 as oid, 'unknown' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'p' as typtype, 'X' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, '0'::varchar as typrelid, 0 as typelem, 0 as typarray, 'unknownin' as typinput, 'unknownout' as typoutput, 'unknownrecv' as typreceive, 'unknownsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'c' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type\n"
                    + "),\n"
                    + " ranges AS (  SELECT pg_range.rngtypid, pg_type.typname AS rngtypname,         pg_type.typarray AS rngtyparray, pg_range.rngsubtype    FROM pg_range LEFT OUTER JOIN pg_type ON pg_type.oid = pg_range.rngtypid)SELECT pg_type.typname, pg_type.typtype, pg_type.oid, pg_type.typarray,       ranges.rngtypname, ranges.rngtypid, ranges.rngtyparray  FROM pg_type LEFT OUTER JOIN ranges ON pg_type.oid = ranges.rngsubtype WHERE (pg_type.typtype IN('b', 'e'))"),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(
                            TypeCode.STRING,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.BOOL)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("SELECT 1+1 AS result"), SELECT2_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("SELECT * FROM users"),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING),
                        ImmutableList.of("id", "name")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .build())
                .build()));
  }

  private String getHost() {
    return "localhost";
  }

  @Test
  public void testSelectUsers() throws Exception {
    String sql = "SELECT * FROM users";

    String output = runTest("testSelectUsers", getHost(), pgServer.getLocalPort());

    assertEquals("Users: 1,Alice\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
    assertFalse(request.getTransaction().getSingleUse().getReadOnly().hasStrong());
    assertTrue(request.getTransaction().getSingleUse().getReadOnly().hasMaxStaleness());
    assertEquals(
        request.getTransaction().getSingleUse().getReadOnly().getMaxStaleness(),
        Duration.newBuilder().setSeconds(15L).build());
  }

  @Test
  public void testSelectUsersInTransaction() throws Exception {
    String sql = "SELECT * FROM users";

    String output = runTest("testSelectUsersInTransaction", getHost(), pgServer.getLocalPort());

    assertEquals("Users: 1,Alice\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  static String runTest(String testName, String host, int port)
      throws IOException, InterruptedException {
    return NodeJSTest.runTest("sequelize-tests", testName, host, port, "db");
  }
}
