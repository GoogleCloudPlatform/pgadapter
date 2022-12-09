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

package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.admin.database.v1.MockDatabaseAdminImpl;
import com.google.cloud.spanner.admin.instance.v1.MockInstanceAdminImpl;
import com.google.cloud.spanner.connection.SpannerPool;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.SpannerGrpc;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeAnnotationCode;
import com.google.spanner.v1.TypeCode;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.postgresql.core.Oid;
import org.postgresql.util.PGobject;

/**
 * Abstract base class for tests that verify that PgAdapter is receiving wire protocol requests
 * correctly, translates these correctly to Spanner RPC invocations, and correctly translates the
 * RPC invocations back to wire protocol responses. The test starts two in-process servers for this
 * purpose: 1. An in-process {@link MockSpannerServiceImpl}. The mock server implements the entire
 * gRPC API of Cloud Spanner, but does not contain an actual query engine or any other
 * implementation. Instead, all query and DML statements that the client will be executing must
 * first be registered as mock results on the server. This makes the mock server dialect agnostic
 * and usable for both normal Spanner requests and Spangres requests. Note that this also means that
 * the server does NOT verify that the SQL statement is correct and valid for the specific dialect.
 * It only verifies that the statement corresponds with one of the previously registered statements
 * on the server. 2. An in-process PgAdapter {@link ProxyServer} that connects to the
 * above-mentioned mock Spanner server. The in-process PgAdapter server listens on a random local
 * port, and tests can use the client of their choosing to connect to the {@link ProxyServer}. The
 * requests are translated by the proxy into RPC invocations on the mock Spanner server, and the
 * responses from the mock Spanner server are translated into wire protocol responses to the client.
 * The tests can then inspect the requests that the mock Spanner server received to verify that the
 * server received the requests that the test expected.
 */
public abstract class AbstractMockServerTest {
  private static final Logger logger = Logger.getLogger(AbstractMockServerTest.class.getName());

  protected static final Statement SELECT_JSONB_TYPE_BY_OID =
      Statement.newBuilder(
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
                  + "SELECT substring(typname, 1, 1)='_' as is_array, typtype, typname, pg_type.oid   FROM pg_type   LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select 1 as r, 'public' as nspname ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  WHERE pg_type.oid = $1  ORDER BY sp.r, pg_type.oid DESC")
          .bind("p1")
          .to(Oid.JSONB)
          .build();
  protected static final ResultSet SELECT_JSONB_TYPE_BY_OID_RESULT_SET =
      ResultSet.newBuilder()
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
                                  .setName("typname")
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
                  .addValues(Value.newBuilder().setStringValue("jsonb").build())
                  .addValues(Value.newBuilder().setStringValue("3802").build())
                  .build())
          .build();
  protected static final Statement SELECT_JSONB_TYPE_BY_NAME =
      Statement.newBuilder(
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
                  + "SELECT pg_type.oid, typname   FROM pg_type   LEFT   JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select 1 as r, 'public' as nspname ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  WHERE typname = $1  ORDER BY sp.r, pg_type.oid DESC LIMIT 1")
          .bind("p1")
          .to("jsonb")
          .build();
  protected static final Statement SELECT_JSONB_TYPE_BY_NAME_SIMPLE_PROTOCOL =
      Statement.of(
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
              + "SELECT pg_type.oid, typname   FROM pg_type   LEFT   JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select 1 as r, 'public' as nspname ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  WHERE typname = 'jsonb'  ORDER BY sp.r, pg_type.oid DESC LIMIT 1");

  protected static final ResultSet SELECT_JSONB_TYPE_BY_NAME_RESULT_SET =
      ResultSet.newBuilder()
          .setMetadata(
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
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("3802").build())
                  .addValues(Value.newBuilder().setStringValue("jsonb").build())
                  .build())
          .build();
  protected static final Statement SELECT_JSONB_TYPE_INFO =
      Statement.newBuilder(
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
                  + "SELECT n.nspname = 'public', n.nspname, t.typname FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = $1")
          .bind("p1")
          .to(Oid.JSONB)
          .build();
  protected static final ResultSet SELECT_JSONB_TYPE_INFO_RESULT_SET =
      ResultSet.newBuilder()
          .setMetadata(
              ResultSetMetadata.newBuilder()
                  .setRowType(
                      StructType.newBuilder()
                          .addFields(
                              Field.newBuilder()
                                  .setName("")
                                  .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                  .build())
                          .addFields(
                              Field.newBuilder()
                                  .setName("nspname")
                                  .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                  .build())
                          .addFields(
                              Field.newBuilder()
                                  .setName("typname")
                                  .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                  .build())
                          .build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setBoolValue(true).build())
                  .addValues(Value.newBuilder().setStringValue("public").build())
                  .addValues(Value.newBuilder().setStringValue("jsonb").build())
                  .build())
          .build();

  protected static final Statement SELECT1 = Statement.of("SELECT 1");
  protected static final Statement SELECT2 = Statement.of("SELECT 2");
  protected static final Statement SELECT_FIVE_ROWS =
      Statement.of("SELECT * FROM TableWithFiveRows");
  protected static final Statement INVALID_SELECT = Statement.of("SELECT foo");
  private static final ResultSetMetadata SELECT1_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("C")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .build())
          .build();
  protected static final com.google.spanner.v1.ResultSet EMPTY_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder().setMetadata(SELECT1_METADATA).build();
  protected static final com.google.spanner.v1.ResultSet SELECT1_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("1").build())
                  .build())
          .setMetadata(SELECT1_METADATA)
          .build();
  protected static final com.google.spanner.v1.ResultSet SELECT2_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("2").build())
                  .build())
          .setMetadata(SELECT1_METADATA)
          .build();

  private static final com.google.spanner.v1.ResultSet SELECT_FIVE_ROWS_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addAllRows(
              ImmutableList.of(
                  ListValue.newBuilder()
                      .addValues(Value.newBuilder().setStringValue("1").build())
                      .build(),
                  ListValue.newBuilder()
                      .addValues(Value.newBuilder().setStringValue("2").build())
                      .build(),
                  ListValue.newBuilder()
                      .addValues(Value.newBuilder().setStringValue("3").build())
                      .build(),
                  ListValue.newBuilder()
                      .addValues(Value.newBuilder().setStringValue("4").build())
                      .build(),
                  ListValue.newBuilder()
                      .addValues(Value.newBuilder().setStringValue("5").build())
                      .build()))
          .setMetadata(SELECT1_METADATA)
          .build();
  protected static final Statement UPDATE_STATEMENT =
      Statement.of("UPDATE FOO SET BAR=1 WHERE BAZ=2");
  protected static final int UPDATE_COUNT = 2;
  protected static final Statement INSERT_STATEMENT = Statement.of("INSERT INTO FOO VALUES (1)");
  protected static final int INSERT_COUNT = 1;
  protected static final Statement INVALID_DML = Statement.of("INSERT INTO FOO VALUES ('abc')");
  protected static final Statement INVALID_DDL = Statement.of("CREATE TABLE FOO (id int64)");

  protected static final ResultSetMetadata ALL_TYPES_METADATA = createAllTypesResultSetMetadata("");
  protected static final com.google.spanner.v1.ResultSet ALL_TYPES_RESULTSET =
      createAllTypesResultSet("");
  protected static final com.google.spanner.v1.ResultSet ALL_TYPES_NULLS_RESULTSET =
      createAllTypesNullResultSet("");

  protected static final StatusRuntimeException EXCEPTION =
      Status.INVALID_ARGUMENT.withDescription("Statement is invalid.").asRuntimeException();

  protected static ResultSet createAllTypesResultSet(String columnPrefix) {
    return com.google.spanner.v1.ResultSet.newBuilder()
        .setMetadata(createAllTypesResultSetMetadata(columnPrefix))
        .addRows(
            ListValue.newBuilder()
                .addValues(Value.newBuilder().setStringValue("1").build())
                .addValues(Value.newBuilder().setBoolValue(true).build())
                .addValues(
                    Value.newBuilder()
                        .setStringValue(
                            Base64.getEncoder()
                                .encodeToString("test".getBytes(StandardCharsets.UTF_8)))
                        .build())
                .addValues(Value.newBuilder().setNumberValue(3.14d).build())
                .addValues(Value.newBuilder().setStringValue("100").build())
                .addValues(Value.newBuilder().setStringValue("6.626").build())
                .addValues(
                    Value.newBuilder().setStringValue("2022-02-16T13:18:02.123456789Z").build())
                .addValues(Value.newBuilder().setStringValue("2022-03-29").build())
                .addValues(Value.newBuilder().setStringValue("test").build())
                .addValues(Value.newBuilder().setStringValue("{\"key\": \"value\"}").build())
                .build())
        .build();
  }

  protected static ResultSet createAllTypesNullResultSet(String columnPrefix) {
    return com.google.spanner.v1.ResultSet.newBuilder()
        .setMetadata(createAllTypesResultSetMetadata(columnPrefix))
        .addRows(
            ListValue.newBuilder()
                .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .build())
        .build();
  }

  protected static ResultSetMetadata createAllTypesResultSetMetadata(String columnPrefix) {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setName(columnPrefix + "col_bigint")
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build()))
                .addFields(
                    Field.newBuilder()
                        .setName(columnPrefix + "col_bool")
                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build()))
                .addFields(
                    Field.newBuilder()
                        .setName(columnPrefix + "col_bytea")
                        .setType(Type.newBuilder().setCode(TypeCode.BYTES).build()))
                .addFields(
                    Field.newBuilder()
                        .setName(columnPrefix + "col_float8")
                        .setType(Type.newBuilder().setCode(TypeCode.FLOAT64).build()))
                .addFields(
                    Field.newBuilder()
                        .setName(columnPrefix + "col_int")
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build()))
                .addFields(
                    Field.newBuilder()
                        .setName(columnPrefix + "col_numeric")
                        .setType(
                            Type.newBuilder()
                                .setCode(TypeCode.NUMERIC)
                                .setTypeAnnotation(TypeAnnotationCode.PG_NUMERIC)
                                .build()))
                .addFields(
                    Field.newBuilder()
                        .setName(columnPrefix + "col_timestamptz")
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build()))
                .addFields(
                    Field.newBuilder()
                        .setName(columnPrefix + "col_date")
                        .setType(Type.newBuilder().setCode(TypeCode.DATE).build()))
                .addFields(
                    Field.newBuilder()
                        .setName(columnPrefix + "col_varchar")
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build()))
                .addFields(
                    Field.newBuilder()
                        .setName(columnPrefix + "col_jsonb")
                        .setType(
                            Type.newBuilder()
                                .setCode(TypeCode.JSON)
                                .setTypeAnnotation(TypeAnnotationCode.PG_JSONB)
                                .build()))
                .build())
        .build();
  }

  protected static MockSpannerServiceImpl mockSpanner;
  protected static MockDatabaseAdminImpl mockDatabaseAdmin;
  protected static MockInstanceAdminImpl mockInstanceAdmin;
  protected static Server spannerServer;
  protected static ProxyServer pgServer;

  protected List<WireMessage> getWireMessages() {
    return new ArrayList<>(pgServer.getDebugMessages());
  }

  protected <T extends WireMessage> List<T> getWireMessagesOfType(Class<T> type) {
    return pgServer.getDebugMessages().stream()
        .filter(msg -> msg.getClass().equals(type))
        .map(msg -> (T) msg)
        .collect(Collectors.toList());
  }

  private static TypeAnnotationCode getTypeAnnotationCode(TypeCode type) {
    switch (type) {
      case NUMERIC:
        return TypeAnnotationCode.PG_NUMERIC;
      case JSON:
        return TypeAnnotationCode.PG_JSONB;
    }
    return TypeAnnotationCode.TYPE_ANNOTATION_CODE_UNSPECIFIED;
  }

  protected static ResultSet createResultSetWithOnlyMetadata(ImmutableList<TypeCode> types) {
    return ResultSet.newBuilder().setMetadata(createMetadata(types)).build();
  }

  protected static ResultSetMetadata createMetadata(ImmutableList<TypeCode> types) {
    StructType.Builder builder = StructType.newBuilder();
    for (int index = 0; index < types.size(); index++) {
      builder.addFields(
          Field.newBuilder()
              .setType(
                  Type.newBuilder()
                      .setCode(types.get(index))
                      .setTypeAnnotation(getTypeAnnotationCode(types.get(index)))
                      .build())
              .setName("")
              .build());
    }
    return ResultSetMetadata.newBuilder().setRowType(builder.build()).build();
  }

  protected static ResultSetMetadata createParameterTypesMetadata(ImmutableList<TypeCode> types) {
    StructType.Builder builder = StructType.newBuilder();
    for (int index = 0; index < types.size(); index++) {
      builder.addFields(
          Field.newBuilder()
              .setType(
                  Type.newBuilder()
                      .setCode(types.get(index))
                      .setTypeAnnotation(getTypeAnnotationCode(types.get(index)))
                      .build())
              .setName("p" + (index + 1))
              .build());
    }
    return ResultSetMetadata.newBuilder().setUndeclaredParameters(builder.build()).build();
  }

  protected static ResultSetMetadata createMetadata(
      ImmutableList<TypeCode> types, ImmutableList<String> names) {
    Preconditions.checkArgument(
        types.size() == names.size(), "Types and names must have same length");
    StructType.Builder builder = StructType.newBuilder();
    for (int index = 0; index < types.size(); index++) {
      builder.addFields(
          Field.newBuilder()
              .setType(
                  Type.newBuilder()
                      .setCode(types.get(index))
                      .setTypeAnnotation(getTypeAnnotationCode(types.get(index)))
                      .build())
              .setName(names.get(index))
              .build());
    }
    return ResultSetMetadata.newBuilder().setRowType(builder.build()).build();
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        new MockSpannerServiceImpl(), "d", Collections.emptyList());
  }

  protected static void doStartMockSpannerAndPgAdapterServers(
      String defaultDatabase, Iterable<String> extraPGAdapterOptions) throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        new MockSpannerServiceImpl(), defaultDatabase, extraPGAdapterOptions);
  }

  public static PGobject createJdbcPgJsonbObject(String value) throws SQLException {
    PGobject result = new PGobject();
    result.setValue(value);
    result.setType("jsonb");
    return result;
  }

  protected static void doStartMockSpannerAndPgAdapterServers(
      MockSpannerServiceImpl mockSpannerService,
      String defaultDatabase,
      Iterable<String> extraPGAdapterOptions)
      throws Exception {
    mockSpanner = mockSpannerService;
    mockSpanner.setAbortProbability(0.0D); // We don't want any unpredictable aborted transactions.
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_JSONB_TYPE_BY_OID, SELECT_JSONB_TYPE_BY_OID_RESULT_SET));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_JSONB_TYPE_BY_NAME, SELECT_JSONB_TYPE_BY_NAME_RESULT_SET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_JSONB_TYPE_BY_NAME_SIMPLE_PROTOCOL, SELECT_JSONB_TYPE_BY_NAME_RESULT_SET));
    mockSpannerService.putStatementResult(
        StatementResult.query(SELECT_JSONB_TYPE_INFO, SELECT_JSONB_TYPE_INFO_RESULT_SET));
    mockSpanner.putStatementResult(StatementResult.query(SELECT1, SELECT1_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.query(SELECT2, SELECT2_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_FIVE_ROWS, SELECT_FIVE_ROWS_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.update(UPDATE_STATEMENT, UPDATE_COUNT));
    mockSpanner.putStatementResult(StatementResult.update(INSERT_STATEMENT, INSERT_COUNT));
    mockSpanner.putStatementResult(
        MockSpannerServiceImpl.StatementResult.detectDialectResult(Dialect.POSTGRESQL));
    mockSpanner.putStatementResult(StatementResult.exception(INVALID_SELECT, EXCEPTION));
    mockSpanner.putStatementResult(StatementResult.exception(INVALID_DML, EXCEPTION));
    mockSpanner.putStatementResult(StatementResult.exception(INVALID_DDL, EXCEPTION));

    mockDatabaseAdmin = new MockDatabaseAdminImpl();
    mockInstanceAdmin = new MockInstanceAdminImpl();

    InetSocketAddress address = new InetSocketAddress("localhost", 0);
    spannerServer =
        NettyServerBuilder.forAddress(address)
            .addService(mockSpanner)
            .addService(mockDatabaseAdmin)
            .addService(mockInstanceAdmin)
            .intercept(
                new ServerInterceptor() {
                  @Override
                  public <ReqT, RespT> Listener<ReqT> interceptCall(
                      ServerCall<ReqT, RespT> serverCall,
                      Metadata metadata,
                      ServerCallHandler<ReqT, RespT> serverCallHandler) {

                    if (SpannerGrpc.getExecuteStreamingSqlMethod()
                        .getFullMethodName()
                        .equals(serverCall.getMethodDescriptor().getFullMethodName())) {
                      String userAgent =
                          metadata.get(
                              Metadata.Key.of(
                                  "x-goog-api-client", Metadata.ASCII_STRING_MARSHALLER));
                      assertNotNull(userAgent);
                      assertTrue(userAgent.contains("pg-adapter"));
                    }
                    return Contexts.interceptCall(
                        Context.current(), serverCall, metadata, serverCallHandler);
                  }
                })
            .build()
            .start();

    ImmutableList.Builder<String> argsListBuilder =
        ImmutableList.<String>builder().add("-p", "p", "-i", "i");
    if (defaultDatabase != null) {
      argsListBuilder.add("-d", defaultDatabase);
    }
    argsListBuilder.add(
        "-debug",
        "-c",
        "", // empty credentials file, as we are using a plain text connection.
        "-s",
        "0", // port 0 to let the OS pick an available port
        "-e",
        String.format("localhost:%d", spannerServer.getPort()),
        "-r",
        "usePlainText=true;");
    argsListBuilder.addAll(extraPGAdapterOptions);
    String[] args = argsListBuilder.build().toArray(new String[0]);
    pgServer = new ProxyServer(new OptionsMetadata(args));
    pgServer.startServer();
  }

  @AfterClass
  public static void stopMockSpannerAndPgAdapterServers() throws Exception {
    if (pgServer != null) {
      try {
        pgServer.stopServer();
      } catch (IllegalStateException exception) {
        logger.warning(
            String.format(
                "Ignoring %s as this can happen if the server is sent multiple invalid messages",
                exception.getMessage()));
      }
    }
    try {
      SpannerPool.closeSpannerPool();
    } catch (SpannerException exception) {
      if (exception.getErrorCode() == ErrorCode.FAILED_PRECONDITION
          && exception
              .getMessage()
              .contains(
                  "connection(s) still open. Close all connections before calling closeSpanner()")) {
        // Ignore this exception for now. It is caused by the fact that the PgAdapter proxy server
        // is not gracefully shutting down all connections when the proxy is stopped, and it also
        // does not wait until any connections that have been requested to close, actually have
        // closed.
        logger.warning(String.format("Ignoring %s as this is expected", exception.getMessage()));
      } else {
        throw exception;
      }
    }
    if (spannerServer != null) {
      spannerServer.shutdown();
      spannerServer.awaitTermination(10L, TimeUnit.SECONDS);
    }
  }

  protected void closeSpannerPool() {
    closeSpannerPool(false);
  }

  /**
   * Closes all open Spanner instances in the pool. Use this to force the recreation of a Spanner
   * instance for a test case. This method will ignore any errors and retry if closing fails.
   */
  protected void closeSpannerPool(boolean ignoreException) {
    SpannerException exception = null;
    for (int attempt = 0; attempt < 1000; attempt++) {
      try {
        SpannerPool.closeSpannerPool();
        return;
      } catch (SpannerException e) {
        try {
          Thread.sleep(1L);
        } catch (InterruptedException interruptedException) {
          throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
        }
        exception = e;
      }
    }
    if (!ignoreException) {
      throw exception;
    }
  }

  @Before
  public void clearRequests() {
    mockSpanner.clearRequests();
    mockDatabaseAdmin.reset();
    mockInstanceAdmin.reset();
    if (pgServer != null) {
      pgServer.clearDebugMessages();
    }
  }

  protected void addDdlResponseToSpannerAdmin() {
    mockDatabaseAdmin.addResponse(
        Operation.newBuilder()
            .setDone(true)
            .setResponse(Any.pack(Empty.getDefaultInstance()))
            .setMetadata(Any.pack(UpdateDatabaseDdlMetadata.getDefaultInstance()))
            .build());
  }

  protected void addDdlExceptionToSpannerAdmin() {
    mockDatabaseAdmin.addException(
        Status.INVALID_ARGUMENT.withDescription("Statement is invalid.").asRuntimeException());
  }

  protected static void addIfNotExistsDdlException() {
    mockDatabaseAdmin.addException(
        Status.INVALID_ARGUMENT
            .withDescription("<IF NOT EXISTS> clause is not supported in <CREATE TABLE> statement.")
            .asRuntimeException());
  }
}
