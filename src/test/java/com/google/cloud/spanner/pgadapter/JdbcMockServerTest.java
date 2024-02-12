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

import static com.google.cloud.spanner.pgadapter.statements.BackendConnection.TRANSACTION_ABORTED_ERROR;
import static com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgNamespace.PG_NAMESPACE_CTE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.EmptyPgEnum;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgAttrdef;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgAttribute;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgCollation;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgConstraint;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgExtension;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgIndex;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.PreparedType;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.GetDatabaseDdlResponse;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeAnnotationCode;
import com.google.spanner.v1.TypeCode;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.postgresql.PGConnection;
import org.postgresql.PGStatement;
import org.postgresql.core.Oid;
import org.postgresql.jdbc.PgStatement;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;

@RunWith(Parameterized.class)
public class JdbcMockServerTest extends AbstractMockServerTest {
  private static final int RANDOM_RESULTS_ROW_COUNT = 10;
  private static final Statement SELECT_RANDOM = Statement.of("select * from random_table");
  private static final ImmutableList<String> JDBC_STARTUP_STATEMENTS =
      ImmutableList.of(
          "SET extra_float_digits = 3", "SET application_name = 'PostgreSQL JDBC Driver'");

  @Parameter public String pgVersion;

  @Parameters(name = "pgVersion = {0}")
  public static Object[] data() {
    return new Object[] {"1.0", "14.1"};
  }

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    addRandomResultResults();
    setupJsonbResults();
  }

  private static void addRandomResultResults() {
    RandomResultSetGenerator generator =
        new RandomResultSetGenerator(RANDOM_RESULTS_ROW_COUNT, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(SELECT_RANDOM, generator.generate()));
  }

  static void setupJsonbResults() {
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

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default extended
   * mode for queries and DML statements.
   */
  private String createUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/?options=-c%%20server_version=%s",
        pgServer.getLocalPort(), pgVersion);
  }

  private String getExpectedInitialApplicationName() {
    return pgVersion.equals("1.0") ? "jdbc" : "PostgreSQL JDBC Driver";
  }

  @Test
  public void testQuery() throws SQLException {
    String sql = "SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    // The statement is only sent once to the mock server. The DescribePortal message will trigger
    // the execution of the query, and the result from that execution will be used for the Execute
    // message.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      assertEquals(sql, request.getSql());
      assertTrue(request.getTransaction().hasSingleUse());
      assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
    }
  }

  @Test
  public void testSelectHelloWorld() throws SQLException {
    String randomString =
        "╍➗⡢ⵄ⯣⺫␐Ⓔ⠊⓭∲Ⳋ⤄▹⡨⿄⦺⒢⠱\u2E5E⾀⭯⛧⫶⏵⽐⓮⻋⥍\u242A⫌⏎⎽⚚⒊ↄ⦛⹐⌣⸤ⳅ⼑╪␦⻛➯⃝⡥⨬⸺⇊⹐┪⍦╳◄⪷ⴺ⽾⣌⛛⬗⍘⧤⃰⩧⬔⇌⣸⮽❨⫘ⱶ⣗⤶⢽⚶⒪⁙♤✾✟⏩⟞\u20C5℈⺙ⵠ⋛✧⧬⯨➛⌁⻚ⰷ∑⼫⊅ⷛ";
    String sql = String.format("SELECT '%s'", randomString);
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(randomString).build())
                        .build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      for (int bufferSize : new int[] {0, 32}) {
        connection
            .createStatement()
            .execute(String.format("set spanner.string_conversion_buffer_size=%d", bufferSize));
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
          assertTrue(resultSet.next());
          assertEquals(randomString, resultSet.getString(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testJsonbBinary() throws SQLException {
    String randomString =
        "{\"key\": \"╍➗⡢ⵄ⯣⺫␐Ⓔ⠊⓭∲Ⳋ⤄▹⡨⿄⦺⒢⠱\u2E5E⾀⭯⛧⫶⏵⽐⓮⻋⥍\u242A⫌⏎⎽⚚⒊ↄ⦛⹐⌣⸤ⳅ⼑╪␦⻛➯⃝⡥⨬⸺⇊⹐┪⍦╳◄⪷ⴺ⽾⣌⛛⬗⍘⧤⃰⩧⬔⇌⣸⮽❨⫘ⱶ⣗⤶⢽⚶⒪⁙♤✾✟⏩⟞\u20C5℈⺙ⵠ⋛✧⧬⯨➛⌁⻚ⰷ∑⼫⊅ⷛ\"}";
    String sql = String.format("SELECT '%s'::jsonb", randomString);
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.JSON)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(randomString).build())
                        .build())
                .build()));

    String binaryTransfer = "&binaryTransferEnable=" + Oid.JSONB;
    try (Connection connection = DriverManager.getConnection(createUrl() + binaryTransfer)) {
      connection.unwrap(PGConnection.class).setPrepareThreshold(-1);
      for (int bufferSize : new int[] {0, 32}) {
        connection
            .createStatement()
            .execute(String.format("set spanner.string_conversion_buffer_size=%d", bufferSize));
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
          assertTrue(resultSet.next());
          assertNotNull(resultSet.getObject(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testGetCatalogs() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with pg_database as (\n"
                    + "  select 0::bigint as oid,\n"
                    + "         catalog_name as datname,\n"
                    + "         0::bigint as datdba,\n"
                    + "         6::bigint as encoding,\n"
                    + "         'c' as datlocprovider,\n"
                    + "         'C' as datcollate,\n"
                    + "         'C' as datctype,\n"
                    + "         false as datistemplate,\n"
                    + "         true as datallowconn,\n"
                    + "         -1::bigint as datconnlimit,\n"
                    + "         0::bigint as datlastsysoid,\n"
                    + "         0::bigint as datfrozenxid,\n"
                    + "         0::bigint as datminmxid,\n"
                    + "         0::bigint as dattablespace,\n"
                    + "         null as daticulocale,\n"
                    + "         null as daticurules,\n"
                    + "         null as datcollversion,\n"
                    + "         null as datacl  from information_schema.information_schema_catalog_name\n"
                    + ")\n"
                    + "SELECT datname AS TABLE_CAT FROM pg_database WHERE datallowconn = true ORDER BY datname"),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("TABLE_CAT")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("test-database").build())
                        .build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet catalogs = connection.getMetaData().getCatalogs()) {
        assertTrue(catalogs.next());
        assertEquals("test-database", catalogs.getString("TABLE_CAT"));
        assertFalse(catalogs.next());
      }
    }
  }

  @Test
  public void testStatementReturnGeneratedKeys() throws SQLException {
    String sql = "insert into test (id, value) values (1, 'One')";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql + "\nRETURNING *"),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING),
                        ImmutableList.of("id", "value")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("One").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl());
        java.sql.Statement statement = connection.createStatement()) {
      assertFalse(statement.execute(sql, java.sql.Statement.RETURN_GENERATED_KEYS));
      assertEquals(1, statement.getUpdateCount());
      try (ResultSet resultSet = statement.getGeneratedKeys()) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals("One", resultSet.getString(2));
      }
      assertFalse(statement.getMoreResults());
    }
  }

  @Test
  public void testStatementGetGeneratedKeys() throws SQLException {
    String sql = "insert into test (id, value) values (1, 'One')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 1L));

    try (Connection connection = DriverManager.getConnection(createUrl());
        java.sql.Statement statement = connection.createStatement()) {
      assertFalse(statement.execute(sql));
      assertEquals(1, statement.getUpdateCount());
      // This should return an empty result set.
      try (ResultSet resultSet = statement.getGeneratedKeys()) {
        assertFalse(resultSet.next());
      }
      assertFalse(statement.getMoreResults());
    }
  }

  @Test
  public void testStatementReturnGeneratedKeysForSelect() throws SQLException {
    String sql = "select * from test";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING),
                        ImmutableList.of("id", "value")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("One").build())
                        .build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl());
        java.sql.Statement statement = connection.createStatement()) {
      assertTrue(statement.execute(sql, java.sql.Statement.RETURN_GENERATED_KEYS));
      assertEquals(-1, statement.getUpdateCount());
      try (ResultSet resultSet = statement.getGeneratedKeys()) {
        assertFalse(resultSet.next());
      }
      assertFalse(statement.getMoreResults());
    }
  }

  @Test
  public void testStatementReturnColumnIndexes() throws SQLException {
    String sql = "insert into test (id, value) values (1, 'One')";
    try (Connection connection = DriverManager.getConnection(createUrl());
        java.sql.Statement statement = connection.createStatement()) {
      PSQLException exception =
          assertThrows(PSQLException.class, () -> statement.execute(sql, new int[] {1}));
      assertEquals(
          "Returning autogenerated keys by column index is not supported.", exception.getMessage());
    }
  }

  @Test
  public void testStatementReturnColumnNames() throws SQLException {
    String sql = "insert into test (id, value) values (1, 'One')";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql + "\nRETURNING \"id\", \"value\""),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING),
                        ImmutableList.of("id", "value")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("One").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl());
        java.sql.Statement statement = connection.createStatement()) {
      assertFalse(statement.execute(sql, new String[] {"id", "value"}));
      assertEquals(1, statement.getUpdateCount());
      try (ResultSet resultSet = statement.getGeneratedKeys()) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals("One", resultSet.getString(2));
      }
      assertFalse(statement.getMoreResults());
    }
  }

  @Test
  public void testPreparedStatementReturnGeneratedKeys() throws SQLException {
    String sql = "insert into test (id, value) values (1, 'One')";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql + "\nRETURNING *"),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING),
                        ImmutableList.of("id", "value")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("One").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl());
        PreparedStatement statement =
            connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
      assertFalse(statement.execute());
      assertEquals(1, statement.getUpdateCount());
      try (ResultSet resultSet = statement.getGeneratedKeys()) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals("One", resultSet.getString(2));
      }
      assertFalse(statement.getMoreResults());
    }
  }

  @Test
  public void testPreparedStatementReturnColumnIndexes() throws SQLException {
    String sql = "insert into test (id, value) values (1, 'One')";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      PSQLException exception =
          assertThrows(PSQLException.class, () -> connection.prepareStatement(sql, new int[] {1}));
      // Yes, this error message is a bit inconsistent.
      assertEquals("Returning autogenerated keys is not supported.", exception.getMessage());
    }
  }

  @Test
  public void testPreparedStatementReturnColumnNames() throws SQLException {
    String sql = "insert into test (id, value) values (1, 'One')";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql + "\nRETURNING \"id\", \"value\""),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING),
                        ImmutableList.of("id", "value")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("One").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl());
        PreparedStatement statement =
            connection.prepareStatement(sql, new String[] {"id", "value"})) {
      assertFalse(statement.execute(sql, new String[] {"id", "value"}));
      assertEquals(1, statement.getUpdateCount());
      try (ResultSet resultSet = statement.getGeneratedKeys()) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals("One", resultSet.getString(2));
      }
      assertFalse(statement.getMoreResults());
    }

    try (Connection connection = DriverManager.getConnection(createUrl());
        PreparedStatement statement =
            connection.prepareStatement(sql, new String[] {"id", "value"})) {
      assertEquals(1, statement.executeUpdate(sql, new String[] {"id", "value"}));
      try (ResultSet resultSet = statement.getGeneratedKeys()) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals("One", resultSet.getString(2));
      }
    }
  }

  @Test
  public void testPreparedStatementReturnColumnNamesForDmlWithReturningClause()
      throws SQLException {
    // A DML statement that already contains a returning clause is not modified by the PG JDBC
    // driver.
    String sql = "insert into test (id, value) values (1, 'One') returning *";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING),
                        ImmutableList.of("id", "value")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("One").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl());
        PreparedStatement statement =
            connection.prepareStatement(sql, new String[] {"id", "value"})) {
      assertFalse(statement.execute(sql, new String[] {"id", "value"}));
      // The result is returned as an update count, although the statement did include a returning
      // clause. This happens because the statement requested generated keys to be returned.
      assertEquals(1, statement.getUpdateCount());
      try (ResultSet resultSet = statement.getGeneratedKeys()) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals("One", resultSet.getString(2));
      }
      assertFalse(statement.getMoreResults());
    }

    try (Connection connection = DriverManager.getConnection(createUrl());
        PreparedStatement statement =
            connection.prepareStatement(sql, new String[] {"id", "value"})) {
      assertEquals(1, statement.executeUpdate(sql, new String[] {"id", "value"}));
      try (ResultSet resultSet = statement.getGeneratedKeys()) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals("One", resultSet.getString(2));
      }
    }
  }

  @Test
  public void testReturnGeneratedKeysForUpdate() throws SQLException {
    String sql = "update test set value='Two' where id=1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql + "\nRETURNING *"),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING),
                        ImmutableList.of("id", "value")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("Two").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl());
        PreparedStatement statement =
            connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
      assertFalse(statement.execute());
      assertEquals(1, statement.getUpdateCount());
      try (ResultSet resultSet = statement.getGeneratedKeys()) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals("Two", resultSet.getString(2));
      }
      assertFalse(statement.getMoreResults());
    }
  }

  @Test
  public void testReturnGeneratedKeysForDelete() throws SQLException {
    String sql = "delete from test where id=1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql + "\nRETURNING *"),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING),
                        ImmutableList.of("id", "value")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("One").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl());
        PreparedStatement statement =
            connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
      assertFalse(statement.execute());
      assertEquals(1, statement.getUpdateCount());
      try (ResultSet resultSet = statement.getGeneratedKeys()) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals("One", resultSet.getString(2));
      }
      assertFalse(statement.getMoreResults());
    }
  }

  @Test
  public void testShowApplicationName() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show application_name")) {
        assertTrue(resultSet.next());
        // If the PG version is 1.0, the JDBC driver thinks that the server does not support the
        // application_name property and does not send any value. That means that PGAdapter fills it
        // in automatically based on the client that is detected.
        // Otherwise, the JDBC driver includes its own name, and that is not overwritten by
        // PGAdapter.
        assertEquals(getExpectedInitialApplicationName(), resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowWellKnownClient() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            String.format("jdbc:postgresql://localhost:%d/", pgServer.getLocalPort()))) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.well_known_client")) {
        assertTrue(resultSet.next());
        assertEquals("JDBC", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testSetWellKnownClient() throws SQLException {
    for (String client : new String[] {"pgx", "npgsql", "sqlalchemy2"}) {
      try (Connection connection =
          DriverManager.getConnection(
              String.format(
                  "jdbc:postgresql://localhost:%d/?options=-c%%20spanner.well_known_client=%s",
                  pgServer.getLocalPort(), client))) {
        try (ResultSet resultSet =
            connection.createStatement().executeQuery("show spanner.well_known_client")) {
          assertTrue(resultSet.next());
          assertEquals(client.toUpperCase(), resultSet.getString(1));
          assertFalse(resultSet.next());
        }
        PSQLException exception =
            assertThrows(
                PSQLException.class,
                () -> connection.createStatement().execute("set spanner.well_known_client='foo'"));
        assertNotNull(exception.getServerErrorMessage());
        assertEquals(
            "parameter \"spanner.well_known_client\" cannot be set after connection start",
            exception.getServerErrorMessage().getMessage());
      }
    }
    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:postgresql://localhost:%d/?options=-c%%20spanner.well_known_client=%s",
                pgServer.getLocalPort(), "foo"))) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.well_known_client")) {
        assertTrue(resultSet.next());
        assertEquals("foo", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testPreparedStatementParameterMetadata() throws SQLException {
    String sql = "SELECT * FROM foo WHERE id=? or value=?";
    String pgSql = "SELECT * FROM foo WHERE id=$1 or value=$2";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("col1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("col2")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p2")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        ParameterMetaData parameters = preparedStatement.getParameterMetaData();
        assertEquals(2, parameters.getParameterCount());
        assertEquals(Types.BIGINT, parameters.getParameterType(1));
        assertEquals(Types.VARCHAR, parameters.getParameterType(2));
      }
    }
  }

  @Test
  public void testInvalidQuery() throws SQLException {
    String sql = "/ not a valid comment / SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      PSQLException exception =
          assertThrows(PSQLException.class, () -> connection.createStatement().executeQuery(sql));
      assertEquals(
          "ERROR: Unknown statement: / not a valid comment / SELECT 1", exception.getMessage());
    }

    // The statement is not sent to the mock server.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testClientSideStatementWithResultSet() throws SQLException {
    String sql = "show statement_timeout";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals("0", resultSet.getString("statement_timeout"));
        assertFalse(resultSet.next());
      }
      connection.createStatement().execute("set statement_timeout=6000");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals("6s", resultSet.getString("statement_timeout"));
        assertFalse(resultSet.next());
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testClientSideStatementWithoutResultSet() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        statement.execute("start batch dml");
        statement.execute(INSERT_STATEMENT.getSql());
        statement.execute(UPDATE_STATEMENT.getSql());
        statement.execute("run batch");
      }
    }
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), request.getStatements(1).getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testSelectCurrentSchema() throws SQLException {
    String sql = "SELECT current_schema";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals("public", resultSet.getString("current_schema"));
        assertFalse(resultSet.next());
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testSelectCurrentDatabase() throws SQLException {
    for (String sql :
        new String[] {
          "SELECT current_database()",
          "select current_database()",
          "select * from CURRENT_DATABASE()"
        }) {

      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
          assertTrue(resultSet.next());
          assertEquals("d", resultSet.getString("current_database"));
          assertFalse(resultSet.next());
        }
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testSelectCurrentCatalog() throws SQLException {
    for (String sql :
        new String[] {
          "SELECT current_catalog", "select current_catalog", "select * from CURRENT_CATALOG"
        }) {

      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
          assertTrue(resultSet.next());
          assertEquals("d", resultSet.getString("current_catalog"));
          assertFalse(resultSet.next());
        }
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testSelectVersion() throws SQLException {
    for (String sql :
        new String[] {"SELECT version()", "select version()", "select * from version()"}) {

      try (Connection connection = DriverManager.getConnection(createUrl())) {
        String version;
        try (ResultSet resultSet =
            connection.createStatement().executeQuery("show server_version")) {
          assertTrue(resultSet.next());
          version = resultSet.getString(1);
          assertFalse(resultSet.next());
        }
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
          assertTrue(resultSet.next());
          assertEquals("PostgreSQL " + version, resultSet.getString("version"));
          assertFalse(resultSet.next());
        }
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testShowSearchPath() throws SQLException {
    String sql = "show search_path";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals("public", resultSet.getString("search_path"));
        assertFalse(resultSet.next());
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testSetSearchPath() throws SQLException {
    String sql = "set search_path to public";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testShowServerVersion() throws SQLException {
    String sql = "show server_version";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(pgVersion, resultSet.getString("server_version"));
        assertFalse(resultSet.next());
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testQueryHint() throws SQLException {
    String sql = "/* @OPTIMIZER_VERSION=1 */ SELECT 1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(sql, executeRequest.getSql());
  }

  @Test
  public void testQueryWithParameters() throws SQLException {
    String jdbcSql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb "
            + "from all_types "
            + "where col_bigint=? "
            + "and col_bool=? "
            + "and col_bytea=? "
            + "and col_int=? "
            + "and col_float8=? "
            + "and col_numeric=? "
            + "and col_timestamptz=? "
            + "and col_date=? "
            + "and col_varchar=? "
            + "and col_jsonb=?";
    String pgSql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb "
            + "from all_types "
            + "where col_bigint=$1 "
            + "and col_bool=$2 "
            + "and col_bytea=$3 "
            + "and col_int=$4 "
            + "and col_float8=$5 "
            + "and col_numeric=$6 "
            + "and col_timestamptz=$7 "
            + "and col_date=$8 "
            + "and col_varchar=$9 "
            + "and col_jsonb=$10";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(pgSql), ALL_TYPES_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(pgSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test"))
                .bind("p4")
                .to(100)
                .bind("p5")
                .to(3.14d)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-02-16T13:18:02.123457000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-03-29"))
                .bind("p9")
                .to("test")
                .bind("p10")
                .to("{\"key\": \"value\"}")
                .build(),
            ALL_TYPES_RESULTSET));

    OffsetDateTime offsetDateTime =
        LocalDateTime.of(2022, 2, 16, 13, 18, 2, 123456789).atOffset(ZoneOffset.UTC);
    OffsetDateTime truncatedOffsetDateTime = offsetDateTime.truncatedTo(ChronoUnit.MICROS);

    // Threshold 5 is the default. Use a named prepared statement if it is executed 5 times or more.
    // Threshold 1 means always use a named prepared statement.
    // Threshold 0 means never use a named prepared statement.
    // Threshold -1 means use binary transfer of values and use DESCRIBE statement.
    // (10 points to you if you guessed the last one up front!).
    for (int preparedThreshold : new int[] {5, 1, 0, -1}) {
      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(jdbcSql)) {
          preparedStatement.unwrap(PgStatement.class).setPrepareThreshold(preparedThreshold);
          int index = 0;
          preparedStatement.setLong(++index, 1L);
          preparedStatement.setBoolean(++index, true);
          preparedStatement.setBytes(++index, "test".getBytes(StandardCharsets.UTF_8));
          preparedStatement.setInt(++index, 100);
          preparedStatement.setDouble(++index, 3.14d);
          preparedStatement.setBigDecimal(++index, new BigDecimal("6.626"));
          preparedStatement.setObject(++index, offsetDateTime);
          preparedStatement.setObject(++index, LocalDate.of(2022, 3, 29));
          preparedStatement.setString(++index, "test");
          preparedStatement.setString(++index, "{\"key\": \"value\"}");
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertTrue(resultSet.next());
            index = 0;
            assertEquals(1L, resultSet.getLong(++index));
            assertTrue(resultSet.getBoolean(++index));
            assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(++index));
            assertEquals(3.14d, resultSet.getDouble(++index), 0.0d);
            assertEquals(100, resultSet.getInt(++index));
            assertEquals(new BigDecimal("6.626"), resultSet.getBigDecimal(++index));
            assertEquals(
                truncatedOffsetDateTime, resultSet.getObject(++index, OffsetDateTime.class));
            assertEquals(LocalDate.of(2022, 3, 29), resultSet.getObject(++index, LocalDate.class));
            assertEquals("test", resultSet.getString(++index));
            assertEquals("{\"key\": \"value\"}", resultSet.getString(++index));

            for (int col = 1; col <= resultSet.getMetaData().getColumnCount(); col++) {
              assertNotNull(resultSet.getObject(col));
            }

            assertFalse(resultSet.next());
          }
        }
      }

      List<ExecuteSqlRequest> requests =
          mockSpanner.getRequests().stream()
              .filter(request -> request instanceof ExecuteSqlRequest)
              .map(request -> (ExecuteSqlRequest) request)
              .filter(request -> request.getSql().equals(pgSql))
              .collect(Collectors.toList());
      // Prepare threshold less than 0 means use binary transfer + DESCRIBE statement.
      assertEquals(preparedThreshold < 0 ? 2 : 1, requests.size());

      ExecuteSqlRequest executeRequest = requests.get(requests.size() - 1);
      assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
      assertEquals(pgSql, executeRequest.getSql());

      Map<String, Value> params = executeRequest.getParams().getFieldsMap();
      Map<String, Type> types = executeRequest.getParamTypesMap();

      assertEquals(TypeCode.INT64, types.get("p1").getCode());
      assertEquals("1", params.get("p1").getStringValue());
      assertEquals(TypeCode.BOOL, types.get("p2").getCode());
      assertTrue(params.get("p2").getBoolValue());
      assertEquals(TypeCode.BYTES, types.get("p3").getCode());
      assertEquals(
          Base64.getEncoder().encodeToString("test".getBytes(StandardCharsets.UTF_8)),
          params.get("p3").getStringValue());
      assertEquals(TypeCode.INT64, types.get("p4").getCode());
      assertEquals("100", params.get("p4").getStringValue());
      assertEquals(TypeCode.FLOAT64, types.get("p5").getCode());
      assertEquals(3.14d, params.get("p5").getNumberValue(), 0.0d);
      assertEquals(TypeCode.NUMERIC, types.get("p6").getCode());
      assertEquals(TypeAnnotationCode.PG_NUMERIC, types.get("p6").getTypeAnnotation());
      assertEquals("6.626", params.get("p6").getStringValue());
      assertEquals(TypeCode.TIMESTAMP, types.get("p7").getCode());
      assertEquals("2022-02-16T13:18:02.123457000Z", params.get("p7").getStringValue());
      assertEquals(TypeCode.DATE, types.get("p8").getCode());
      assertEquals("2022-03-29", params.get("p8").getStringValue());
      assertEquals(TypeCode.STRING, types.get("p9").getCode());
      assertEquals("test", params.get("p9").getStringValue());
      assertEquals("{\"key\": \"value\"}", params.get("p10").getStringValue());

      mockSpanner.clearRequests();
    }
  }

  @Test
  public void testQueryWithLegacyDateParameter() throws SQLException {
    String jdbcSql = "select col_date from all_types where col_date=?";
    String pgSql = "select col_date from all_types where col_date=$1";
    ResultSetMetadata metadata =
        ALL_TYPES_METADATA
            .toBuilder()
            .setUndeclaredParameters(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("p1")
                            .setType(Type.newBuilder().setCode(TypeCode.DATE).build())
                            .build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgSql), ALL_TYPES_RESULTSET.toBuilder().setMetadata(metadata).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(pgSql).bind("p1").to(Date.parseDate("2022-03-29")).build(),
            ALL_TYPES_RESULTSET));

    // Threshold 5 is the default. Use a named prepared statement if it is executed 5 times or more.
    // Threshold 1 means always use a named prepared statement.
    // Threshold 0 means never use a named prepared statement.
    // Threshold -1 means use binary transfer of values and use DESCRIBE statement.
    // (10 points to you if you guessed the last one up front!).
    for (int preparedThreshold : new int[] {5, 1, 0, -1}) {
      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(jdbcSql)) {
          preparedStatement.unwrap(PgStatement.class).setPrepareThreshold(preparedThreshold);
          int index = 0;
          preparedStatement.setDate(++index, new java.sql.Date(2022 - 1900, Calendar.MARCH, 29));
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertTrue(resultSet.next());
            assertEquals(
                new java.sql.Date(2022 - 1900, Calendar.MARCH, 29), resultSet.getDate("col_date"));
            assertFalse(resultSet.next());
          }
        }

        List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
        // Prepare threshold less than 0 means use binary transfer + DESCRIBE statement.
        // However, the legacy date type will never use BINARY transfer and will always be sent with
        // unspecified type by the JDBC driver the first time. This means that we need 2 round trips
        // in all cases, as the statement will either use an explicit DESCRIBE message, or it will
        // be auto-described by PGAdapter.
        int expectedRequestCount = 2;
        assertEquals(
            "Prepare threshold: " + preparedThreshold, expectedRequestCount, requests.size());

        ExecuteSqlRequest executeRequest = requests.get(requests.size() - 1);
        assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
        assertEquals(pgSql, executeRequest.getSql());

        Map<String, Value> params = executeRequest.getParams().getFieldsMap();
        Map<String, Type> types = executeRequest.getParamTypesMap();

        assertEquals(TypeCode.DATE, types.get("p1").getCode());
        assertEquals("2022-03-29", params.get("p1").getStringValue());

        mockSpanner.clearRequests();
      }
    }
  }

  @Test
  public void testCharParam() throws SQLException {
    String sql = "insert into foo values ($1)";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 1L));
    String jdbcSql = "insert into foo values (?)";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(jdbcSql)) {
        PGobject pgObject = new PGobject();
        pgObject.setType("char");
        pgObject.setValue("a");
        preparedStatement.setObject(1, pgObject);
        assertEquals(1, preparedStatement.executeUpdate());
      }
    }

    List<ParseMessage> parseMessages =
        pgServer.getDebugMessages().stream()
            .filter(message -> message instanceof ParseMessage)
            .map(message -> (ParseMessage) message)
            .collect(Collectors.toList());
    assertFalse(parseMessages.isEmpty());
    ParseMessage parseMessage = parseMessages.get(parseMessages.size() - 1);
    assertEquals(1, parseMessage.getStatement().getGivenParameterDataTypes().length);
    assertEquals(Oid.CHAR, parseMessage.getStatement().getGivenParameterDataTypes()[0]);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    // Oid.CHAR is not a recognized type in PGAdapter.
    assertEquals(0, request.getParamTypesCount());
    assertEquals(1, request.getParams().getFieldsCount());
    assertEquals("a", request.getParams().getFieldsMap().get("p1").getStringValue());
  }

  @Test
  public void testAutoDescribedStatementsAreReused() throws SQLException {
    String jdbcSql = "select col_date from all_types where col_date=?";
    String pgSql = "select col_date from all_types where col_date=$1";
    ResultSetMetadata metadata =
        ALL_TYPES_METADATA
            .toBuilder()
            .setUndeclaredParameters(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("p1")
                            .setType(Type.newBuilder().setCode(TypeCode.DATE).build())
                            .build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgSql), ALL_TYPES_RESULTSET.toBuilder().setMetadata(metadata).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(pgSql).bind("p1").to(Date.parseDate("2022-03-29")).build(),
            ALL_TYPES_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      for (int attempt : new int[] {1, 2}) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(jdbcSql)) {
          // Threshold 0 means never use a named prepared statement.
          preparedStatement.unwrap(PgStatement.class).setPrepareThreshold(0);
          preparedStatement.setDate(1, new java.sql.Date(2022 - 1900, Calendar.MARCH, 29));
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertTrue(resultSet.next());
            assertEquals(
                new java.sql.Date(2022 - 1900, Calendar.MARCH, 29), resultSet.getDate("col_date"));
            assertFalse(resultSet.next());
          }
        }

        // The first time we execute this statement the number of requests should be 2, as the
        // statement is auto-described by the backend. The second time we execute the statement the
        // backend should reuse the result from the first auto-describe roundtrip.
        List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
        if (attempt == 1) {
          assertEquals(2, requests.size());
          ExecuteSqlRequest describeRequest = requests.get(0);
          assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
          assertEquals(pgSql, describeRequest.getSql());
          // Even though we are sending two requests to Cloud Spanner, we should not start a
          // transaction for these two statements, as the first statement is only used to describe
          // the parameters, and not to get any actual data.
          assertTrue(describeRequest.hasTransaction());
          assertTrue(describeRequest.getTransaction().hasSingleUse());
          assertTrue(describeRequest.getTransaction().getSingleUse().hasReadOnly());
        } else {
          assertEquals(1, requests.size());
        }

        ExecuteSqlRequest executeRequest = requests.get(requests.size() - 1);
        assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
        assertEquals(pgSql, executeRequest.getSql());
        assertTrue(executeRequest.hasTransaction());
        assertTrue(executeRequest.getTransaction().hasSingleUse());
        assertTrue(executeRequest.getTransaction().getSingleUse().hasReadOnly());

        Map<String, Value> params = executeRequest.getParams().getFieldsMap();
        Map<String, Type> types = executeRequest.getParamTypesMap();

        assertEquals(TypeCode.DATE, types.get("p1").getCode());
        assertEquals("2022-03-29", params.get("p1").getStringValue());

        assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
        assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));

        mockSpanner.clearRequests();
      }
    }
  }

  @Test
  public void testDescribeDdlStatement() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("create table foo (id bigint primary key, value varchar)")) {
        ParameterMetaData parameterMetaData = preparedStatement.getParameterMetaData();
        assertEquals(0, parameterMetaData.getParameterCount());
        assertNull(preparedStatement.getMetaData());
      }
    }
  }

  @Test
  public void testDescribeClientSideNoResultStatement() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement("start batch dml")) {
        ParameterMetaData parameterMetaData = preparedStatement.getParameterMetaData();
        assertEquals(0, parameterMetaData.getParameterCount());
        assertNull(preparedStatement.getMetaData());
      }
    }
  }

  @Test
  public void testDescribeClientSideResultSetStatement() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("show statement_timeout")) {
        SQLException exception =
            assertThrows(SQLException.class, preparedStatement::getParameterMetaData);
        assertEquals(
            "ERROR: ResultSetMetadata are available only for results that were returned from Cloud Spanner",
            exception.getMessage());
      }
    }
  }

  @Test(timeout = 60_000)
  public void testMultiplePreparedStatements() throws SQLException {
    // Execute more statements than there are sessions in the pool to verify that repeatedly
    // creating a prepared statement with the same name does not cause a session leak.
    final int numStatements = 1000;
    String sql = "SELECT 1";

    for (boolean autocommit : new boolean[] {true, false}) {
      try (Connection connection = DriverManager.getConnection(createUrl())) {
        // Verify that there's no session leak both in autocommit and transactional mode.
        connection.setAutoCommit(autocommit);
        // Force the use of prepared statements.
        connection.unwrap(PGConnection.class).setPrepareThreshold(-1);
        for (int i = 0; i < numStatements; i++) {
          try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
            assertTrue(resultSet.next());
            assertEquals(1L, resultSet.getLong(1));
            assertFalse(resultSet.next());
          }
        }
      }
    }
  }

  @Test
  public void testMultipleQueriesInTransaction() throws SQLException {
    String sql = "SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Use a read/write transaction to execute two queries.
      connection.setAutoCommit(false);
      // Force the use of prepared statements.
      connection.unwrap(PGConnection.class).setPrepareThreshold(-1);
      for (int i = 0; i < 2; i++) {
        // https://github.com/GoogleCloudPlatform/pgadapter/issues/278
        // This would return `ERROR: FAILED_PRECONDITION: This ResultSet is closed`
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testTransactionAbortedWithPreparedStatements() throws SQLException {
    String sql = "SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Use a read/write transaction to execute two queries.
      connection.setAutoCommit(false);
      // Force the use of prepared statements.
      connection.unwrap(PGConnection.class).setPrepareThreshold(-1);
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      assertEquals(1, connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql()));
      mockSpanner.abortAllTransactions();
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      connection.commit();
    }

    assertEquals(11, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
    assertEquals(SELECT1.getSql(), requests.get(1).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(1).getQueryMode());

    assertEquals(INSERT_STATEMENT.getSql(), requests.get(2).getSql());
    assertEquals(QueryMode.PLAN, requests.get(2).getQueryMode());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(3).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(3).getQueryMode());

    // This returns Aborted and initiates a retry.
    assertEquals(SELECT1.getSql(), requests.get(4).getSql());
    assertEquals(QueryMode.PLAN, requests.get(4).getQueryMode());

    // Start of retry.
    assertEquals(SELECT1.getSql(), requests.get(5).getSql());
    assertEquals(QueryMode.PLAN, requests.get(5).getQueryMode());
    assertEquals(SELECT1.getSql(), requests.get(6).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(6).getQueryMode());

    assertEquals(INSERT_STATEMENT.getSql(), requests.get(7).getSql());
    // This should be QueryMode.PLAN. See https://github.com/googleapis/java-spanner/issues/2009.
    assertEquals(QueryMode.PLAN, requests.get(7).getQueryMode());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(8).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(8).getQueryMode());

    assertEquals(SELECT1.getSql(), requests.get(9).getSql());
    assertEquals(QueryMode.PLAN, requests.get(9).getQueryMode());

    // End of retry.
    assertEquals(SELECT1.getSql(), requests.get(10).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(10).getQueryMode());
  }

  @Test
  public void testQueryWithNonExistingTable() throws SQLException {
    String sql = "select * from non_existing_table where id=?";
    String pgSql = "select * from non_existing_table where id=$1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.newBuilder(pgSql).bind("p1").to(1L).build(),
            Status.INVALID_ARGUMENT
                .withDescription("relation \"non_existing_table\" does not exist")
                .asRuntimeException()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        preparedStatement.setLong(1, 1L);
        PSQLException exception =
            assertThrows(PSQLException.class, preparedStatement::executeQuery);
        assertEquals(
            "ERROR: relation \"non_existing_table\" does not exist - Statement: 'select * from non_existing_table where id=$1'",
            exception.getMessage());
        assertEquals(SQLState.UndefinedTable.toString(), exception.getSQLState());
      }
    }

    // PGAdapter tries to execute the query directly when describing the portal, so we receive one
    // ExecuteSqlRequest in normal execute mode.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(pgSql, requests.get(0).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(0).getQueryMode());
  }

  @Test
  public void testDmlWithNonExistingTable() throws SQLException {
    String sql = "update non_existing_table set value=? where id=?";
    String pgSql = "update non_existing_table set value=$1 where id=$2";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.newBuilder(pgSql).bind("p1").to("foo").bind("p2").to(1L).build(),
            Status.NOT_FOUND
                .withDescription("Table non_existing_table not found")
                .asRuntimeException()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        preparedStatement.setString(1, "foo");
        preparedStatement.setLong(2, 1L);
        SQLException exception = assertThrows(SQLException.class, preparedStatement::executeUpdate);
        assertEquals("ERROR: Table non_existing_table not found", exception.getMessage());
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(pgSql, requests.get(0).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(0).getQueryMode());
  }

  @Test
  public void testNullValues() throws SQLException {
    String pgSql =
        "insert into all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, "
            + "col_array_bigint, col_array_bool, col_array_bytea, col_array_float8, col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)";
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgSql)
                .bind("p1")
                .to(2L)
                .bind("p2")
                .to((Boolean) null)
                .bind("p3")
                .to((ByteArray) null)
                .bind("p4")
                .to((Double) null)
                .bind("p5")
                .to((Long) null)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric(null))
                .bind("p7")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p8")
                .to((Date) null)
                .bind("p9")
                .to((String) null)
                .bind("p10")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p11")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p12")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p13")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p14")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p15")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p16")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p17")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p18")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p19")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p20")
                .to((com.google.cloud.spanner.Value) null)
                .build(),
            1L));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from all_types where col_bigint is null"),
            ALL_TYPES_NULLS_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "insert into all_types "
                  + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, "
                  + "col_array_bigint, col_array_bool, col_array_bytea, col_array_float8, col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb) "
                  + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
        int index = 0;
        statement.setLong(++index, 2);
        statement.setNull(++index, Types.BOOLEAN);
        statement.setNull(++index, Types.BINARY);
        statement.setNull(++index, Types.DOUBLE);
        statement.setNull(++index, Types.INTEGER);
        statement.setNull(++index, Types.NUMERIC);
        statement.setNull(++index, Types.TIMESTAMP_WITH_TIMEZONE);
        statement.setNull(++index, Types.DATE);
        statement.setNull(++index, Types.VARCHAR);
        statement.setNull(++index, Types.OTHER);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);

        assertEquals(1, statement.executeUpdate());
      }

      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("select * from all_types where col_bigint is null")) {
        assertTrue(resultSet.next());

        int index = 0;
        // Note: JDBC returns the zero-value for primitive types if the value is NULL, and you have
        // to call wasNull() to determine whether the value was NULL or zero.
        assertEquals(0L, resultSet.getLong(++index));
        assertTrue(resultSet.wasNull());
        assertFalse(resultSet.getBoolean(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getBytes(++index));
        assertTrue(resultSet.wasNull());
        assertEquals(0d, resultSet.getDouble(++index), 0.0d);
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getBigDecimal(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getTimestamp(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getDate(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getString(++index));
        assertTrue(resultSet.wasNull());

        assertFalse(resultSet.next());
      }
    }
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testDescribeQueryWithNonExistingTable() throws SQLException {
    String sql = "select * from non_existing_table where id=$1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.NOT_FOUND
                .withDescription("Table non_existing_table not found")
                .asRuntimeException()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        SQLException exception =
            assertThrows(SQLException.class, preparedStatement::getParameterMetaData);
        assertEquals(
            "ERROR: Table non_existing_table not found - Statement: 'select * from non_existing_table where id=$1'",
            exception.getMessage());
      }
    }

    // We only receive one ExecuteSql request, as PGAdapter tries to describe the portal first.
    // As that statement fails, it does not try to describe the parameters in the statement, but
    // just returns the error from the DescribePortal statement.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(sql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
  }

  @Test
  public void testDescribeDmlWithNonExistingTable() throws SQLException {
    String sql = "update non_existing_table set value=$2 where id=$1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.NOT_FOUND
                .withDescription("Table non_existing_table not found")
                .asRuntimeException()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        SQLException exception =
            assertThrows(SQLException.class, preparedStatement::getParameterMetaData);
        assertEquals("ERROR: Table non_existing_table not found", exception.getMessage());
      }
    }

    // We receive one ExecuteSql requests:
    // 1. DescribeStatement (parameters). This statement fails as the table does not exist.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(sql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
  }

  @Test
  public void testDescribeDmlWithSchemaPrefix() throws SQLException {
    String sql = "update public.my_table set value=? where id=?";
    String pgSql = "update public.my_table set value=$1 where id=$2";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        ParameterMetaData metadata = preparedStatement.getParameterMetaData();
        assertEquals(Types.VARCHAR, metadata.getParameterType(1));
        assertEquals(Types.BIGINT, metadata.getParameterType(2));
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(pgSql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
  }

  @Test
  public void testDescribeDmlWithQuotedSchemaPrefix() throws SQLException {
    String sql = "update \"public\".\"my_table\" set value=? where id=?";
    String pgSql = "update \"public\".\"my_table\" set value=$1 where id=$2";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        ParameterMetaData metadata = preparedStatement.getParameterMetaData();
        assertEquals(Types.VARCHAR, metadata.getParameterType(1));
        assertEquals(Types.BIGINT, metadata.getParameterType(2));
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(pgSql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
  }

  @Test
  public void testTwoDmlStatements() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // The PG JDBC driver will internally split the following SQL string into two statements and
        // execute these sequentially. We still get the results back as if they were executed as one
        // batch on the same statement.
        assertFalse(statement.execute(String.format("%s;%s;", INSERT_STATEMENT, UPDATE_STATEMENT)));

        // Note that we have sent two DML statements to the database in one string. These should be
        // treated as separate statements, and there should therefore be two results coming back
        // from the server. That is; The first update count should be 1 (the INSERT), and the second
        // should be 2 (the UPDATE).
        assertEquals(1, statement.getUpdateCount());

        // The following is a prime example of how not to design an API, but this is how JDBC works.
        // getMoreResults() returns true if the next result is a ResultSet. However, if the next
        // result is an update count, it returns false, and we have to check getUpdateCount() to
        // verify whether there were any more results.
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());

        // There are no more results. This is indicated by getMoreResults returning false AND
        // getUpdateCount returning -1.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    // The DML statements are split by the JDBC driver and sent as separate statements to PgAdapter.
    // The Sync message is however sent after the second DML statement, which means that PGAdapter
    // is able to batch these together into one ExecuteBatchDml statement.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), request.getStatements(1).getSql());
  }

  @Test
  public void testTwoDmlStatements_withError() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception =
            assertThrows(
                SQLException.class,
                () -> statement.execute(String.format("%s;%s;", INSERT_STATEMENT, INVALID_DML)));
        assertEquals("ERROR: Statement is invalid.", exception.getMessage());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(INVALID_DML.getSql(), request.getStatements(1).getSql());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testTwoDmlStatements_randomlyAborted() throws SQLException {
    mockSpanner.setAbortProbability(0.5);
    for (int run = 0; run < 50; run++) {
      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (java.sql.Statement statement = connection.createStatement()) {
          assertFalse(
              statement.execute(String.format("%s;%s;", INSERT_STATEMENT, UPDATE_STATEMENT)));
          assertEquals(1, statement.getUpdateCount());
          assertFalse(statement.getMoreResults());
          assertEquals(2, statement.getUpdateCount());
          assertFalse(statement.getMoreResults());
          assertEquals(-1, statement.getUpdateCount());
        }
      } finally {
        mockSpanner.setAbortProbability(0.0);
      }
    }
  }

  @Test
  public void testJdbcBatch() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        statement.addBatch(INSERT_STATEMENT.getSql());
        statement.addBatch(UPDATE_STATEMENT.getSql());
        int[] updateCounts = statement.executeBatch();

        assertEquals(2, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(2, updateCounts[1]);
      }
    }

    // The PostgreSQL JDBC driver will send the DML statements as separated statements to PG, but it
    // will only send a Sync after the second statement. This means that PGAdapter is able to batch
    // these together in one ExecuteBatchDml request.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), request.getStatements(1).getSql());
  }

  @Test
  public void testJdbcPreparedStatementBatch() throws SQLException {
    String pgRewrittenInsertSql = "insert into my_table (id, value) values ($1, $2),($3, $4)";

    String insertSql = "insert into my_table (id, value) values (?, ?)";
    String pgInsertSql = "insert into my_table (id, value) values ($1, $2)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgInsertSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgRewrittenInsertSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64, TypeCode.STRING, TypeCode.INT64, TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgInsertSql).bind("p1").to(1L).bind("p2").to("One").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgInsertSql).bind("p1").to(2L).bind("p2").to("Two").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgRewrittenInsertSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to("One")
                .bind("p3")
                .to(2L)
                .bind("p4")
                .to("Two")
                .build(),
            2L));
    String selectSql = "select value from my_table where id=?";
    String pgSelectSql = "select value from my_table where id=$1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(pgSelectSql).bind("p1").to(1L).build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("One").build())
                        .build())
                .build()));
    String updateSql = "update my_table set value=? where id=?";
    String pgUpdateSql = "update my_table set value=$1 where id=$2";
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgUpdateSql).bind("p1").to("One").bind("p2").to(1L).build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgUpdateSql).bind("p1").to("Two").bind("p2").to(2L).build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgUpdateSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));

    try (Connection connection =
        DriverManager.getConnection(createUrl() + "&reWriteBatchedInserts=true")) {
      connection.setAutoCommit(false);

      for (int i = 0; i < 10; i++) {
        try (PreparedStatement insertStatement = connection.prepareStatement(insertSql);
            PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {
          insertStatement.setLong(1, 1L);
          insertStatement.setString(2, "One");
          insertStatement.addBatch();

          updateStatement.setString(1, "One");
          updateStatement.setLong(2, 1L);
          updateStatement.addBatch();
          updateStatement.setString(1, "Two");
          updateStatement.setLong(2, 2L);
          updateStatement.addBatch();

          try (PreparedStatement selectStatement = connection.prepareStatement(selectSql)) {
            selectStatement.setLong(1, 1L);
            try (ResultSet resultSet = selectStatement.executeQuery()) {
              assertTrue(resultSet.next());
              assertEquals("One", resultSet.getString(1));
              assertFalse(resultSet.next());
            }
          }

          insertStatement.setLong(1, 2L);
          insertStatement.setString(2, "Two");
          insertStatement.addBatch();

          int[] updateCounts = insertStatement.executeBatch();
          assertEquals(2, updateCounts.length);

          updateCounts = updateStatement.executeBatch();
          assertEquals(2, updateCounts.length);

          connection.commit();
        }
      }
    }

    assertEquals(10, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    // We receive 21 ExecuteSql requests, because the update statement is described.
    assertEquals(21, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testBatchedAutoDescribedPreparedStatement() throws SQLException {
    String insertSql = "insert into my_table (id, value) values (?, ?)";
    String pgInsertSql = "insert into my_table (id, value) values ($1, $2)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgInsertSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.TIMESTAMP)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgInsertSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .build(),
            1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgInsertSql)
                .bind("p1")
                .to(2L)
                .bind("p2")
                .to(Timestamp.parseTimestamp("2023-03-08T18:11:00Z"))
                .build(),
            1L));
    String updateSql = "update my_table set value=? where id=?";
    String pgUpdateSql = "update my_table set value=$1 where id=$2";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgUpdateSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(TypeCode.TIMESTAMP, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgUpdateSql)
                .bind("p1")
                .to(Timestamp.parseTimestamp("2023-03-08T18:11:00Z"))
                .bind("p2")
                .to(1L)
                .build(),
            1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgUpdateSql)
                .bind("p1")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p2")
                .to(2L)
                .build(),
            1L));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      try (PreparedStatement insertStatement = connection.prepareStatement(insertSql);
          PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {
        insertStatement.setLong(1, 1L);
        insertStatement.setTimestamp(
            2, Timestamp.parseTimestamp("2023-03-08T18:10:00Z").toSqlTimestamp());
        insertStatement.addBatch();
        insertStatement.setLong(1, 2L);
        insertStatement.setTimestamp(
            2, Timestamp.parseTimestamp("2023-03-08T18:11:00Z").toSqlTimestamp());
        insertStatement.addBatch();

        updateStatement.setTimestamp(
            1, Timestamp.parseTimestamp("2023-03-08T18:11:00Z").toSqlTimestamp());
        updateStatement.setLong(2, 1L);
        updateStatement.addBatch();
        updateStatement.setTimestamp(
            1, Timestamp.parseTimestamp("2023-03-08T18:10:00Z").toSqlTimestamp());
        updateStatement.setLong(2, 2L);
        updateStatement.addBatch();

        int[] updateCounts = insertStatement.executeBatch();
        assertEquals(2, updateCounts.length);

        updateCounts = updateStatement.executeBatch();
        assertEquals(2, updateCounts.length);

        connection.commit();
      }
    }

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    // Both statements are auto-described.
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testBatchedRewrittenPreparedStatement() throws SQLException {
    String insertSql = "insert into my_table (id, value) values (?, ?)";
    String rewrittenInsertSql1 =
        "insert into my_table (id, value) values ($1, $2),($3, $4),($5, $6),($7, $8),($9, $10),($11, $12),($13, $14),($15, $16)";
    String rewrittenInsertSql2 = "insert into my_table (id, value) values ($1, $2),($3, $4)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(rewrittenInsertSql1),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(rewrittenInsertSql2),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(rewrittenInsertSql1)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p3")
                .to(2L)
                .bind("p4")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p5")
                .to(3L)
                .bind("p6")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p7")
                .to(4L)
                .bind("p8")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p9")
                .to(5L)
                .bind("p10")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p11")
                .to(6L)
                .bind("p12")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p13")
                .to(7L)
                .bind("p14")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p15")
                .to(8L)
                .bind("p16")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .build(),
            8L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(rewrittenInsertSql2)
                .bind("p1")
                .to(9L)
                .bind("p2")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p3")
                .to(10L)
                .bind("p4")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .build(),
            2L));

    try (Connection connection =
        DriverManager.getConnection(createUrl() + "&reWriteBatchedInserts=true")) {
      connection.setAutoCommit(false);

      try (PreparedStatement insertStatement = connection.prepareStatement(insertSql)) {
        for (int i = 1; i <= 10; i++) {
          insertStatement.setLong(1, i);
          insertStatement.setTimestamp(
              2, Timestamp.parseTimestamp("2023-03-08T18:10:00Z").toSqlTimestamp());
          insertStatement.addBatch();
        }

        int[] updateCounts = insertStatement.executeBatch();
        assertEquals(10, updateCounts.length);

        connection.commit();
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    // Both statements are auto-described.
    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testBatchedRewrittenPreparedStatementWithErrorInBind() throws SQLException {
    String insertSql = "insert into my_table (id, value) values (?, ?)";
    String rewrittenInsertSql1 =
        "insert into my_table (id, value) values ($1, $2),($3, $4),($5, $6),($7, $8),($9, $10),($11, $12),($13, $14),($15, $16)";
    String rewrittenInsertSql2 = "insert into my_table (id, value) values ($1, $2),($3, $4)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(rewrittenInsertSql1),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(rewrittenInsertSql2),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(rewrittenInsertSql1)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p3")
                .to(2L)
                .bind("p4")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p5")
                .to(3L)
                .bind("p6")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p7")
                .to(4L)
                .bind("p8")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p9")
                .to(5L)
                .bind("p10")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p11")
                .to(6L)
                .bind("p12")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p13")
                .to(7L)
                .bind("p14")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p15")
                .to(8L)
                .bind("p16")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .build(),
            8L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(rewrittenInsertSql2)
                .bind("p1")
                .to(9L)
                .bind("p2")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .bind("p3")
                .to(10L)
                .bind("p4")
                .to(Timestamp.parseTimestamp("2023-03-08T18:10:00Z"))
                .build(),
            2L));

    try (Connection connection =
        DriverManager.getConnection(createUrl() + "&reWriteBatchedInserts=true")) {
      connection.setAutoCommit(false);

      try (PreparedStatement insertStatement = connection.prepareStatement(insertSql)) {
        for (int i = 1; i <= 10; i++) {
          insertStatement.setLong(1, i);
          insertStatement.setTimestamp(2, new java.sql.Timestamp(Long.MIN_VALUE));
          insertStatement.addBatch();
        }

        BatchUpdateException exception =
            assertThrows(BatchUpdateException.class, insertStatement::executeBatch);
        assertTrue(
            exception.getMessage(), exception.getMessage().contains("Invalid timestamp value"));

        connection.rollback();
      }
    }
  }

  @Test
  public void testBatchedRewrittenPreparedStatementWithGenericBatchExecutionError()
      throws SQLException {
    String insertSql = "insert into my_table (id, value) values (?, ?)";
    String rewrittenInsertSql1 =
        "insert into my_table (id, value) values ($1, $2),($3, $4),($5, $6),($7, $8),($9, $10),($11, $12),($13, $14),($15, $16)";
    String rewrittenInsertSql2 = "insert into my_table (id, value) values ($1, $2),($3, $4)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(rewrittenInsertSql1),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(rewrittenInsertSql2),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64, TypeCode.TIMESTAMP,
                            TypeCode.INT64, TypeCode.TIMESTAMP)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.setExecuteBatchDmlExecutionTime(
        SimulatedExecutionTime.ofException(
            Status.INVALID_ARGUMENT
                .withDescription("Too many values in insert clause")
                .asRuntimeException()));

    try (Connection connection =
        DriverManager.getConnection(createUrl() + "&reWriteBatchedInserts=true")) {
      connection.setAutoCommit(false);

      try (PreparedStatement insertStatement = connection.prepareStatement(insertSql)) {
        for (int i = 1; i <= 10; i++) {
          insertStatement.setLong(1, i);
          insertStatement.setTimestamp(
              2, Timestamp.parseTimestamp("2023-03-08T18:10:00Z").toSqlTimestamp());
          insertStatement.addBatch();
        }

        BatchUpdateException exception =
            assertThrows(BatchUpdateException.class, insertStatement::executeBatch);
        assertTrue(
            exception.getMessage(),
            exception.getMessage().contains("Too many values in insert clause"));

        connection.rollback();
      }
    }
  }

  @Test
  public void testParameterizedOffsetWithoutLimit() throws SQLException {
    // Add a result for the non-limited query that contains one row.
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder("select * from foo offset $1").bind("p1").to(0L).build(),
            SELECT1_RESULTSET));
    // Add a result for the limited query that is empty.
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder("select * from foo offset $1 limit 4611686018427387903")
                .bind("p1")
                .to(0L)
                .build(),
            EMPTY_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      for (boolean addLimit : new boolean[] {true, false}) {
        connection.createStatement().execute("set spanner.auto_add_limit_clause=" + addLimit);
        try (PreparedStatement statement =
            connection.prepareStatement("select * from foo offset ?")) {
          statement.setLong(1, 0);
          try (ResultSet resultSet = statement.executeQuery()) {
            // We should get the empty result set when the auto-limit feature is enabled.
            if (addLimit) {
              assertFalse(resultSet.next());
            } else {
              assertTrue(resultSet.next());
              assertEquals(1L, resultSet.getLong(1));
              assertFalse(resultSet.next());
            }
          }
        }
      }
    }
  }

  @Test
  public void testTwoQueries() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns true if the result is a result set.
        assertTrue(statement.execute("SELECT 1;SELECT 2;"));

        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        // getMoreResults() returns true if the next result is a ResultSet.
        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(2L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        // getMoreResults() should now return false. We should also check getUpdateCount() as that
        // method should return -1 to indicate that there is also no update count available.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testDdl() throws SQLException {
    String sql = "CREATE TABLE foo (id bigint primary key)";
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(sql, updateDatabaseDdlRequests.get(0).getStatements(0));
  }

  @Test
  public void testDdlBatch() throws SQLException {
    ImmutableList<String> statements =
        ImmutableList.of(
            "CREATE TABLE foo (id bigint primary key)",
            "CREATE TABLE bar (id bigint primary key, value text)",
            "CREATE INDEX idx_foo ON bar (text)");
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        for (String sql : statements) {
          statement.addBatch(sql);
        }
        int[] updateCounts = statement.executeBatch();
        assertArrayEquals(new int[] {0, 0, 0}, updateCounts);
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(3, updateDatabaseDdlRequests.get(0).getStatementsCount());
    for (int i = 0; i < statements.size(); i++) {
      assertEquals(statements.get(i), updateDatabaseDdlRequests.get(0).getStatements(i));
    }
  }

  @Test
  public void testCreateTableIfNotExists_withBackendSupport() throws SQLException {
    String sql = "CREATE TABLE IF NOT EXISTS foo (id bigint primary key)";
    // Add a response for the DDL statement that is sent to Spanner.
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "CREATE TABLE IF NOT EXISTS foo (id bigint primary key)",
        updateDatabaseDdlRequests.get(0).getStatements(0));
  }

  @Test
  public void testPreparedStatement() throws SQLException {
    String pgSql =
        "insert into all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, "
            + "col_array_bigint, col_array_bool, col_array_bytea, col_array_float8, col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON),
                        true))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(100L)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-02-16T13:18:02.123457000"))
                .bind("p8")
                .to(Date.parseDate("2022-03-29"))
                .bind("p9")
                .to("test")
                .bind("p10")
                .to(
                    com.google.cloud.spanner.Value.pgJsonb(
                        "{\"key1\": \"value1\", \"key2\": \"value2\"}"))
                .bind("p11")
                .toInt64Array(Arrays.asList(1L, null, 2L))
                .bind("p12")
                .toBoolArray(Arrays.asList(true, null, false))
                .bind("p13")
                .toBytesArray(
                    Arrays.asList(ByteArray.copyFrom("bytes1"), null, ByteArray.copyFrom("bytes2")))
                .bind("p14")
                .toFloat64Array(Arrays.asList(3.14d, null, 6.626d))
                .bind("p15")
                .toInt64Array(Arrays.asList(-1L, null, -2L))
                .bind("p16")
                .toPgNumericArray(Arrays.asList("3.14", null, "6.626"))
                .bind("p17")
                .toTimestampArray(
                    Arrays.asList(
                        Timestamp.parseTimestamp("2022-02-11T12:45:00.123456000Z"),
                        null,
                        Timestamp.parseTimestamp("2000-01-01T00:00:00Z")))
                .bind("p18")
                .toDateArray(
                    Arrays.asList(Date.parseDate("2000-01-01"), null, Date.parseDate("1970-01-01")))
                .bind("p19")
                .toStringArray(Arrays.asList("string1", null, "string2"))
                .bind("p20")
                .toPgJsonbArray(
                    Arrays.asList(
                        "{\"key1\": \"value1\", \"key2\": \"value2\"}",
                        null,
                        "{\"key1\": \"value3\", \"key2\": \"value4\"}"))
                .build(),
            1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(pgSql)
                .bind("p1")
                .to(2L)
                .bind("p2")
                .to((Boolean) null)
                .bind("p3")
                .to((ByteArray) null)
                .bind("p4")
                .to((Double) null)
                .bind("p5")
                .to((Long) null)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric(null))
                .bind("p7")
                .to((Timestamp) null)
                .bind("p8")
                .to((Date) null)
                .bind("p9")
                .to((String) null)
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb(null))
                .bind("p11")
                .toInt64Array((long[]) null)
                .bind("p12")
                .toBoolArray((boolean[]) null)
                .bind("p13")
                .toBytesArray(null)
                .bind("p14")
                .toFloat64Array((double[]) null)
                .bind("p15")
                .toInt64Array((long[]) null)
                .bind("p16")
                .toPgNumericArray(null)
                .bind("p17")
                .toTimestampArray(null)
                .bind("p18")
                .toDateArray(null)
                .bind("p19")
                .toStringArray(null)
                .bind("p20")
                .toPgJsonbArray(null)
                .build(),
            1L));

    OffsetDateTime zonedDateTime =
        LocalDateTime.of(2022, 2, 16, 13, 18, 2, 123456789).atOffset(ZoneOffset.UTC);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "insert into all_types "
                  + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, "
                  + "col_array_bigint, col_array_bool, col_array_bytea, col_array_float8, col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb) "
                  + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
        PGStatement pgStatement = statement.unwrap(PGStatement.class);
        pgStatement.setPrepareThreshold(1);

        int index = 0;
        statement.setLong(++index, 1L);
        statement.setBoolean(++index, true);
        statement.setBytes(++index, "test".getBytes(StandardCharsets.UTF_8));
        statement.setDouble(++index, 3.14d);
        statement.setInt(++index, 100);
        statement.setBigDecimal(++index, new BigDecimal("6.626"));
        statement.setObject(++index, zonedDateTime);
        statement.setObject(++index, LocalDate.of(2022, 3, 29));
        statement.setString(++index, "test");
        statement.setObject(++index, "{\"key1\": \"value1\", \"key2\": \"value2\"}", Types.OTHER);

        statement.setArray(++index, connection.createArrayOf("bigint", new Long[] {1L, null, 2L}));
        statement.setArray(
            ++index, connection.createArrayOf("bool", new Boolean[] {true, null, false}));
        statement.setArray(
            ++index,
            connection.createArrayOf(
                "bytea",
                new byte[][] {
                  "bytes1".getBytes(StandardCharsets.UTF_8),
                  null,
                  "bytes2".getBytes(StandardCharsets.UTF_8)
                }));
        statement.setArray(
            ++index, connection.createArrayOf("float8", new Double[] {3.14d, null, 6.626}));
        statement.setArray(++index, connection.createArrayOf("int", new Integer[] {-1, null, -2}));
        statement.setArray(
            ++index,
            connection.createArrayOf(
                "numeric",
                new BigDecimal[] {new BigDecimal("3.14"), null, new BigDecimal("6.626")}));
        statement.setArray(
            ++index,
            connection.createArrayOf(
                "timestamptz",
                new java.sql.Timestamp[] {
                  Timestamp.parseTimestamp("2022-02-11T13:45:00.123456+01:00").toSqlTimestamp(),
                  null,
                  Timestamp.parseTimestamp("2000-01-01T00:00:00Z").toSqlTimestamp()
                }));
        statement.setArray(
            ++index,
            connection.createArrayOf(
                "date",
                new LocalDate[] {LocalDate.of(2000, 1, 1), null, LocalDate.of(1970, 1, 1)}));
        statement.setArray(
            ++index,
            connection.createArrayOf("varchar", new String[] {"string1", null, "string2"}));
        statement.setArray(
            ++index,
            connection.createArrayOf(
                "jsonb",
                new String[] {
                  "{\"key1\": \"value1\", \"key2\": \"value2\"}",
                  null,
                  "{\"key1\": \"value3\", \"key2\": \"value4\"}"
                }));

        assertEquals(1, statement.executeUpdate());

        index = 0;
        statement.setLong(++index, 2);
        statement.setNull(++index, Types.BOOLEAN);
        statement.setNull(++index, Types.BINARY);
        statement.setNull(++index, Types.DOUBLE);
        statement.setNull(++index, Types.INTEGER);
        statement.setNull(++index, Types.NUMERIC);
        statement.setNull(++index, Types.TIMESTAMP_WITH_TIMEZONE);
        statement.setNull(++index, Types.DATE);
        statement.setNull(++index, Types.VARCHAR);
        statement.setNull(++index, Types.OTHER);

        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);
        statement.setNull(++index, Types.ARRAY);

        assertEquals(1, statement.executeUpdate());
      }
    }
  }

  @Test
  public void testPreparedStatementReturning() throws SQLException {
    String pgSql =
        "insert into all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) "
            + "returning *";
    String sql =
        "insert into all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            + "returning *";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(pgSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ALL_TYPES_METADATA
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.INT64,
                                        TypeCode.BOOL,
                                        TypeCode.BYTES,
                                        TypeCode.FLOAT64,
                                        TypeCode.INT64,
                                        TypeCode.NUMERIC,
                                        TypeCode.TIMESTAMP,
                                        TypeCode.DATE,
                                        TypeCode.STRING,
                                        TypeCode.JSON))
                                .getUndeclaredParameters())
                        .build())
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(pgSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(100L)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-02-16T13:18:02.123457000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-03-29"))
                .bind("p9")
                .to("test")
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                .build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(ALL_TYPES_METADATA)
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(ALL_TYPES_RESULTSET.getRows(0))
                .build()));

    OffsetDateTime zonedDateTime =
        LocalDateTime.of(2022, 2, 16, 13, 18, 2, 123456789).atOffset(ZoneOffset.UTC);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        ParameterMetaData parameterMetaData = statement.getParameterMetaData();
        assertEquals(10, parameterMetaData.getParameterCount());
        assertEquals(Types.BIGINT, parameterMetaData.getParameterType(1));
        assertEquals(Types.BIT, parameterMetaData.getParameterType(2));
        assertEquals(Types.BINARY, parameterMetaData.getParameterType(3));
        assertEquals(Types.DOUBLE, parameterMetaData.getParameterType(4));
        assertEquals(Types.BIGINT, parameterMetaData.getParameterType(5));
        assertEquals(Types.NUMERIC, parameterMetaData.getParameterType(6));
        assertEquals(Types.TIMESTAMP, parameterMetaData.getParameterType(7));
        assertEquals(Types.DATE, parameterMetaData.getParameterType(8));
        assertEquals(Types.VARCHAR, parameterMetaData.getParameterType(9));
        assertEquals(Types.OTHER, parameterMetaData.getParameterType(10));

        ResultSetMetaData metadata = statement.getMetaData();
        assertEquals(20, metadata.getColumnCount());
        int index = 0;
        assertEquals(Types.BIGINT, metadata.getColumnType(++index));
        assertEquals(Types.BIT, metadata.getColumnType(++index));
        assertEquals(Types.BINARY, metadata.getColumnType(++index));
        assertEquals(Types.DOUBLE, metadata.getColumnType(++index));
        assertEquals(Types.BIGINT, metadata.getColumnType(++index));
        assertEquals(Types.NUMERIC, metadata.getColumnType(++index));
        assertEquals(Types.TIMESTAMP, metadata.getColumnType(++index));
        assertEquals(Types.DATE, metadata.getColumnType(++index));
        assertEquals(Types.VARCHAR, metadata.getColumnType(++index));
        assertEquals(Types.OTHER, metadata.getColumnType(++index));

        assertEquals(Types.ARRAY, metadata.getColumnType(++index));
        assertEquals(Types.ARRAY, metadata.getColumnType(++index));
        assertEquals(Types.ARRAY, metadata.getColumnType(++index));
        assertEquals(Types.ARRAY, metadata.getColumnType(++index));
        assertEquals(Types.ARRAY, metadata.getColumnType(++index));
        assertEquals(Types.ARRAY, metadata.getColumnType(++index));
        assertEquals(Types.ARRAY, metadata.getColumnType(++index));
        assertEquals(Types.ARRAY, metadata.getColumnType(++index));
        assertEquals(Types.ARRAY, metadata.getColumnType(++index));
        assertEquals(Types.ARRAY, metadata.getColumnType(++index));

        index = 0;
        statement.setLong(++index, 1L);
        statement.setBoolean(++index, true);
        statement.setBytes(++index, "test".getBytes(StandardCharsets.UTF_8));
        statement.setDouble(++index, 3.14d);
        statement.setInt(++index, 100);
        statement.setBigDecimal(++index, new BigDecimal("6.626"));
        statement.setObject(++index, zonedDateTime);
        statement.setObject(++index, LocalDate.of(2022, 3, 29));
        statement.setString(++index, "test");
        statement.setObject(++index, createJdbcPgJsonbObject("{\"key\": \"value\"}"));

        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testCursorSuccess() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      try (PreparedStatement statement = connection.prepareStatement(SELECT_FIVE_ROWS.getSql())) {
        // Fetch two rows at a time from the PG server.
        statement.setFetchSize(2);
        try (ResultSet resultSet = statement.executeQuery()) {
          int index = 0;
          while (resultSet.next()) {
            assertEquals(++index, resultSet.getInt(1));
          }
          assertEquals(5, index);
        }
      }
      connection.commit();
    }
    // The ExecuteSql request should only be sent once to Cloud Spanner.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(SELECT_FIVE_ROWS.getSql(), executeRequest.getSql());

    // PGAdapter should receive 5 Execute messages:
    // 1. BEGIN
    // 2. Execute - fetch rows 1, 2
    // 3. Execute - fetch rows 3, 4
    // 4. Execute - fetch rows 5
    // 5. COMMIT
    if (pgServer != null) {
      List<DescribeMessage> describeMessages = getWireMessagesOfType(DescribeMessage.class);
      assertEquals(1, describeMessages.size());
      DescribeMessage describeMessage = describeMessages.get(0);
      assertEquals(PreparedType.Portal, describeMessage.getType());

      List<ExecuteMessage> executeMessages =
          getWireMessagesOfType(ExecuteMessage.class).stream()
              .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
              .collect(Collectors.toList());
      assertEquals(5, executeMessages.size());
      assertEquals("", executeMessages.get(0).getName());
      for (ExecuteMessage executeMessage : executeMessages.subList(1, executeMessages.size() - 1)) {
        assertEquals(describeMessage.getName(), executeMessage.getName());
        assertEquals(2, executeMessage.getMaxRows());
      }
      assertEquals("", executeMessages.get(executeMessages.size() - 1).getName());

      List<ParseMessage> parseMessages =
          getWireMessagesOfType(ParseMessage.class).stream()
              .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
              .collect(Collectors.toList());
      assertEquals(3, parseMessages.size());
      assertEquals("BEGIN", parseMessages.get(0).getStatement().getSql());
      assertEquals(SELECT_FIVE_ROWS.getSql(), parseMessages.get(1).getStatement().getSql());
      assertEquals("COMMIT", parseMessages.get(2).getStatement().getSql());
    }
  }

  @Test
  public void testCursorFailsHalfway() throws SQLException {
    mockSpanner.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStreamException(Status.DATA_LOSS.asRuntimeException(), 2));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      try (PreparedStatement statement = connection.prepareStatement(SELECT_FIVE_ROWS.getSql())) {
        // Fetch one row at a time from the PG server.
        statement.setFetchSize(1);
        try (ResultSet resultSet = statement.executeQuery()) {
          // The first row should succeed.
          assertTrue(resultSet.next());
          // The second row should fail.
          assertThrows(SQLException.class, resultSet::next);
        }
      }
      connection.rollback();
    }
    // The ExecuteSql request should only be sent once to Cloud Spanner.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(SELECT_FIVE_ROWS.getSql(), executeRequest.getSql());

    // PGAdapter should receive 4 Execute messages:
    // 1. BEGIN
    // 2. Execute - fetch row 1
    // 3. Execute - fetch row 2 -- This fails with a DATA_LOSS error
    // The JDBC driver does not send a ROLLBACK
    if (pgServer != null) {
      List<DescribeMessage> describeMessages = getWireMessagesOfType(DescribeMessage.class);
      assertEquals(1, describeMessages.size());
      DescribeMessage describeMessage = describeMessages.get(0);
      assertEquals(PreparedType.Portal, describeMessage.getType());

      List<ExecuteMessage> executeMessages =
          getWireMessagesOfType(ExecuteMessage.class).stream()
              .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
              .collect(Collectors.toList());
      assertEquals(4, executeMessages.size());
      assertEquals("", executeMessages.get(0).getName());
      for (ExecuteMessage executeMessage : executeMessages.subList(1, executeMessages.size() - 1)) {
        assertEquals(describeMessage.getName(), executeMessage.getName());
        assertEquals(1, executeMessage.getMaxRows());
      }
      assertEquals("", executeMessages.get(executeMessages.size() - 1).getName());

      List<ParseMessage> parseMessages =
          getWireMessagesOfType(ParseMessage.class).stream()
              .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
              .collect(Collectors.toList());
      assertEquals(3, parseMessages.size());
      assertEquals("BEGIN", parseMessages.get(0).getStatement().getSql());
      assertEquals(SELECT_FIVE_ROWS.getSql(), parseMessages.get(1).getStatement().getSql());
      assertEquals("ROLLBACK", parseMessages.get(2).getStatement().getSql());
    }
  }

  @Test
  public void testJulianDate() throws SQLException {
    String sql = "select d from foo";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("d")
                                        .setType(Type.newBuilder().setCode(TypeCode.DATE).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("0300-02-20").build())
                        .build())
                .build()));
    String binaryTransferEnable = "&binaryTransferEnable=" + Oid.DATE;

    try (Connection connection = DriverManager.getConnection(createUrl() + binaryTransferEnable)) {
      connection.createStatement().execute("set time zone 'utc'");
      connection.unwrap(PGConnection.class).setPrepareThreshold(-1);
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(LocalDate.of(300, 2, 20), resultSet.getDate(1).toLocalDate());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testRandomResults() throws SQLException {
    for (int bufferSize : new int[] {0, 32}) {
      for (boolean binary : new boolean[] {false, true}) {
        // Also get the random results using the normal Spanner client to compare the results with
        // what is returned by PGAdapter.
        Spanner spanner =
            SpannerOptions.newBuilder()
                .setProjectId("p")
                .setHost(String.format("http://localhost:%d", spannerServer.getPort()))
                .setCredentials(NoCredentials.getInstance())
                .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                .setClientLibToken("pg-adapter")
                .build()
                .getService();
        DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of("p", "i", "d"));
        com.google.cloud.spanner.ResultSet spannerResult =
            client.singleUse().executeQuery(SELECT_RANDOM);

        String binaryTransferEnable =
            "&binaryTransferEnable="
                + ImmutableList.of(
                        Oid.BOOL,
                        Oid.BYTEA,
                        Oid.VARCHAR,
                        Oid.NUMERIC,
                        Oid.FLOAT8,
                        Oid.INT8,
                        Oid.DATE,
                        Oid.TIMESTAMPTZ)
                    .stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(","));

        final int fetchSize = 3;
        try (Connection connection =
            DriverManager.getConnection(createUrl() + binaryTransferEnable)) {
          connection.setAutoCommit(false);
          connection.unwrap(PGConnection.class).setPrepareThreshold(binary ? -1 : 5);
          connection
              .createStatement()
              .execute(String.format("set spanner.string_conversion_buffer_size=%d", bufferSize));
          connection
              .createStatement()
              .execute(String.format("set spanner.binary_conversion_buffer_size=%d", bufferSize));

          try (PreparedStatement statement = connection.prepareStatement(SELECT_RANDOM.getSql())) {
            statement.setFetchSize(fetchSize);
            try (ResultSet resultSet = statement.executeQuery()) {
              int rowCount = 0;
              while (resultSet.next()) {
                assertTrue(spannerResult.next());
                for (int col = 0; col < resultSet.getMetaData().getColumnCount(); col++) {
                  // TODO: Remove once we have a replacement for pg_type, as the JDBC driver will
                  // try
                  // to read type information from the backend when it hits an 'unknown' type (jsonb
                  // is not one of the types that the JDBC driver will load automatically).
                  if (col == 5 || col == 14) {
                    resultSet.getString(col + 1);
                  } else {
                    resultSet.getObject(col + 1);
                  }
                }
                assertEqual(spannerResult, resultSet);
                rowCount++;
              }
              assertEquals(RANDOM_RESULTS_ROW_COUNT, rowCount);
            }
          }
          connection.commit();
        }

        // Close the resources used by the normal Spanner client.
        spannerResult.close();
        spanner.close();

        // The ExecuteSql request should only be sent once to Cloud Spanner by PGAdapter.
        // The normal Spanner client will also send it once to Spanner.
        assertEquals(binary ? 3 : 2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
        ExecuteSqlRequest executeRequest =
            mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(binary ? 2 : 1);
        assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
        assertEquals(SELECT_RANDOM.getSql(), executeRequest.getSql());

        // PGAdapter should receive 5 Execute messages:
        // 1. BEGIN
        // 2. Execute - fetch rows 1, 2
        // 3. Execute - fetch rows 3, 4
        // 4. Execute - fetch rows 5
        // 5. COMMIT
        if (pgServer != null) {
          List<DescribeMessage> describeMessages =
              getWireMessagesOfType(DescribeMessage.class).stream()
                  .filter(message -> message.getSql().equals(SELECT_RANDOM.getSql()))
                  .collect(Collectors.toList());
          assertEquals(1, describeMessages.size());
          DescribeMessage describeMessage = describeMessages.get(0);
          assertEquals(
              binary ? PreparedType.Statement : PreparedType.Portal, describeMessage.getType());

          List<ExecuteMessage> executeMessages =
              getWireMessagesOfType(ExecuteMessage.class).stream()
                  .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
                  .filter(message -> !message.getSql().startsWith("set spanner."))
                  .collect(Collectors.toList());
          int expectedExecuteMessageCount =
              RANDOM_RESULTS_ROW_COUNT / fetchSize
                  + ((RANDOM_RESULTS_ROW_COUNT % fetchSize) > 0 ? 1 : 0)
                  + 2;
          assertEquals(expectedExecuteMessageCount, executeMessages.size());
          assertEquals("", executeMessages.get(0).getName());
          for (ExecuteMessage executeMessage :
              executeMessages.subList(1, executeMessages.size() - 1)) {
            assertEquals(fetchSize, executeMessage.getMaxRows());
          }
          assertEquals("", executeMessages.get(executeMessages.size() - 1).getName());

          List<ParseMessage> parseMessages =
              getWireMessagesOfType(ParseMessage.class).stream()
                  .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
                  .filter(message -> !message.getSql().startsWith("set spanner."))
                  .collect(Collectors.toList());
          assertEquals(3, parseMessages.size());
          assertEquals("BEGIN", parseMessages.get(0).getStatement().getSql());
          assertEquals(SELECT_RANDOM.getSql(), parseMessages.get(1).getStatement().getSql());
          assertEquals("COMMIT", parseMessages.get(2).getStatement().getSql());
        }
        mockSpanner.clearRequests();
        pgServer.clearDebugMessages();
      }
    }
  }

  private void assertEqual(com.google.cloud.spanner.ResultSet spannerResult, ResultSet pgResult)
      throws SQLException {
    assertEquals(spannerResult.getColumnCount(), pgResult.getMetaData().getColumnCount());
    for (int col = 0; col < spannerResult.getColumnCount(); col++) {
      if (spannerResult.isNull(col)) {
        assertNull(pgResult.getObject(col + 1));
        assertTrue(pgResult.wasNull());
        continue;
      }

      switch (spannerResult.getColumnType(col).getCode()) {
        case BOOL:
          assertEquals(spannerResult.getBoolean(col), pgResult.getBoolean(col + 1));
          break;
        case INT64:
          assertEquals(spannerResult.getLong(col), pgResult.getLong(col + 1));
          break;
        case FLOAT64:
          assertEquals(spannerResult.getDouble(col), pgResult.getDouble(col + 1), 0.0d);
          break;
        case PG_NUMERIC:
        case STRING:
          assertEquals(spannerResult.getString(col), pgResult.getString(col + 1));
          break;
        case BYTES:
          assertArrayEquals(spannerResult.getBytes(col).toByteArray(), pgResult.getBytes(col + 1));
          break;
        case TIMESTAMP:
          // Compare milliseconds, as PostgreSQL does not natively support nanosecond precision, and
          // this is lost when using binary encoding.
          assertEquals(
              spannerResult.getTimestamp(col).toSqlTimestamp().getTime(),
              pgResult.getTimestamp(col + 1).getTime());
          break;
        case DATE:
          LocalDate expected =
              LocalDate.of(
                  spannerResult.getDate(col).getYear(),
                  spannerResult.getDate(col).getMonth(),
                  spannerResult.getDate(col).getDayOfMonth());
          if ((expected.getYear() == 1582 && expected.getMonth() == Month.OCTOBER)
              || (expected.getYear() <= 1582
                  && expected.getMonth() == Month.FEBRUARY
                  && expected.getDayOfMonth() > 20)) {
            // Just assert that we can get the value. Dates in the Julian/Gregorian cutover period
            // are weird, as are potential intercalaris values.
            assertNotNull(pgResult.getDate(col + 1));
          } else {
            assertEquals(expected, pgResult.getDate(col + 1).toLocalDate());
          }
          break;
        case PG_JSONB:
          assertEquals(spannerResult.getPgJsonb(col), pgResult.getString(col + 1));
          break;
        case ARRAY:
          break;
        case NUMERIC:
        case JSON:
        case STRUCT:
          fail("unsupported PG type: " + spannerResult.getColumnType(col));
      }
    }
  }

  @Test
  public void testInformationSchemaQueryInTransaction() throws SQLException {
    String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";
    // Register a result for the query. Note that we don't really care what the result is, just that
    // there is a result.
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Make sure that we start a transaction.
      connection.setAutoCommit(false);

      // Execute a query to start the transaction.
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT1.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // This ensures that the following query returns an error the first time it is executed, and
      // then succeeds the second time. This happens because the exception is 'popped' from the
      // response queue when it is returned. The next time the query is executed, it will return the
      // actual result that we set.
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofException(
              Status.INVALID_ARGUMENT
                  .withDescription(
                      "Unsupported concurrency mode in query using INFORMATION_SCHEMA.")
                  .asRuntimeException()));
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // Make sure that the connection is still usable.
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT2.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(2L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      connection.commit();
    }

    // We should receive the INFORMATION_SCHEMA statement twice on Cloud Spanner:
    // 1. The first time it returns an error because it is using the wrong concurrency mode.
    // 2. The specific error will cause the connection to retry the statement using a single-use
    //    read-only transaction.
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // The first statement should start a transaction
    assertTrue(requests.get(0).getTransaction().hasBegin());
    // The second statement (the initial attempt of the INFORMATION_SCHEMA query) should try to use
    // the transaction.
    assertTrue(requests.get(1).getTransaction().hasId());
    assertEquals(sql, requests.get(1).getSql());
    // The INFORMATION_SCHEMA query is then retried using a single-use read-only transaction.
    assertFalse(requests.get(2).hasTransaction());
    assertEquals(sql, requests.get(2).getSql());
    // The last statement should use the transaction.
    assertTrue(requests.get(3).getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(commitRequest.getTransactionId(), requests.get(1).getTransaction().getId());
    assertEquals(commitRequest.getTransactionId(), requests.get(3).getTransaction().getId());
  }

  @Test
  public void testInformationSchemaQueryAsFirstQueryInTransaction() throws SQLException {
    // Running an information_schema query as the first query in a transaction will cause some
    // additional retrying and transaction magic. This is because:
    // 1. The first query in a transaction will also try to begin the transaction.
    // 2. If the first query fails, it will also fail to create a transaction.
    // 3. If an additional query is executed in the transaction, the entire transaction will be
    //    retried using an explicit BeginTransaction RPC. This is done so that we can include the
    //    first query in the transaction, as an error message in itself can give away information
    //    about the state of the database, and therefore must be included in the transaction to
    //    guarantee the consistency.

    String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";
    // Register a result for the query. Note that we don't really care what the result is, just that
    // there is a result.
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Make sure that we start a transaction.
      connection.setAutoCommit(false);

      // This makes sure the INFORMATION_SCHEMA query will return an error the first time it is
      // executed. Then it is retried without a transaction.
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofException(
              Status.INVALID_ARGUMENT
                  .withDescription(
                      "Unsupported concurrency mode in query using INFORMATION_SCHEMA.")
                  .asRuntimeException()));
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // This ensures that the next query will once again return the same error. The reason that we
      // do this is that the following query will cause the entire transaction to be retried, and we
      // need the first statement (the INFORMATION_SCHEMA query) to return exactly the same result
      // as the first time in order to make the retry succeed.
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofException(
              Status.INVALID_ARGUMENT
                  .withDescription(
                      "Unsupported concurrency mode in query using INFORMATION_SCHEMA.")
                  .asRuntimeException()));

      // Verify that the connection is still usable.
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT2.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(2L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      connection.commit();
    }

    // We should receive the INFORMATION_SCHEMA statement three times on Cloud Spanner:
    // 1. The first time it returns an error because it is using the wrong concurrency mode.
    // 2. The specific error will cause the connection to retry the statement using a single-use
    //    read-only transaction.
    // 3. The second query in the transaction will cause the entire transaction to retry, which will
    //    cause the INFORMATION_SCHEMA query to be executed once more.
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // The first statement should try to start a transaction (although it fails).
    assertTrue(requests.get(0).getTransaction().hasBegin());
    // The second statement is the INFORMATION_SCHEMA query without a transaction.
    assertFalse(requests.get(1).hasTransaction());
    assertEquals(sql, requests.get(1).getSql());

    // The transaction is then retried, which means that we get the INFORMATION_SCHEMA query again.
    // This time the query tries to use a transaction that has been created using an explicit
    // BeginTransaction RPC invocation.
    assertTrue(requests.get(2).getTransaction().hasId());
    assertEquals(sql, requests.get(2).getSql());
    // The last query should also use that transaction.
    assertTrue(requests.get(3).getTransaction().hasId());
    assertEquals(SELECT2.getSql(), requests.get(3).getSql());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(commitRequest.getTransactionId(), requests.get(2).getTransaction().getId());
    assertEquals(commitRequest.getTransactionId(), requests.get(3).getTransaction().getId());
    // Verify that we also got a BeginTransaction request.
    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
  }

  @Test
  public void testInformationSchemaQueryInTransactionWithErrorDuringRetry() throws SQLException {
    String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";
    // Register a result for the query. Note that we don't really care what the result is, just that
    // there is a result.
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Make sure that we start a transaction.
      connection.setAutoCommit(false);

      // Execute a query to start the transaction.
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT1.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // This ensures that the following query returns the specific concurrency error the first time
      // it is executed, and then a different error.
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofExceptions(
              ImmutableList.of(
                  Status.INVALID_ARGUMENT
                      .withDescription(
                          "Unsupported concurrency mode in query using INFORMATION_SCHEMA.")
                      .asRuntimeException(),
                  Status.INTERNAL.withDescription("test error").asRuntimeException())));
      SQLException sqlException =
          assertThrows(SQLException.class, () -> connection.createStatement().executeQuery(sql));
      assertEquals(
          "ERROR: test error - Statement: 'SELECT * FROM INFORMATION_SCHEMA.TABLES'",
          sqlException.getMessage());

      // Make sure that the connection is now in the aborted state.
      SQLException abortedException =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().executeQuery(SELECT2.getSql()));
      assertEquals(
          "ERROR: current transaction is aborted, commands ignored until end of transaction block",
          abortedException.getMessage());
    }

    // We should receive the INFORMATION_SCHEMA statement twice on Cloud Spanner:
    // 1. The first time it returns an error because it is using the wrong concurrency mode.
    // 2. The specific error will cause the connection to retry the statement using a single-use
    //    read-only transaction. That will also fail with the second error.
    // 3. The following SELECT query is never sent to Cloud Spanner, as the transaction is in the
    //    aborted state.
    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInformationSchemaQueryInTransactionWithReplacedPgCatalogTables()
      throws SQLException {
    String sql = "SELECT 1 FROM pg_namespace";
    String replacedSql = "with " + PG_NAMESPACE_CTE + "\nSELECT 1 FROM pg_namespace";
    // Register a result for the query. Note that we don't really care what the result is, just that
    // there is a result.
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(replacedSql), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Make sure that we start a transaction.
      connection.setAutoCommit(false);

      // Execute a query to start the transaction.
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT1.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // This ensures that the following query returns an error the first time it is executed, and
      // then succeeds the second time. This happens because the exception is 'popped' from the
      // response queue when it is returned. The next time the query is executed, it will return the
      // actual result that we set.
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofException(
              Status.INVALID_ARGUMENT
                  .withDescription(
                      "Unsupported concurrency mode in query using INFORMATION_SCHEMA.")
                  .asRuntimeException()));
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // Make sure that the connection is still usable.
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT2.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(2L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      connection.commit();
    }

    // We should receive the INFORMATION_SCHEMA statement twice on Cloud Spanner:
    // 1. The first time it returns an error because it is using the wrong concurrency mode.
    // 2. The specific error will cause the connection to retry the statement using a single-use
    //    read-only transaction.
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // The first statement should start a transaction
    assertTrue(requests.get(0).getTransaction().hasBegin());
    // The second statement (the initial attempt of the INFORMATION_SCHEMA query) should try to use
    // the transaction.
    assertTrue(requests.get(1).getTransaction().hasId());
    assertEquals(replacedSql, requests.get(1).getSql());
    // The INFORMATION_SCHEMA query is then retried using a single-use read-only transaction.
    assertFalse(requests.get(2).hasTransaction());
    assertEquals(replacedSql, requests.get(2).getSql());
    // The last statement should use the transaction.
    assertTrue(requests.get(3).getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(commitRequest.getTransactionId(), requests.get(1).getTransaction().getId());
    assertEquals(commitRequest.getTransactionId(), requests.get(3).getTransaction().getId());
  }

  @Test
  public void testPgEnum() throws SQLException {
    for (String sql :
        new String[] {
          "select * from pg_enum",
          "select * from pg_catalog.pg_enum",
          "select * from PG_CATALOG.PG_ENUM"
        }) {
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.of("with " + new EmptyPgEnum().getTableExpression() + "\n" + sql),
              SELECT1_RESULTSET));
      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testShowGuessTypes() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.guess_types")) {
        assertTrue(resultSet.next());
        assertEquals(String.format("%d,%d", Oid.TIMESTAMPTZ, Oid.DATE), resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowGuessTypesOverwritten() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(createUrl() + "?options=-c%20spanner.guess_types=0")) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.guess_types")) {
        assertTrue(resultSet.next());
        assertEquals("0", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowValidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show application_name")) {
        assertTrue(resultSet.next());
        assertEquals(getExpectedInitialApplicationName(), resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowSettingWithStartupValue() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // DATESTYLE is set to 'ISO' by the JDBC driver at startup, except in version 42.7.0, which
      // sets 'ISO, MDY'.
      try (ResultSet resultSet = connection.createStatement().executeQuery("show DATESTYLE")) {
        assertTrue(resultSet.next());
        String dateStyle = resultSet.getString(1);
        assertTrue(dateStyle, "ISO".equals(dateStyle) || "ISO, MDY".equals(dateStyle));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowInvalidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().executeQuery("show random_setting"));
      assertEquals(
          "ERROR: unrecognized configuration parameter \"random_setting\"", exception.getMessage());
    }
  }

  @Test
  public void testSetValidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set application_name to 'my-application'");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show application_name ")) {
        assertTrue(resultSet.next());
        assertEquals("my-application", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testSetCaseInsensitiveSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // The setting is called 'DateStyle' in the pg_settings table.
      connection.createStatement().execute("set datestyle to 'iso'");

      try (ResultSet resultSet = connection.createStatement().executeQuery("show DATESTYLE")) {
        assertTrue(resultSet.next());
        assertEquals("iso", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testSetInvalidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  connection.createStatement().executeQuery("set random_setting to 'some-value'"));
      assertEquals(
          "ERROR: unrecognized configuration parameter \"random_setting\"", exception.getMessage());
    }
  }

  @Test
  public void testResetValidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set application_name to 'my-application'");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show application_name ")) {
        assertTrue(resultSet.next());
        assertEquals("my-application", resultSet.getString(1));
        assertFalse(resultSet.next());
      }

      connection.createStatement().execute("reset application_name");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show application_name ")) {
        assertTrue(resultSet.next());
        assertNull(resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testResetSettingWithStartupValue() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      String originalDateStyle;
      try (ResultSet resultSet = connection.createStatement().executeQuery("show datestyle")) {
        assertTrue(resultSet.next());
        originalDateStyle = resultSet.getString(1);
        assertTrue(
            originalDateStyle,
            "ISO".equals(originalDateStyle) || "ISO, MDY".equals(originalDateStyle));
        assertFalse(resultSet.next());
      }

      connection.createStatement().execute("set datestyle to 'iso, ymd'");

      try (ResultSet resultSet = connection.createStatement().executeQuery("show datestyle")) {
        assertTrue(resultSet.next());
        assertEquals("iso, ymd", resultSet.getString(1));
        assertFalse(resultSet.next());
      }

      connection.createStatement().execute("reset datestyle");

      try (ResultSet resultSet = connection.createStatement().executeQuery("show datestyle")) {
        assertTrue(resultSet.next());
        assertEquals(originalDateStyle, resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testResetInvalidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().executeQuery("reset random_setting"));
      assertEquals(
          "ERROR: unrecognized configuration parameter \"random_setting\"", exception.getMessage());
    }
  }

  @Test
  public void testShowUndefinedExtensionSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().executeQuery("show spanner.some_setting"));
      assertEquals(
          "ERROR: unrecognized configuration parameter \"spanner.some_setting\"",
          exception.getMessage());
    }
  }

  @Test
  public void testSetExtensionSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set spanner.some_setting to 'some-value'");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.some_setting ")) {
        assertTrue(resultSet.next());
        assertEquals("some-value", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testResetValidExtensionSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set spanner.some_setting to 'some-value'");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.some_setting")) {
        assertTrue(resultSet.next());
        assertEquals("some-value", resultSet.getString(1));
        assertFalse(resultSet.next());
      }

      connection.createStatement().execute("reset spanner.some_setting");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.some_setting")) {
        assertTrue(resultSet.next());
        assertNull(resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testResetUndefinedExtensionSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Resetting an undefined extension setting is allowed by PostgreSQL, and will effectively set
      // the extension setting to null.
      connection.createStatement().execute("reset spanner.some_setting");

      verifySettingIsNull(connection, "spanner.some_setting");
    }
  }

  @Test
  public void testCommitSet() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      // Verify that the initial value is 'PostgreSQL JDBC Driver'.
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
      connection.createStatement().execute("set application_name to \"my-application\"");
      verifySettingValue(connection, "application_name", "my-application");
      // Committing the transaction should persist the value.
      connection.commit();
      verifySettingValue(connection, "application_name", "my-application");
    }
  }

  @Test
  public void testRollbackSet() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      // Verify that the initial value is null.
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
      connection.createStatement().execute("set application_name to \"my-application\"");
      verifySettingValue(connection, "application_name", "my-application");
      // Rolling back the transaction should reset the value to what it was before the transaction.
      connection.rollback();
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
    }
  }

  @Test
  public void testCommitSetExtension() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      connection.createStatement().execute("set spanner.random_setting to \"42\"");
      verifySettingValue(connection, "spanner.random_setting", "42");
      // Committing the transaction should persist the value.
      connection.commit();
      verifySettingValue(connection, "spanner.random_setting", "42");
    }
  }

  @Test
  public void testRollbackSetExtension() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      connection.createStatement().execute("set spanner.random_setting to \"42\"");
      verifySettingValue(connection, "spanner.random_setting", "42");
      // Rolling back the transaction should reset the value to what it was before the transaction.
      // In this case, that means that it should be undefined.
      connection.rollback();
      verifySettingIsUnrecognized(connection, "spanner.random_setting");
    }
  }

  @Test
  public void testRollbackDefinedExtension() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // First define the extension setting.
      connection.createStatement().execute("set spanner.random_setting to '100'");

      connection.setAutoCommit(false);

      connection.createStatement().execute("set spanner.random_setting to \"42\"");
      verifySettingValue(connection, "spanner.random_setting", "42");
      // Rolling back the transaction should reset the value to what it was before the transaction.
      // In this case, that means back to '100'.
      connection.rollback();
      verifySettingValue(connection, "spanner.random_setting", "100");
    }
  }

  @Test
  public void testCommitSetLocal() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      // Verify that the initial value is 'PostgreSQL JDBC Driver'.
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
      connection.createStatement().execute("set local application_name to \"my-application\"");
      verifySettingValue(connection, "application_name", "my-application");
      // Committing the transaction should not persist the value as it was only set for the current
      // transaction.
      connection.commit();
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
    }
  }

  @Test
  public void testCommitSetLocalAndSession() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      // Verify that the initial value is 'PostgreSQL JDBC Driver'.
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
      // Set both a session and a local value. The session value will be 'hidden' by the local
      // value, but the session value will be committed.
      connection
          .createStatement()
          .execute("set session application_name to \"my-session-application\"");
      verifySettingValue(connection, "application_name", "my-session-application");
      connection
          .createStatement()
          .execute("set local application_name to \"my-local-application\"");
      verifySettingValue(connection, "application_name", "my-local-application");
      // Committing the transaction should persist the session value.
      connection.commit();
      verifySettingValue(connection, "application_name", "my-session-application");
    }
  }

  @Test
  public void testCommitSetLocalAndSessionExtension() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Verify that the initial value is undefined.
      verifySettingIsUnrecognized(connection, "spanner.custom_setting");

      connection.setAutoCommit(false);

      // Set both a session and a local value. The session value will be 'hidden' by the local
      // value, but the session value will be committed.
      connection.createStatement().execute("set spanner.custom_setting to 'session-value'");
      verifySettingValue(connection, "spanner.custom_setting", "session-value");
      connection.createStatement().execute("set local spanner.custom_setting to 'local-value'");
      verifySettingValue(connection, "spanner.custom_setting", "local-value");
      // Committing the transaction should persist the session value.
      connection.commit();
      verifySettingValue(connection, "spanner.custom_setting", "session-value");
    }
  }

  @Test
  public void testInvalidShowAbortsTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      // Verify that executing an invalid show statement will abort the transaction.
      assertThrows(
          SQLException.class,
          () -> connection.createStatement().execute("show spanner.non_existing_param"));
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().execute("show application_name "));
      assertEquals("ERROR: " + TRANSACTION_ABORTED_ERROR, exception.getMessage());

      connection.rollback();

      // Verify that the connection is usable again.
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
    }
  }

  @Test
  public void testShowAll() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery("show all")) {
        assertEquals(3, resultSet.getMetaData().getColumnCount());
        assertEquals("name", resultSet.getMetaData().getColumnName(1));
        assertEquals("setting", resultSet.getMetaData().getColumnName(2));
        assertEquals("description", resultSet.getMetaData().getColumnName(3));
        int count = 0;
        while (resultSet.next()) {
          if ("client_encoding".equals(resultSet.getString("name"))) {
            assertEquals("UTF8", resultSet.getString("setting"));
          }
          count++;
        }
        assertEquals(361, count);
      }
    }
  }

  @Test
  public void testResetAll() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set application_name to 'my-app'");
      connection.createStatement().execute("set search_path to 'my_schema'");
      verifySettingValue(connection, "application_name", "my-app");
      verifySettingValue(connection, "search_path", "my_schema");

      connection.createStatement().execute("reset all");

      verifySettingIsNull(connection, "application_name");
      verifySettingValue(connection, "search_path", "public");
    }
  }

  @Test
  public void testSetToDefault() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set application_name to 'my-app'");
      connection.createStatement().execute("set search_path to 'my_schema'");
      verifySettingValue(connection, "application_name", "my-app");
      verifySettingValue(connection, "search_path", "my_schema");

      connection.createStatement().execute("set application_name to default");
      connection.createStatement().execute("set search_path to default");

      verifySettingIsNull(connection, "application_name");
      verifySettingValue(connection, "search_path", "public");
    }
  }

  @Test
  public void testSetToEmpty() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set application_name to ''");
      verifySettingValue(connection, "application_name", "");
    }
  }

  @Test
  public void testSetTimeZone() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set time zone 'IST'");
      verifySettingValue(connection, "timezone", "Asia/Kolkata");
    }
  }

  @Test
  public void testSetTimeZoneEST() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Java considers 'EST' to always be '-05:00'. That is; it is never DST.
      connection.createStatement().execute("set time zone 'EST'");
      verifySettingValue(connection, "timezone", "-05:00");
      // 'EST5EDT' is the ID for the timezone that will change with DST.
      connection.createStatement().execute("set time zone 'EST5EDT'");
      verifySettingValue(connection, "timezone", "EST5EDT");
      // 'America/New_York' is the full name of the geographical timezone.
      connection.createStatement().execute("set time zone 'America/New_York'");
      verifySettingValue(connection, "timezone", "America/New_York");
    }
  }

  @Test
  public void testSetTimeZoneToServerDefault() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set time zone 'atlantic/faeroe'");
      verifySettingValue(connection, "timezone", "Atlantic/Faeroe");
      connection.createStatement().execute("set time zone default");
      verifySettingValue(connection, "timezone", TimeZone.getDefault().getID());
    }
  }

  @Test
  public void testSetTimeZoneToLocaltime() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set time zone 'atlantic/jan_mayen'");
      verifySettingValue(connection, "timezone", "Atlantic/Jan_Mayen");
      connection.createStatement().execute("set time zone localtime");
      verifySettingValue(connection, "timezone", TimeZone.getDefault().getID());
    }
  }

  @Test
  public void testSetTimeZoneToDefault() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(createUrl() + "?options=-c%20timezone=IST")) {
      connection.createStatement().execute("set time zone default");
      verifySettingValue(connection, "timezone", "Asia/Kolkata");
    }
  }

  @Test
  public void testSetTimeZoneToLocal() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(createUrl() + "?options=-c%20timezone=IST")) {
      connection.createStatement().execute("set time zone local");
      verifySettingValue(connection, "timezone", "Asia/Kolkata");
    }
  }

  @Test
  public void testSetTimeZoneWithTransactionCommit() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      connection.createStatement().execute("set time zone 'UTC'");
      verifySettingValue(connection, "timezone", "UTC");
      connection.commit();
      verifySettingValue(connection, "timezone", "UTC");
    }
  }

  @Test
  public void testSetTimeZoneWithTransactionRollback() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      String originalTimeZone = null;
      try (ResultSet rs = connection.createStatement().executeQuery("SHOW TIMEZONE")) {
        assertTrue(rs.next());
        originalTimeZone = rs.getString(1);
        assertFalse(rs.next());
      }
      assertNotNull(originalTimeZone);
      connection.setAutoCommit(false);

      connection.createStatement().execute("set time zone 'UTC'");
      verifySettingValue(connection, "time zone", "UTC");
      connection.rollback();
      verifySettingValue(connection, "time zone", originalTimeZone);
    }
  }

  @Test
  public void testSetNames() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set names 'utf8'");
      verifySettingValue(connection, "client_encoding", "utf8");
      connection.createStatement().execute("set names 'foo'");
      verifySettingValue(connection, "client_encoding", "foo");
    }
  }

  @Test
  public void testSettingsAreUniqueToConnections() throws SQLException {
    // Verify that each new connection gets a separate set of settings.
    for (int connectionNum = 0; connectionNum < 5; connectionNum++) {
      try (Connection connection = DriverManager.getConnection(createUrl())) {
        // Verify that the initial value is 'PostgreSQL JDBC Driver'.
        verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
        connection.createStatement().execute("set application_name to \"my-application\"");
        verifySettingValue(connection, "application_name", "my-application");
      }
    }
  }

  @Test
  public void testSettingInConnectionOptions() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            createUrl()
                + "?options=-c%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction")) {
      verifySettingValue(
          connection, "spanner.ddl_transaction_mode", "AutocommitExplicitTransaction");
    }
  }

  @Test
  public void testMultipleSettingsInConnectionOptions() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            createUrl() + "?options=-c%20spanner.setting1=value1%20-c%20spanner.setting2=value2")) {
      verifySettingValue(connection, "spanner.setting1", "value1");
      verifySettingValue(connection, "spanner.setting2", "value2");
    }
  }

  @Test
  public void testServerVersionInConnectionOptions() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(createUrl() + "?options=-c%20server_version=4.1")) {
      verifySettingValue(connection, "server_version", "4.1");
      verifySettingValue(connection, "server_version_num", "40001");
    }
  }

  @Test
  public void testCustomServerVersionInConnectionOptions() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            createUrl() + "?options=-c%20server_version=5.2 custom version")) {
      verifySettingValue(connection, "server_version", "5.2 custom version");
      verifySettingValue(connection, "server_version_num", "50002");
    }
  }

  @Test
  public void testSetConnectionApiOptionInConnectionOptions() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            createUrl() + "?options=-c%20spanner.autocommit_dml_mode='partitioned_non_atomic'")) {
      verifySettingValue(connection, "spanner.autocommit_dml_mode", "PARTITIONED_NON_ATOMIC");
    }
  }

  @Test
  public void testSetInvalidConnectionApiOptionInConnectionOptions() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            createUrl() + "?options=-c%20spanner.read_only_staleness='foo'")) {
      verifySettingValue(connection, "spanner.read_only_staleness", "STRONG");
    }
  }

  @Test
  public void testSetInvalidAndValidConnectionApiOptionInConnectionOptions() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            createUrl()
                + "?options=-c%20spanner.read_only_staleness='foo'"
                + "%20-c%20spanner.autocommit_dml_mode='partitioned_non_atomic'")) {
      verifySettingValue(connection, "spanner.read_only_staleness", "STRONG");
      verifySettingValue(connection, "spanner.autocommit_dml_mode", "PARTITIONED_NON_ATOMIC");
    }
  }

  @Test
  public void testSelectPgType() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("with " + PG_TYPE_PREFIX + "\nselect * from pg_type"), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from pg_catalog.pg_type")) {
        // The result is not consistent with selecting from pg_type, but that's not relevant here.
        // We just want to ensure that it includes the correct common table expressions.
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testSelectPgTypeAndPgNamespace() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with "
                    + PG_TYPE_PREFIX
                    + "\nselect * from pg_type join pg_namespace on pg_type.typnamespace=pg_namespace.oid"),
            SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "select * from pg_catalog.pg_type "
                      + "join pg_catalog.pg_namespace on pg_type.typnamespace=pg_namespace.oid")) {
        // The result is not consistent with selecting from pg_type, but that's not relevant here.
        // We just want to ensure that it includes the correct common table expressions.
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testDefaultReplacePgCatalogTables() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.replace_pg_catalog_tables")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
        assertFalse(resultSet.next());
      }
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.of("with " + PG_NAMESPACE_CTE + "\n" + "select * from pg_namespace"),
              SELECT1_RESULTSET));
      // Just verify that this works, we don't care about the result.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from pg_catalog.pg_namespace")) {
        //noinspection StatementWithEmptyBody
        while (resultSet.next()) {}
      }
    }
  }

  @Test
  public void testReplacePgCatalogTablesOff() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            createUrl() + "?options=-c%20spanner.replace_pg_catalog_tables=off")) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.replace_pg_catalog_tables")) {
        assertTrue(resultSet.next());
        assertFalse(resultSet.getBoolean(1));
        assertFalse(resultSet.next());
      }

      // The query will now not be modified by PGAdapter before it is sent to Cloud Spanner.
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.of("select * from pg_catalog.pg_namespace"), SELECT1_RESULTSET));
      // Just verify that this works, we don't care about the result.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from pg_catalog.pg_namespace")) {
        //noinspection StatementWithEmptyBody
        while (resultSet.next()) {}
      }
    }
  }

  @Test
  public void testDescribeStatementWithMoreThan50Parameters() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Force binary transfer + usage of server-side prepared statements.
      connection.unwrap(PGConnection.class).setPrepareThreshold(1);
      String sql =
          String.format(
              "insert into foo values (%s)",
              IntStream.rangeClosed(1, 51).mapToObj(i -> "?").collect(Collectors.joining(",")));
      String pgSql =
          String.format(
              "insert into foo values (%s)",
              IntStream.rangeClosed(1, 51).mapToObj(i -> "$" + i).collect(Collectors.joining(",")));
      ImmutableList<TypeCode> typeCodes =
          ImmutableList.copyOf(
              IntStream.rangeClosed(1, 51)
                  .mapToObj(i -> TypeCode.STRING)
                  .collect(Collectors.toList()));
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.of(pgSql),
              com.google.spanner.v1.ResultSet.newBuilder()
                  .setMetadata(createParameterTypesMetadata(typeCodes))
                  .setStats(ResultSetStats.newBuilder().build())
                  .build()));
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        ParameterMetaData metadata = preparedStatement.getParameterMetaData();
        assertEquals(51, metadata.getParameterCount());
      }

      Statement.Builder builder = Statement.newBuilder(pgSql);
      IntStream.rangeClosed(1, 51).forEach(i -> builder.bind("p" + i).to((String) null));
      Statement statement = builder.build();
      mockSpanner.putStatementResult(StatementResult.update(statement, 1L));
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        for (int i = 0; i < 51; i++) {
          preparedStatement.setNull(i + 1, Types.NULL);
        }
        assertEquals(1, preparedStatement.executeUpdate());
      }
    }
  }

  @Test
  public void testDmlReturning() throws SQLException {
    String sql = "INSERT INTO test (value) values ('test') RETURNING id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(ImmutableList.of(TypeCode.INT64), ImmutableList.of("id")))
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("9999").build())
                        .build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(9999L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      try (java.sql.Statement statement = connection.createStatement()) {
        assertTrue(statement.execute(sql));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(9999L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }
  }

  @Test
  public void testDmlReturningMultipleRows() throws SQLException {
    String sql = "UPDATE test SET value='new_value' WHERE value='old_value' RETURNING id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(ImmutableList.of(TypeCode.INT64), ImmutableList.of("id")))
                .setStats(ResultSetStats.newBuilder().setRowCountExact(3L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("3").build())
                        .build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertTrue(resultSet.next());
        assertEquals(2L, resultSet.getLong(1));
        assertTrue(resultSet.next());
        assertEquals(3L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testUUIDParameter() throws SQLException {
    assumeTrue(pgVersion.equals("14.1"));

    String jdbcSql = "SELECT * FROM all_types WHERE col_uuid=?";
    String pgSql = "SELECT * FROM all_types WHERE col_uuid=$1";
    UUID uuid = UUID.randomUUID();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(pgSql).bind("p1").to(uuid.toString()).build(),
            ALL_TYPES_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(jdbcSql)) {
        statement.setObject(1, uuid);
        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());
          assertFalse(resultSet.next());
        }
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testVacuumStatement_noTables() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("vacuum");
    }
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testVacuumStatement_oneTable() throws SQLException {
    String sql = "select * from my_table limit 1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("vacuum my_table");
    }
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(sql, request.getSql());
    assertEquals(QueryMode.PLAN, request.getQueryMode());
  }

  @Test
  public void testVacuumStatement_multipleTables() throws SQLException {
    String sql1 = "select * from my_table1 limit 1";
    String sql2 = "select * from my_table2 limit 1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql1), SELECT1_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), SELECT2_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("vacuum my_table1, my_table2");
    }
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request1 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(sql1, request1.getSql());
    assertEquals(QueryMode.PLAN, request1.getQueryMode());
    ExecuteSqlRequest request2 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql2, request2.getSql());
    assertEquals(QueryMode.PLAN, request1.getQueryMode());
  }

  @Test
  public void testVacuumStatement_oneTableWithColumns() throws SQLException {
    String sql = "select col1,col2 from my_table limit 1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("vacuum my_table (col1, col2)");
    }
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(sql, request.getSql());
    assertEquals(QueryMode.PLAN, request.getQueryMode());
  }

  @Test
  public void testVacuumStatement_multipleTablesWithColumns() throws SQLException {
    String sql1 = "select col1 from my_table1 limit 1";
    String sql2 = "select col1,col2 from my_table2 limit 1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql1), SELECT1_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), SELECT2_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("vacuum my_table1 (col1), my_table2(col1,col2)");
    }
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request1 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(sql1, request1.getSql());
    assertEquals(QueryMode.PLAN, request1.getQueryMode());
    ExecuteSqlRequest request2 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(sql2, request2.getSql());
    assertEquals(QueryMode.PLAN, request1.getQueryMode());
  }

  @Test
  public void testVacuumStatement_unknownTable() throws SQLException {
    String sql = "select * from unknown_table limit 1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.NOT_FOUND
                .withDescription("Table not found: unknown_table")
                .asRuntimeException()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().execute("vacuum unknown_table"));
      assertEquals(
          "ERROR: Table not found: unknown_table - Statement: 'select * from unknown_table limit 1'",
          exception.getMessage());
    }
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(sql, request.getSql());
    assertEquals(QueryMode.PLAN, request.getQueryMode());
  }

  @Test
  public void testVacuumStatementInTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      PSQLException exception =
          assertThrows(PSQLException.class, () -> connection.createStatement().execute("vacuum"));
      assertEquals("ERROR: VACUUM cannot run inside a transaction block", exception.getMessage());
      assertEquals(SQLState.ActiveSqlTransaction.toString(), exception.getSQLState());
    }
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testTruncateTransactional() throws SQLException {
    String sql = "delete from foo";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 10L));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      // TRUNCATE does not return the update count.
      assertEquals(0, connection.createStatement().executeUpdate("truncate foo"));
      connection.rollback();
    }

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertTrue(request.getTransaction().hasBegin());
    assertEquals(1, request.getStatementsCount());
    assertEquals(sql, request.getStatements(0).getSql());
  }

  @Test
  public void testTruncateTransactional_multipleTables() throws SQLException {
    String sql1 = "delete from foo";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql1), 10L));
    String sql2 = "delete from bar";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 10L));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      // TRUNCATE does not return the update count.
      assertEquals(0, connection.createStatement().executeUpdate("truncate foo, bar"));
      connection.commit();
    }

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertTrue(request.getTransaction().hasBegin());
    assertEquals(2, request.getStatementsCount());
    assertEquals(sql1, request.getStatements(0).getSql());
    assertEquals(sql2, request.getStatements(1).getSql());
  }

  @Test
  public void testTruncateAutocommitAtomic() throws SQLException {
    String sql = "delete from foo";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 10L));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(true);
      // TRUNCATE does not return the update count.
      assertEquals(0, connection.createStatement().executeUpdate("truncate foo"));
    }

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertTrue(request.getTransaction().hasBegin());
    assertEquals(1, request.getStatementsCount());
    assertEquals(sql, request.getStatements(0).getSql());
  }

  @Test
  public void testTruncateAutocommitAtomic_multipleTables() throws SQLException {
    String sql1 = "delete from foo";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql1), 10L));
    String sql2 = "delete from bar";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql2),
            Status.FAILED_PRECONDITION
                .withDescription("foreign key constraint violation")
                .asRuntimeException()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(true);
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().executeUpdate("truncate foo, bar"));
      assertTrue(exception.getMessage().contains("foreign key constraint violation"));
    }

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertTrue(request.getTransaction().hasBegin());
    assertEquals(2, request.getStatementsCount());
    assertEquals(sql1, request.getStatements(0).getSql());
    assertEquals(sql2, request.getStatements(1).getSql());
  }

  @Test
  public void testTruncateAutocommitNonAtomic() throws SQLException {
    String sql = "delete from foo";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 10L));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(true);
      connection
          .createStatement()
          .execute("set spanner.autocommit_dml_mode='partitioned_non_atomic'");
      assertEquals(0, connection.createStatement().executeUpdate("truncate foo"));
    }

    // PDML transactions are not committed.
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testForceAutocommit() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Force PGAdapter to use autocommit mode in all cases.
        statement.execute("set spanner.force_autocommit to on");
        // Set the connection to transactional mode. This will be ignored by PGAdapter.
        connection.setAutoCommit(false);
        statement.execute(SELECT1.getSql());
        statement.execute(UPDATE_STATEMENT.getSql());
        statement.execute(INSERT_STATEMENT.getSql());
      }
    }
    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    ExecuteSqlRequest selectRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(selectRequest.getSql(), SELECT1.getSql());
    assertTrue(selectRequest.getTransaction().hasSingleUse());
    assertTrue(selectRequest.getTransaction().getSingleUse().hasReadOnly());
    ExecuteSqlRequest updateRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertTrue(updateRequest.getTransaction().hasBegin());
    assertTrue(updateRequest.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest insertRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertTrue(insertRequest.getTransaction().hasBegin());
    assertTrue(insertRequest.getTransaction().getBegin().hasReadWrite());
  }

  @Test
  public void testForceAutocommitWithPdml() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Force PGAdapter to use autocommit mode in all cases.
        statement.execute("set spanner.force_autocommit to on");
        statement.execute("set spanner.autocommit_dml_mode to 'partitioned_non_atomic'");
        // Set the connection to transactional mode. This will be ignored by PGAdapter.
        connection.setAutoCommit(false);
        statement.execute(UPDATE_STATEMENT.getSql());
        statement.execute(INSERT_STATEMENT.getSql());
      }
    }
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    // The update statement should use a partitioned DML transaction.
    ExecuteSqlRequest updateRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertFalse(updateRequest.getTransaction().hasBegin());
    assertTrue(updateRequest.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    assertEquals(
        1,
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).stream()
            .filter(request -> request.getOptions().hasPartitionedDml())
            .count());
    // The insert statement will use a normal read/write transaction.
    ExecuteSqlRequest insertRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertTrue(insertRequest.getTransaction().hasBegin());
    assertTrue(insertRequest.getTransaction().getBegin().hasReadWrite());
  }

  @Test
  public void testInsertPdml() throws SQLException {
    String sql = "insert into my_table (id, value) values (1, 'one')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.FAILED_PRECONDITION
                .withDescription(
                    "insert statements are not allowed in Partitioned DML transactions")
                .asRuntimeException()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        statement.execute("set spanner.autocommit_dml_mode to 'partitioned_non_atomic'");
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertEquals(
            "ERROR: insert statements are not allowed in Partitioned DML transactions",
            exception.getMessage());
      }
    }
    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    assertEquals(
        1,
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).stream()
            .filter(request -> request.getOptions().hasPartitionedDml())
            .count());
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    assertEquals(
        1,
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .count());
  }

  @Test
  public void testTruncateInDdlBatch() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("start batch ddl");
      // TRUNCATE is not supported in DDL batches.
      SQLException exception =
          assertThrows(
              SQLException.class, () -> connection.createStatement().executeUpdate("truncate foo"));
      assertEquals("ERROR: Cannot execute TRUNCATE in a DDL batch", exception.getMessage());
    }

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
  }

  @Test
  public void testTruncateInDmlBatch() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("start batch dml");
      // TRUNCATE is not supported in DDL batches.
      SQLException exception =
          assertThrows(
              SQLException.class, () -> connection.createStatement().executeUpdate("truncate foo"));
      assertEquals("ERROR: Cannot execute TRUNCATE in a DML batch", exception.getMessage());
    }

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
  }

  @Test
  public void testDescribeTruncate() throws SQLException {
    String sql = "delete from foo";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 10L));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.unwrap(PGConnection.class).setPrepareThreshold(-1);
      try (PreparedStatement preparedStatement = connection.prepareStatement("truncate foo")) {
        assertEquals(0, preparedStatement.executeUpdate());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertTrue(request.getTransaction().hasBegin());
    assertEquals(1, request.getStatementsCount());
    assertEquals(sql, request.getStatements(0).getSql());
  }

  @Test
  public void testUnnamedSavepoint() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      Savepoint savepoint = connection.setSavepoint();
      assertNotNull(savepoint);
      assertEquals(0, savepoint.getSavepointId());

      Savepoint savepoint2 = connection.setSavepoint();
      assertEquals(1, savepoint2.getSavepointId());
    }
  }

  @Test
  public void testNamedSavepoint() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      assertEquals("my_savepoint", connection.setSavepoint("my_savepoint").getSavepointName());
    }
  }

  @Test
  public void testReleaseSavepoint() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      Savepoint savepoint = connection.setSavepoint("my_savepoint");
      connection.releaseSavepoint(savepoint);
    }
  }

  @Test
  public void testRollbackToSavepoint() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      Savepoint savepoint = connection.setSavepoint("my_savepoint");
      connection.rollback(savepoint);
    }
  }

  @Test
  public void testDescribeAndExecute() throws SQLException {
    String sql = "delete from foo";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 10L));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.unwrap(PGConnection.class).setPrepareThreshold(-1);
      try (PreparedStatement preparedStatement = connection.prepareStatement("truncate foo")) {
        assertEquals(0, preparedStatement.executeUpdate());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertTrue(request.getTransaction().hasBegin());
    assertEquals(1, request.getStatementsCount());
    assertEquals(sql, request.getStatements(0).getSql());
  }

  @Test
  public void testRemoveForUpdate() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("/*@ LOCK_SCANNED_RANGES=exclusive */select 1 from my_table where id=1"),
            SELECT1_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select 2 from my_table where id=1 for update"), SELECT2_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection
              .createStatement()
              // This 'for update' clause will be replaced with a lock_scanned_ranges hint.
              .executeQuery("select 1 from my_table where id=1 for update")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));
        assertFalse(resultSet.next());
      }
      connection.createStatement().execute("set spanner.replace_for_update to off");
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("select 2 from my_table where id=1 for update")) {
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getInt(1));
        assertFalse(resultSet.next());
      }

      // FOR UPDATE is not removed if 'delay transaction start' is enabled. This prevents unexpected
      // behavior if the SELECT ... FOR UPDATE statement is executed without a transaction.
      connection.createStatement().execute("set spanner.replace_for_update to on");
      connection
          .createStatement()
          .execute("set spanner.delay_transaction_start_until_first_write=true");
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("select 2 from my_table where id=1 for update")) {
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getInt(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testEmulatePgClass() throws SQLException {
    String withEmulation = "with " + EMULATED_PG_CLASS_PREFIX + "\nselect 1 from pg_class";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(withEmulation), SELECT1_RESULTSET));
    String withoutEmulation = "with " + PG_CLASS_PREFIX + "\nselect 1 from pg_class";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(withoutEmulation), EMPTY_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("with " + PgCollation.PG_COLLATION_CTE + "\nselect 1 from pg_collation"),
            SELECT1_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("with " + PgExtension.PG_EXTENSION_CTE + "\nselect 1 from pg_extension"),
            SELECT1_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with " + PgAttribute.EMPTY_PG_ATTRIBUTE_CTE + "\nselect 1 from pg_attribute"),
            EMPTY_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("with " + EMULATED_PG_ATTRIBUTE_PREFIX + "\nselect 1 from pg_attribute"),
            SELECT1_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("with " + PgAttrdef.EMPTY_PG_ATTRDEF_CTE + "\nselect 1 from pg_attrdef"),
            EMPTY_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("with " + PgAttrdef.PG_ATTRDEF_CTE + "\nselect 1 from pg_attrdef"),
            SELECT1_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with " + PgConstraint.EMPTY_PG_CONSTRAINT_CTE + "\nselect 1 from pg_constraint"),
            EMPTY_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with " + PgConstraint.PG_CONSTRAINT_CTE + "\nselect 1 from pg_constraint"),
            SELECT1_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("with " + PgIndex.EMPTY_PG_INDEX_CTE + "\nselect 1 from pg_index"),
            EMPTY_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("with " + PgIndex.PG_INDEX_CTE + "\nselect 1 from pg_index"),
            SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      for (boolean emulate : new boolean[] {true, false}) {
        connection.createStatement().execute("set spanner.emulate_pg_class_tables=" + emulate);
        try (ResultSet resultSet =
            connection.createStatement().executeQuery("select 1 from pg_class")) {
          if (emulate) {
            assertTrue(resultSet.next());
            assertEquals(1L, resultSet.getLong(1));
          }
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            connection.createStatement().executeQuery("select 1 from pg_collation")) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            connection.createStatement().executeQuery("select 1 from pg_extension")) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            connection.createStatement().executeQuery("select 1 from pg_attribute")) {
          if (emulate) {
            assertTrue(resultSet.next());
            assertEquals(1L, resultSet.getLong(1));
          }
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            connection.createStatement().executeQuery("select 1 from pg_attrdef")) {
          if (emulate) {
            assertTrue(resultSet.next());
            assertEquals(1L, resultSet.getLong(1));
          }
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            connection.createStatement().executeQuery("select 1 from pg_constraint")) {
          if (emulate) {
            assertTrue(resultSet.next());
            assertEquals(1L, resultSet.getLong(1));
          }
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            connection.createStatement().executeQuery("select 1 from pg_index")) {
          if (emulate) {
            assertTrue(resultSet.next());
            assertEquals(1L, resultSet.getLong(1));
          }
          assertFalse(resultSet.next());
        }
      }
    }
    assertEquals(
        1,
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(withEmulation))
            .count());
    assertEquals(
        1,
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(withoutEmulation))
            .count());
  }

  @Test
  public void testShowDatabaseDdl() throws SQLException {
    String ddl = "CREATE TABLE table1 (id bigint primary key, value varchar)";
    mockDatabaseAdmin.addResponse(GetDatabaseDdlResponse.newBuilder().addStatements(ddl).build());

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery("show database ddl")) {
        assertTrue(resultSet.next());
        assertEquals(ddl + ";", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowDatabaseDdlForPostgreSQL() throws SQLException {
    String createNormalTable = "CREATE TABLE table1 (id bigint primary key, value varchar)";
    String createInterleavedTable =
        "CREATE TABLE table2 (id bigint primary key, value varchar) INTERLEAVE IN PARENT table1";
    String createSqlSecurityInvokerView =
        "CREATE VIEW view1 SQL SECURITY INVOKER AS SELECT id from table1";
    String createTtlTable =
        "CREATE TABLE table2 (id bigint primary key, value varchar, ts timestamptz) TTL INTERVAL '3 days' ON ts";
    String createInterleavedTtlTable =
        "CREATE TABLE table2 (id bigint primary key, value varchar, ts timestamptz) INTERLEAVE IN PARENT table1 TTL INTERVAL '3 days' ON ts";
    String createInterleavedCascadeTtlTable =
        "CREATE TABLE table2 (id bigint primary key, value varchar, ts timestamptz) INTERLEAVE IN PARENT table1 ON DELETE CASCADE\nTTL INTERVAL '3 days' ON ts";
    String createInterleavedNoActionTtlTable =
        "CREATE TABLE table2 (id bigint primary key, value varchar, ts timestamptz) INTERLEAVE IN PARENT table1 ON DELETE NO ACTION\nTTL INTERVAL '3 days' ON ts";
    String createChangeStream = "CREATE CHANGE STREAM my_stream FOR ALL";
    String createInterleavedIndex = "CREATE INDEX my_index ON table2 (value) INTERLEAVE IN table1";
    mockDatabaseAdmin.addResponse(
        GetDatabaseDdlResponse.newBuilder()
            .addStatements(createNormalTable)
            .addStatements(createInterleavedTable)
            .addStatements(createSqlSecurityInvokerView)
            .addStatements(createTtlTable)
            .addStatements(createInterleavedTtlTable)
            .addStatements(createInterleavedCascadeTtlTable)
            .addStatements(createInterleavedNoActionTtlTable)
            .addStatements(createChangeStream)
            .addStatements(createInterleavedIndex)
            .build());

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show database ddl for postgresql")) {
        assertTrue(resultSet.next());
        assertEquals(createNormalTable + ";", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals(
            "CREATE TABLE table2 (id bigint primary key, value varchar) /* INTERLEAVE IN PARENT table1 */;",
            resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals(
            "CREATE VIEW view1 /* SQL SECURITY INVOKER */ AS SELECT id from table1;",
            resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals(
            "CREATE TABLE table2 (id bigint primary key, value varchar, ts timestamptz) /* TTL INTERVAL '3 days' ON ts */;",
            resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals(
            "CREATE TABLE table2 (id bigint primary key, value varchar, ts timestamptz) /* INTERLEAVE IN PARENT table1 TTL INTERVAL '3 days' ON ts */;",
            resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals(
            "CREATE TABLE table2 (id bigint primary key, value varchar, ts timestamptz) /* INTERLEAVE IN PARENT table1 ON DELETE CASCADE\nTTL INTERVAL '3 days' ON ts */;",
            resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals(
            "CREATE TABLE table2 (id bigint primary key, value varchar, ts timestamptz) /* INTERLEAVE IN PARENT table1 ON DELETE NO ACTION\nTTL INTERVAL '3 days' ON ts */;",
            resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("/* CREATE CHANGE STREAM my_stream FOR ALL */;", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals(
            "CREATE INDEX my_index ON table2 (value) /* INTERLEAVE IN table1 */;",
            resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testTransactionAbortedByCloudSpanner() throws SQLException {
    String sql = "SELECT * FROM random";
    RandomResultSetGenerator generator = new RandomResultSetGenerator(5, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), generator.generate()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          // just consume the results.
        }
      }
      // Change the results to force any retry of the transaction to fail.
      mockSpanner.putStatementResult(
          StatementResult.query(Statement.of(sql), generator.generate()));
      mockSpanner.abortNextStatement();
      PSQLException exception = assertThrows(PSQLException.class, connection::commit);
      assertNotNull(exception.getServerErrorMessage());
      assertEquals(
          SQLState.SerializationFailure.toString(),
          exception.getServerErrorMessage().getSQLState());
    }
  }

  @Ignore("Only used for manual performance testing")
  @Test
  public void testBasePerformance() throws SQLException {
    final int numRuns = 1000;
    String sql = "select * from random_benchmark";
    RandomResultSetGenerator generator = new RandomResultSetGenerator(10, Dialect.POSTGRESQL);
    for (int run = 0; run < numRuns; run++) {
      mockSpanner.putStatementResult(
          StatementResult.query(Statement.of(sql + run), generator.generate()));
    }
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      Stopwatch watch = Stopwatch.createStarted();
      for (int run = 0; run < numRuns; run++) {
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql + run)) {
          while (resultSet.next()) {
            // ignore
          }
        }
      }
      System.out.printf("Elapsed: %dms\n", watch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  private void verifySettingIsNull(Connection connection, String setting) throws SQLException {
    try (ResultSet resultSet =
        connection.createStatement().executeQuery(String.format("show %s", setting))) {
      assertTrue(resultSet.next());
      assertNull(resultSet.getString(1));
      assertFalse(resultSet.next());
    }
  }

  private void verifySettingValue(Connection connection, String setting, String value)
      throws SQLException {
    try (ResultSet resultSet =
        connection.createStatement().executeQuery(String.format("show %s", setting))) {
      assertTrue(resultSet.next());
      assertEquals(value, resultSet.getString(1));
      assertFalse(resultSet.next());
    }
  }

  private void verifySettingIsUnrecognized(Connection connection, String setting) {
    SQLException exception =
        assertThrows(
            SQLException.class,
            () -> connection.createStatement().execute(String.format("show %s", setting)));
    assertEquals(
        String.format("ERROR: unrecognized configuration parameter \"%s\"", setting),
        exception.getMessage());
  }
}
