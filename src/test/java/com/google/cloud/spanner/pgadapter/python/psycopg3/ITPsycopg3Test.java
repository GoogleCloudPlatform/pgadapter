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

package com.google.cloud.spanner.pgadapter.python.psycopg3;

import static com.google.cloud.spanner.pgadapter.python.psycopg3.Psycopg3MockServerTest.DIRECTORY_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.python.PythonTestUtil;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class ITPsycopg3Test {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled()
        ? new Object[] {"localhost", "/tmp"}
        : new Object[] {"localhost"};
  }

  @BeforeClass
  public static void setup() throws Exception {
    PythonTestUtil.createVirtualEnv(DIRECTORY_NAME);
    testEnv.setUp();
    database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  private static Iterable<String> getDdlStatements() {
    return Collections.singletonList(
        "create table all_types ("
            + "col_bigint bigint not null primary key, "
            + "col_bool bool, "
            + "col_bytea bytea, "
            + "col_float8 float8, "
            + "col_int int, "
            + "col_numeric numeric, "
            + "col_timestamptz timestamptz, "
            + "col_date date, "
            + "col_varchar varchar(100),"
            + "col_jsonb jsonb)");
  }

  private String createConnectionString() {
    return String.format(
        "host=%s port=%d dbname=d sslmode=disable", host, testEnv.getServer().getLocalPort());
  }

  @Before
  public void insertTestData() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        Collections.singletonList(
            Mutation.newInsertOrUpdateBuilder("all_types")
                .set("col_bigint")
                .to(1L)
                .set("col_bool")
                .to(true)
                .set("col_bytea")
                .to(ByteArray.copyFrom("test"))
                .set("col_float8")
                .to(3.14d)
                .set("col_int")
                .to(100)
                .set("col_numeric")
                .to(new BigDecimal("6.626"))
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("2022-02-16T14:18:02.123456789+01:00"))
                .set("col_date")
                .to(Date.parseDate("2022-03-29"))
                .set("col_varchar")
                .to("test")
                .set("col_jsonb")
                .to("{\"key\": \"value\"}")
                .build()));
  }

  @After
  public void clearTestData() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", KeySet.all())));
  }

  String execute(String method) throws Exception {
    return Psycopg3MockServerTest.execute(method, createConnectionString());
  }

  @Test
  public void testShowServerVersion() throws Exception {
    String result = execute("show_server_version");
    assertEquals("14.1\n", result);
  }

  @Test
  public void testShowApplicationName() throws Exception {
    String actualOutput = execute("show_application_name");
    assertEquals("None\n", actualOutput);
  }

  @Test
  public void testSelect1() throws Exception {
    String sql = "SELECT 1";

    String actualOutput = execute("select1");
    String expectedOutput = "(1,)\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testQueryAllDataTypes() throws Exception {
    String result = execute("query_all_data_types");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n",
        result);
  }

  @Test
  public void testQueryAllDataTypesWithParameter() throws Exception {
    String result = execute("query_all_data_types_with_parameter");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n",
        result);
  }

  @Test
  public void testQueryAllDataTypesText() throws Exception {
    testQueryAllDataTypesWithFixedFormat(DataFormat.POSTGRESQL_TEXT);
  }

  @Test
  public void testQueryAllDataTypesBinary() throws Exception {
    testQueryAllDataTypesWithFixedFormat(DataFormat.POSTGRESQL_BINARY);
  }

  private void testQueryAllDataTypesWithFixedFormat(DataFormat format) throws Exception {
    String result =
        execute(
            "query_all_data_types_" + (format == DataFormat.POSTGRESQL_BINARY ? "binary" : "text"));
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n",
        result);
  }

  @Test
  public void testUpdateAllDataTypes() throws Exception {
    String result = execute("update_all_data_types");
    assertEquals("Update count: 1\n", result);
  }

  @Test
  public void testInsertAllDataTypes() throws Exception {
    // Make sure there is no row that already exists with col_bigint=100
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", Key.of(100L))));

    String result = execute("insert_all_data_types");
    assertEquals("Insert count: 1\n", result);
  }

  @Test
  public void testInsertAllDataTypesBinary() throws Exception {
    // Make sure there is no row that already exists with col_bigint=100
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", Key.of(100L))));

    String result = execute("insert_all_data_types_binary");
    assertEquals("Insert count: 1\n", result);
  }

  @Test
  public void testInsertAllDataTypesText() throws Exception {
    // Make sure there is no row that already exists with col_bigint=100
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", Key.of(100L))));

    String result = execute("insert_all_data_types_text");
    assertEquals("Insert count: 1\n", result);
  }

  @Test
  public void testInsertNullsAllDataTypes() throws Exception {
    // Make sure there is no row that already exists with col_bigint=100
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", Key.of(100L))));

    String result = execute("insert_nulls_all_data_types");
    assertEquals("Insert count: 1\n", result);
  }

  @Test
  public void testInsertAllDataTypesReturning() throws Exception {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", KeySet.all())));

    String result = execute("insert_all_data_types_returning");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n",
        result);
  }

  @Test
  public void testInsertBatch() throws Exception {
    // Make sure the table is empty before we execute the test.
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", KeySet.all())));
    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM all_types"))) {
      assertTrue(resultSet.next());
      assertEquals(0L, resultSet.getLong(0));
      assertFalse(resultSet.next());
    }

    String result = execute("insert_batch");
    assertEquals("Insert count: 10\n", result);

    // Verify that we really received 10 rows.
    final long batchSize = 10L;
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM all_types"))) {
      assertTrue(resultSet.next());
      assertEquals(batchSize, resultSet.getLong(0));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testMixedBatch() throws Exception {
    // Make sure the table is empty before we execute the test.
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", KeySet.all())));
    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM all_types"))) {
      assertTrue(resultSet.next());
      assertEquals(0L, resultSet.getLong(0));
      assertFalse(resultSet.next());
    }

    String result = execute("mixed_batch");
    assertEquals("Insert count: 5\n" + "Count: (3,)\n" + "Update count: 3\n", result);

    final long batchSize = 5L;
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM all_types"))) {
      assertTrue(resultSet.next());
      assertEquals(batchSize, resultSet.getLong(0));
      assertFalse(resultSet.next());
    }
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(Statement.of("SELECT COUNT(*) FROM all_types WHERE col_bool=true"))) {
      assertTrue(resultSet.next());
      assertEquals(0L, resultSet.getLong(0));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testBatchExecutionError() throws Exception {
    // Add a new record to cause the batch insert to fail with an ALREADY_EXISTS error.
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        Collections.singletonList(
            Mutation.newInsertOrUpdateBuilder("all_types")
                .set("col_bigint")
                .to(101L)
                .set("col_bool")
                .to(true)
                .set("col_bytea")
                .to(ByteArray.copyFrom("test"))
                .set("col_float8")
                .to(3.14d)
                .set("col_int")
                .to(100)
                .set("col_numeric")
                .to(new BigDecimal("6.626"))
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("2022-02-16T14:18:02.123456789+01:00"))
                .set("col_date")
                .to(Date.parseDate("2022-03-29"))
                .set("col_varchar")
                .to("test")
                .set("col_jsonb")
                .to("{\"key\": \"value\"}")
                .build()));

    String result = execute("batch_execution_error");
    assertTrue(result, result.contains("Executing batch failed with error"));
    assertTrue(result, result.contains("Row [101] in table all_types already exists"));
  }

  @Test
  public void testBinaryCopyIn() throws Exception {
    testCopyIn("binary_copy_in");
  }

  @Test
  public void testTextCopyIn() throws Exception {
    testCopyIn("text_copy_in");
  }

  private void testCopyIn(String testMethod) throws Exception {
    // Make sure the table is empty before we execute the test.
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", KeySet.all())));

    String result = execute(testMethod);
    assertEquals("Copy count: 2\n", result);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(Statement.of("SELECT * FROM all_types ORDER BY col_bigint"))) {
      assertTrue(resultSet.next());
      int col = 0;
      assertEquals(1L, resultSet.getLong(col++));
      assertTrue(resultSet.getBoolean(col++));
      assertArrayEquals(
          "test_bytes".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(col++).toByteArray());
      assertEquals(3.14d, resultSet.getDouble(col++), 0.0d);
      assertEquals(10L, resultSet.getLong(col++));
      assertEquals("6.626", resultSet.getString(col++));
      assertEquals(
          Timestamp.parseTimestamp("2022-03-24T12:39:10.123456000Z"),
          resultSet.getTimestamp(col++));
      assertEquals(Date.parseDate("2022-07-01"), resultSet.getDate(col++));
      assertEquals("test", resultSet.getString(col++));
      assertEquals("{\"key\": \"value\"}", resultSet.getPgJsonb(col++));

      assertTrue(resultSet.next());
      col = 0;
      assertEquals(2L, resultSet.getLong(col++));
      assertEquals(10, resultSet.getColumnCount());
      for (col = 1; col < resultSet.getColumnCount(); col++) {
        assertTrue(resultSet.isNull(col));
      }

      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testBinaryCopyOut() throws Exception {
    // Add an extra NULL-row to the table.
    addNullRow();

    String result = execute("binary_copy_out");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n"
            + "col_bigint: 2\n"
            + "col_bool: None\n"
            + "col_bytea: None\n"
            + "col_float8: None\n"
            + "col_int: None\n"
            + "col_numeric: None\n"
            + "col_timestamptz: None\n"
            + "col_date: None\n"
            + "col_string: None\n"
            + "col_jsonb: None\n",
        result);
  }

  @Test
  public void testTextCopyOut() throws Exception {
    // Add an extra NULL-row to the table.
    addNullRow();

    String result = execute("text_copy_out");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n"
            + "col_bigint: 2\n"
            + "col_bool: None\n"
            + "col_bytea: None\n"
            + "col_float8: None\n"
            + "col_int: None\n"
            + "col_numeric: None\n"
            + "col_timestamptz: None\n"
            + "col_date: None\n"
            + "col_string: None\n"
            + "col_jsonb: None\n",
        result);
  }

  @Test
  public void testPrepareQuery() throws Exception {
    String sql = "SELECT * FROM all_types WHERE col_bigint=$1";
    addNullRow();

    String result = execute("prepare_query");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n"
            + "col_bigint: 2\n"
            + "col_bool: None\n"
            + "col_bytea: None\n"
            + "col_float8: None\n"
            + "col_int: None\n"
            + "col_numeric: None\n"
            + "col_timestamptz: None\n"
            + "col_date: None\n"
            + "col_string: None\n"
            + "col_jsonb: None\n",
        result);
  }

  @Test
  public void testReadWriteTransaction() throws Exception {
    String result = execute("read_write_transaction");
    assertEquals("(1,)\n" + "Insert count: 1\n" + "Insert count: 1\n", result);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM all_types"))) {
      assertTrue(resultSet.next());
      assertEquals(3L, resultSet.getLong(0));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testReadOnlyTransaction() throws Exception {
    String result = execute("read_only_transaction");
    assertEquals("(1,)\n" + "(2,)\n", result);
  }

  private void addNullRow() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        Collections.singletonList(
            Mutation.newInsertOrUpdateBuilder("all_types")
                .set("col_bigint")
                .to(2L)
                .set("col_bool")
                .to((Boolean) null)
                .set("col_bytea")
                .to((ByteArray) null)
                .set("col_float8")
                .to((Double) null)
                .set("col_int")
                .to((Long) null)
                .set("col_numeric")
                .to(com.google.cloud.spanner.Value.pgNumeric(null))
                .set("col_timestamptz")
                .to((Timestamp) null)
                .set("col_date")
                .to((Date) null)
                .set("col_varchar")
                .to((String) null)
                .set("col_jsonb")
                .to(Value.pgJsonb(null))
                .build()));
  }
}
