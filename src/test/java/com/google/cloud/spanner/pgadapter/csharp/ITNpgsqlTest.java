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

package com.google.cloud.spanner.pgadapter.csharp;

import static com.google.cloud.spanner.pgadapter.csharp.AbstractNpgsqlMockServerTest.execute;
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
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
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
public class ITNpgsqlTest implements IntegrationTest {
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
    return PgAdapterTestEnv.getOnlyAllTypesDdl();
  }

  private String createConnectionString() {
    return String.format(
        "Host=%s;Port=%d;Database=d;SSL Mode=Disable;Timeout=60;Command Timeout=60",
        host, testEnv.getServer().getLocalPort());
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
                .set("col_array_bigint")
                .toInt64Array(Arrays.asList(1L, null, 2L))
                .set("col_array_bool")
                .toBoolArray(Arrays.asList(true, null, false))
                .set("col_array_bytea")
                .toBytesArray(
                    Arrays.asList(ByteArray.copyFrom("bytes1"), null, ByteArray.copyFrom("bytes2")))
                .set("col_array_float8")
                .toFloat64Array(Arrays.asList(3.14d, null, -99.99))
                .set("col_array_int")
                .toInt64Array(Arrays.asList(-100L, null, -200L))
                .set("col_array_numeric")
                .toPgNumericArray(Arrays.asList("6.626", null, "-3.14"))
                .set("col_array_timestamptz")
                .toTimestampArray(
                    Arrays.asList(
                        Timestamp.parseTimestamp("2022-02-16T16:18:02.123456Z"),
                        null,
                        Timestamp.parseTimestamp("2000-01-01T00:00:00Z")))
                .set("col_array_date")
                .toDateArray(
                    Arrays.asList(Date.parseDate("2023-02-20"), null, Date.parseDate("2000-01-01")))
                .set("col_array_varchar")
                .toStringArray(Arrays.asList("string1", null, "string2"))
                .set("col_array_jsonb")
                .toPgJsonbArray(
                    Arrays.asList("{\"key\": \"value1\"}", null, "{\"key\": \"value2\"}"))
                .build()));
  }

  @After
  public void clearTestData() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", KeySet.all())));
  }

  @Test
  public void testShowServerVersion() throws IOException, InterruptedException {
    String result = execute("TestShowServerVersion", createConnectionString());
    assertEquals("14.1\n", result);
  }

  @Test
  public void testSelect1() throws IOException, InterruptedException {
    String result = execute("TestSelect1", createConnectionString());
    assertEquals("Success\n", result);
  }

  @Test
  public void testQueryAllDataTypes() throws IOException, InterruptedException {
    String result = execute("TestQueryAllDataTypes", createConnectionString());
    assertEquals("Success\n", result);
  }

  @Test
  public void testUpdateAllDataTypes() throws IOException, InterruptedException {
    String result = execute("TestUpdateAllDataTypes", createConnectionString());
    assertEquals("Success\n", result);
  }

  @Test
  public void testInsertAllDataTypes() throws IOException, InterruptedException {
    // Make sure there is no row that already exists with col_bigint=100
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", Key.of(100L))));

    String result = execute("TestInsertAllDataTypes", createConnectionString());
    assertEquals("Success\n", result);
  }

  @Test
  public void testInsertNullsAllDataTypes() throws IOException, InterruptedException {
    // Make sure there is no row that already exists with col_bigint=100
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", Key.of(100L))));

    String result = execute("TestInsertNullsAllDataTypes", createConnectionString());
    assertEquals("Success\n", result);
  }

  @Test
  public void testInsertAllDataTypesReturning() throws IOException, InterruptedException {
    // Make sure there is no row that already exists with col_bigint=1
    String databaseId = database.getId().getDatabase();
    testEnv.write(databaseId, Collections.singletonList(Mutation.delete("all_types", Key.of(1))));

    String result = execute("TestInsertAllDataTypesReturning", createConnectionString());
    assertEquals("Success\n", result);
  }

  @Test
  public void testInsertBatch() throws IOException, InterruptedException {
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

    String result = execute("TestInsertBatch", createConnectionString());
    assertEquals("Success\n", result);

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
  public void testMixedBatch() throws IOException, InterruptedException {
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

    String result = execute("TestMixedBatch", createConnectionString());
    assertEquals("Success\n", result);

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
  public void testBatchExecutionError() throws IOException, InterruptedException {
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

    String result = execute("TestBatchExecutionError", createConnectionString());
    assertTrue(result, result.contains("Executing batch failed with error"));
    assertTrue(result, result.contains("Row [101] in table all_types already exists"));
  }

  @Test
  public void testBinaryCopyIn() throws IOException, InterruptedException {
    testCopyIn("TestBinaryCopyIn");
  }

  @Test
  public void testTextCopyIn() throws IOException, InterruptedException {
    testCopyIn("TestTextCopyIn");
  }

  private void testCopyIn(String testMethod) throws IOException, InterruptedException {
    // Make sure the table is empty before we execute the test.
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", KeySet.all())));

    String result = execute(testMethod, createConnectionString());
    assertEquals("Success\n", result);

    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(Statement.of("SELECT * FROM all_types ORDER BY col_bigint"))) {
      assertTrue(resultSet.next());
      int col = 0;
      assertEquals(1L, resultSet.getLong(col++));
      assertTrue(resultSet.getBoolean(col++));
      assertArrayEquals(new byte[] {1, 2, 3}, resultSet.getBytes(col++).toByteArray());
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
      assertEquals(20, resultSet.getColumnCount());
      for (col = 1; col < resultSet.getColumnCount(); col++) {
        assertTrue(resultSet.isNull(col));
      }

      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testBinaryCopyOut() throws IOException, InterruptedException {
    // Add an extra NULL-row to the table.
    addNullRow();
    String result = execute("TestBinaryCopyOut", createConnectionString());
    assertEquals(
        "1\tTrue\tdGVzdA==\t3.14\t100\t6.626\t20220216T131802123456\t20220329\ttest\t{\"key\": \"value\"}\t[1, , 2]\t[True, , False]\t[Ynl0ZXMx, , Ynl0ZXMy]\t[3.14, , -99.99]\t[-100, , -200]\t[6.626, , -3.14]\t[20220216T161802123456, , 20000101T000000]\t[20230220, , 20000101]\t[string1, , string2]\t[{\"key\": \"value1\"}, , {\"key\": \"value2\"}]\n"
            + "2\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\n"
            + "Success\n",
        result);
  }

  @Test
  public void testTextCopyOut() throws IOException, InterruptedException {
    // Add an extra NULL-row to the table.
    addNullRow();
    String result = execute("TestTextCopyOut", createConnectionString());
    assertEquals(
        "1\tt\t\\\\x74657374\t3.14\t100\t6.626\t2022-02-16 14:18:02.123456+01\t2022-03-29\ttest\t{\"key\": \"value\"}\t{1,NULL,2}\t{t,NULL,f}\t{\"\\\\\\\\x627974657331\",NULL,\"\\\\\\\\x627974657332\"}\t{3.14,NULL,-99.99}\t{-100,NULL,-200}\t{6.626,NULL,-3.14}\t{\"2022-02-16 17:18:02.123456+01\",NULL,\"2000-01-01 01:00:00+01\"}\t{\"2023-02-20\",NULL,\"2000-01-01\"}\t{\"string1\",NULL,\"string2\"}\t{\"{\\\\\"key\\\\\": \\\\\"value1\\\\\"}\",NULL,\"{\\\\\"key\\\\\": \\\\\"value2\\\\\"}\"}\n"
            + "2\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\n"
            + "Success\n",
        result);
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
                .to(Value.pgNumeric(null))
                .set("col_timestamptz")
                .to((Timestamp) null)
                .set("col_date")
                .to((Date) null)
                .set("col_varchar")
                .to((String) null)
                .set("col_jsonb")
                .to(Value.pgJsonb(null))
                .set("col_array_bigint")
                .toInt64Array((long[]) null)
                .set("col_array_bool")
                .toBoolArray((boolean[]) null)
                .set("col_array_bytea")
                .toBytesArray(null)
                .set("col_array_float8")
                .toFloat64Array((double[]) null)
                .set("col_array_int")
                .toInt64Array((long[]) null)
                .set("col_array_numeric")
                .toPgNumericArray(null)
                .set("col_array_timestamptz")
                .toTimestampArray(null)
                .set("col_array_date")
                .toDateArray(null)
                .set("col_array_varchar")
                .toStringArray(null)
                .set("col_array_jsonb")
                .toPgJsonbArray(null)
                .build()));
  }

  @Test
  public void testSimplePrepare() throws IOException, InterruptedException {
    String result = execute("TestSimplePrepare", createConnectionString());
    assertEquals("Success\n", result);
  }

  @Test
  public void testPrepareAndExecute() throws IOException, InterruptedException {
    String result = execute("TestPrepareAndExecute", createConnectionString());
    assertEquals("Success\n", result);
  }

  @Test
  public void testReadWriteTransaction() throws IOException, InterruptedException {
    String result = execute("TestReadWriteTransaction", createConnectionString());
    assertEquals("Row: 1\n" + "Success\n", result);
    DatabaseClient client = testEnv.getSpanner().getDatabaseClient(database.getId());
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM all_types"))) {
      assertTrue(resultSet.next());
      assertEquals(3L, resultSet.getLong(0));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testReadOnlyTransaction() throws IOException, InterruptedException {
    String result = execute("TestReadOnlyTransaction", createConnectionString());
    assertEquals("Row: 1\n" + "Row: 2\n" + "Success\n", result);
  }
}
