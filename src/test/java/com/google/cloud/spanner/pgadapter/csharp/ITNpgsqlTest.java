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
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
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
        "Host=%s;Port=%d;Database=d;SSL Mode=Disable", host, testEnv.getServer().getLocalPort());
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
}
