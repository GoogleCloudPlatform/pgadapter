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

package com.google.cloud.spanner.pgadapter.golang;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
public class ITPgx5Test implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;
  private static PgxTest pgxTest;

  @Parameter public String preferQueryMode;

  @Parameter(1)
  public boolean useDomainSocket;

  @Parameters(name = "preferQueryMode = {0}, useDomainSocket = {1}")
  public static List<Object[]> data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    boolean[] useDomainSockets;
    if (options.isDomainSocketEnabled()) {
      useDomainSockets = new boolean[] {true, false};
    } else {
      useDomainSockets = new boolean[] {false};
    }
    String[] queryModes = {"extended", "simple"};
    List<Object[]> parameters = new ArrayList<>();
    for (String queryMode : queryModes) {
      for (boolean useDomainSocket : useDomainSockets) {
        parameters.add(new Object[] {queryMode, useDomainSocket});
      }
    }
    return parameters;
  }

  @BeforeClass
  public static void setup() throws Exception {
    try {
      pgxTest = GolangTest.compile("pgadapter_pgx5_tests/pgx.go", PgxTest.class);
    } catch (UnsatisfiedLinkError unsatisfiedLinkError) {
      // This probably means that there is a version mismatch for GLIBC (or no GLIBC at all
      // installed).
      assumeFalse(
          "Skipping ecosystem test because of missing dependency",
          System.getProperty("allowSkipUnsupportedEcosystemTest", "false")
              .equalsIgnoreCase("true"));
      throw unsatisfiedLinkError;
    }

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
    return PgAdapterTestEnv.DEFAULT_DATA_MODEL;
  }

  private GoString createConnString() {
    if (useDomainSocket) {
      return new GoString(
          String.format(
              "host=/tmp port=%d prefer_simple_protocol=%s",
              testEnv.getServer().getLocalPort(), preferQueryMode.equals("simple")));
    }
    return new GoString(
        String.format(
            "postgres://uid:pwd@localhost:%d/?sslmode=disable&prefer_simple_protocol=%s",
            testEnv.getServer().getLocalPort(), preferQueryMode.equals("simple")));
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
  public void testSelectHelloWorld() {
    assertNull(pgxTest.TestHelloWorld(createConnString()));
  }

  @Test
  public void testQueryAllDataTypes() {
    assertNull(pgxTest.TestQueryAllDataTypes(createConnString(), 0, 0));
  }

  @Test
  public void testInsertAllDataTypes() {
    // Make sure there is no row that already exists with col_bigint=100
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", Key.of(100L))));

    assertNull(pgxTest.TestInsertAllDataTypes(createConnString()));
  }

  @Test
  public void testPrepareStatement() {
    assertNull(pgxTest.TestPrepareStatement(createConnString()));
  }

  @Test
  public void testPrepareSelectStatement() {
    assertNull(pgxTest.TestPrepareSelectStatement(createConnString()));
  }

  @Test
  public void testInsertNullsAllDataTypes() {
    // Make sure there is no row that already exists with col_bigint=100
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", Key.of(100L))));

    assertNull(pgxTest.TestInsertNullsAllDataTypes(createConnString()));
  }

  @Test
  public void testInsertBatch() {
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

    assertNull(pgxTest.TestInsertBatch(createConnString()));

    final long batchSize = 10L;
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM all_types"))) {
      assertTrue(resultSet.next());
      assertEquals(batchSize, resultSet.getLong(0));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testMixedBatch() {
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

    assertNull(pgxTest.TestMixedBatch(createConnString()));

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
