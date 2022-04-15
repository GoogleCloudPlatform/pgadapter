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

import static org.junit.Assert.assertNull;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import java.math.BigDecimal;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITPgxTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;
  private static PgxTest pgxTest;

  @Parameter public String preferQueryMode;

  @Parameters(name = "preferQueryMode = {0}")
  public static Object[] data() {
    return new Object[] {"extended", "simple"};
  }

  @BeforeClass
  public static void setup() throws Exception {
    pgxTest = PgxTest.compile();

    testEnv.setUp();
    database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServer(database.getId(), Collections.emptyList());
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
            + "col_varchar varchar(100))");
  }

  private GoString createConnString() {
    return new GoString(
        String.format(
            "postgres://uid:pwd@localhost:%d/?statement_cache_capacity=0&sslmode=disable",
            testEnv.getServer().getLocalPort()));
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
                .set("col_varchar")
                .to("test")
                .build()));
  }

  @Test
  public void testSelectHelloWorld() {
    assertNull(pgxTest.TestHelloWorld(createConnString()));
  }

  @Test
  public void testQueryAllDataTypes() {
    assertNull(pgxTest.TestQueryAllDataTypes(createConnString(), false));
  }

  @Test
  public void testInsertAllDataTypes() {
    // Make sure there is no row that already exists with col_bigint=100
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", Key.of(100L))));

    assertNull(pgxTest.TestInsertAllDataTypes(createConnString(), false));
  }

  @Ignore("Untyped null values are not supported by the backend yet")
  @Test
  public void testInsertNullsAllDataTypes() {
    // Make sure there is no row that already exists with col_bigint=100
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId, Collections.singletonList(Mutation.delete("all_types", Key.of(100L))));

    assertNull(pgxTest.TestInsertNullsAllDataTypes(createConnString(), false));
  }
}
