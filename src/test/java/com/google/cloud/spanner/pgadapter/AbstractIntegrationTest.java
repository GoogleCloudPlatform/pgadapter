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

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class AbstractIntegrationTest implements IntegrationTest {
  private static final String PG_ADAPTER_ADDRESS = System.getProperty("PG_ADAPTER_ADDRESS", null);
  private static final String PG_ADAPTER_LOCAL_PORT =
      System.getProperty("PG_ADAPTER_LOCAL_PORT", null);
  protected static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static ProxyServer server;
  private static Database database;
  private static boolean datamodelCreated;

  @BeforeClass
  public static void createDatabaseAndStartPGAdapter() throws Exception {
    testEnv.setUp();
    if (testEnv.isUseExistingDb()) {
      database = testEnv.getExistingDatabase();
    } else {
      database = testEnv.createDatabase();
    }
    if (PG_ADAPTER_ADDRESS == null || PG_ADAPTER_LOCAL_PORT == null) {
      String credentials = testEnv.getCredentials();
      ImmutableList.Builder<String> argsListBuilder =
          ImmutableList.<String>builder()
              .add(
                  "-p",
                  testEnv.getProjectId(),
                  "-i",
                  testEnv.getInstanceId(),
                  "-d",
                  database.getId().getDatabase(),
                  "-s",
                  String.valueOf(testEnv.getPort()));
      if (testEnv.getSpannerUrl() != null) {
        argsListBuilder.add("-e", testEnv.getSpannerUrl());
      }
      if (credentials != null) {
        argsListBuilder.add("-c", testEnv.getCredentials());
      }
      String[] args = argsListBuilder.build().toArray(new String[0]);
      server = new ProxyServer(new OptionsMetadata(args));
      server.startServer();
    }
  }

  protected static ProxyServer getServer() {
    return server;
  }

  /**
   * Returns the DDL statements that should be executed for this test. Override in a concrete
   * subclass if you want a different data model than the default.
   */
  protected Iterable<String> getDdlStatements() {
    return ImmutableList.of(
        "create table numbers (num int not null primary key, name varchar(100))",
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

  @Before
  public void createDataModel() throws Exception {
    if (!testEnv.isUseExistingDb() && !datamodelCreated) {
      testEnv.updateDdl(database.getId().getDatabase(), getDdlStatements());
      datamodelCreated = true;
    }
  }

  protected static Database getDatabase() {
    return database;
  }

  protected static String getPGAdapterHostAndPort() {
    if (server != null) {
      return String.format("localhost:%d", server.getLocalPort());
    }
    return String.format("%s:%s", PG_ADAPTER_ADDRESS, PG_ADAPTER_LOCAL_PORT);
  }

  protected static String getPGAdapterHost() {
    if (server != null) {
      return "localhost";
    }
    return PG_ADAPTER_ADDRESS;
  }

  protected static int getPGAdapterPort() {
    if (server != null) {
      return server.getLocalPort();
    }
    return Integer.parseInt(PG_ADAPTER_LOCAL_PORT);
  }

  protected static void waitForServer() throws Exception {
    if (server != null) {
      testEnv.waitForServer(server);
    }
  }

  @AfterClass
  public static void teardown() {
    if (server != null) {
      server.stopServer();
    }
    testEnv.cleanUp();
  }
}
