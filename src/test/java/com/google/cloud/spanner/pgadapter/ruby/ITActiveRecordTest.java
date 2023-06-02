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

package com.google.cloud.spanner.pgadapter.ruby;

import static com.google.cloud.spanner.pgadapter.ruby.AbstractRubyMockServerTest.createVirtualEnv;
import static com.google.cloud.spanner.pgadapter.ruby.AbstractRubyMockServerTest.run;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITActiveRecordTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();

  private static Database database;

  private static final String DIRECTORY_NAME = "./src/test/ruby/activerecord";

  @BeforeClass
  public static void installGems() throws Exception {}

  @BeforeClass
  public static void setup() throws Exception {
    createVirtualEnv(DIRECTORY_NAME);
    testEnv.setUp();
    database = testEnv.createDatabase(ImmutableList.of());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Test
  public void testMigrateAndRun() throws Exception {
    run(
        new String[] {"bundle", "exec", "rake", "db:migrate"},
        DIRECTORY_NAME,
        ImmutableMap.of(
            "PGHOST", "localhost", "PGPORT", String.valueOf(testEnv.getServer().getLocalPort())));
    run(
        new String[] {"bundle", "exec", "rake", "run"},
        DIRECTORY_NAME,
        ImmutableMap.of(
            "PGHOST",
            "localhost",
            "PGPORT",
            String.valueOf(testEnv.getServer().getLocalPort()),
            "RAILS_ENV",
            "development"));
    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:postgresql://localhost:%d/%s",
                testEnv.getServer().getLocalPort(), database.getId().getDatabase()))) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select count(1) from singers")) {
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select count(1) from albums")) {
        assertTrue(resultSet.next());
        assertEquals(30, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select count(1) from tracks")) {
        assertTrue(resultSet.next());
        assertEquals(300, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }
}
