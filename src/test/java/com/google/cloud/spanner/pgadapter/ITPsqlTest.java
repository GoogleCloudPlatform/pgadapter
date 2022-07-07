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

import static com.google.cloud.spanner.pgadapter.PgAdapterTestEnv.DEFAULT_DATA_MODEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spanner.Database;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITPsqlTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  private static final String POSTGRES_HOST = System.getenv("POSTGRES_HOST");
  private static final String POSTGRES_PORT = System.getenv("POSTGRES_PORT");
  private static final String POSTGRES_USER = System.getenv("POSTGRES_USER");
  private static final String POSTGRES_DATABASE = System.getenv("POSTGRES_DATABASE");

  @BeforeClass
  public static void setup() throws Exception {
    assumeTrue("This test requires psql to be installed", isPsqlAvailable());
    assumeFalse("This test the environment variable POSTGRES_HOST to point to a valid PostgreSQL host",
        Strings.isNullOrEmpty(POSTGRES_HOST));
    assumeFalse("This test the environment variable POSTGRES_PORT to point to a valid PostgreSQL port number",
        Strings.isNullOrEmpty(POSTGRES_PORT));
    assumeFalse("This test the environment variable POSTGRES_USER to point to a valid PostgreSQL user",
        Strings.isNullOrEmpty(POSTGRES_USER));
    assumeFalse("This test the environment variable POSTGRES_DATABASE to point to a valid PostgreSQL database",
        Strings.isNullOrEmpty(POSTGRES_PORT));

    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    dropDataModel();
    createDataModel();

//     testEnv.setUp();
//     database = testEnv.createDatabase(DEFAULT_DATA_MODEL);
//     testEnv.startPGAdapterServer(Collections.emptyList());
  }

  @AfterClass
  public static void teardown() throws Exception {
//    testEnv.stopPGAdapterServer();
//    testEnv.cleanUp();
//    dropDataModel();
  }

  private static boolean isPsqlAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand = new String[] {"psql", "--version"};
    builder.command(psqlCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  private static void createDataModel() throws Exception {
    String[] createTablesCommand =
        new String[] {
            "psql",
            "-h",
            POSTGRES_HOST,
            "-p",
            POSTGRES_PORT,
            "-U",
            POSTGRES_USER,
            "-d",
            POSTGRES_DATABASE,
            "-c",
            String.join(";", DEFAULT_DATA_MODEL) + ";\n"
        };
    ProcessBuilder builder = new ProcessBuilder().command(createTablesCommand);
    Process process = builder.start();

    int res = process.waitFor();
    assertEquals(0, res);
  }

  private static void dropDataModel() throws Exception {
    String[] dropTablesCommand =
        new String[] {
            "psql",
            "-h",
            POSTGRES_HOST,
            "-p",
            POSTGRES_PORT,
            "-U",
            POSTGRES_USER,
            "-d",
            POSTGRES_DATABASE,
            "-c",
            "drop table if exists all_types; drop table if exists numbers;"
        };
    ProcessBuilder builder = new ProcessBuilder().command(dropTablesCommand);
    Process process = builder.start();

    int res = process.waitFor();
    assertEquals(0, res);
  }

  private String createJdbcUrlForLocalPg() {
    if (POSTGRES_HOST.startsWith("/")) {
      return String.format(
          "jdbc:postgresql://localhost/%s?"
              + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
              + "&socketFactoryArg=%s/.s.PGSQL.%d"
              + "&preferQueryMode=simple",
          pgServer.getLocalPort());
    }
    return String.format("jdbc:postgresql://%s:%s/%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE);
  }

  @Test
  public void testRealPostgreSQLHelloWorld() throws Exception {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand = new String[] {"psql", "-h", POSTGRES_HOST, "-p", POSTGRES_PORT, "-U", POSTGRES_USER, "-d", POSTGRES_DATABASE};
    builder.command(psqlCommand);
    Process process = builder.start();
    String errors;
    String output;

    try (OutputStreamWriter writer = new OutputStreamWriter(process.getOutputStream());
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      writer.write("SELECT 'Hello World!' AS hello;\n");
      writer.write("\\q\n");
      writer.flush();
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    assertEquals("", errors);
    assertEquals("    hello     \n" + "--------------\n" + " Hello World!\n" + "(1 row)\n", output);
    int res = process.waitFor();
    assertEquals(0, res);
  }

  @Test
  public void testCopyFromPostgreSQLToCloudSpanner() throws Exception {
    // Generate 100 random rows and copy these into the all_types table in a local PostgreSQL database.
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(
        "bash",
        "-c",
        "psql"
            + " -h " + POSTGRES_HOST
            + " -p " + POSTGRES_PORT
            + " -U " + POSTGRES_USER
            + " -d " + POSTGRES_DATABASE
            + " -c \"copy (select (random()*9223372036854775807)::bigint, "
            + "  random()<0.5, md5(random()::text || clock_timestamp()::text)::bytea, random()*123456789, "
            + "  (random()*999999)::int, (random()*999999999999)::numeric, now()-random()*interval '250 year', "
            + "  (current_date-random()*interval '400 year')::date, md5(random()::text || clock_timestamp()::text)::varchar "
            + "  from generate_series(1, 100) s(i)) to stdout\" "
            + "  | psql "
            + " -h " + POSTGRES_HOST
            + " -p " + POSTGRES_PORT
            + " -U " + POSTGRES_USER
            + " -d " + POSTGRES_DATABASE
            + " -c \"copy all_types from stdin;\"\n");
    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);

    try (Connection connection = DriverManager.getConnection()) {

    }
  }

}
