// Copyright 2026 Google LLC
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

package com.google.cloud.spanner.pgadapter.nodejs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({IntegrationTest.class})
@RunWith(JUnit4.class)
public class ITDrizzleSampleTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static final String SAMPLE_DIR = "./samples/nodejs/drizzle";

  @BeforeClass
  public static void setup() throws Exception {
    NodeJSTest.installDependencies(new File(SAMPLE_DIR));
    testEnv.setUp();
    Database database = testEnv.createDatabase(Collections.emptyList());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Test
  public void testDrizzleSample() throws Exception {
    String output =
        runSample(
            new File(SAMPLE_DIR), testEnv.getServer().getLocalPort(), testEnv.getDatabaseId());

    assertTrue(output, output.contains("Checking whether tables already exists..."));
    assertTrue(output, output.contains("Creating tables..."));
    assertTrue(output, output.contains("Finished creating tables."));
    assertTrue(
        output,
        output.contains("Creating 5 random singers and their albums inside a transaction..."));
    assertTrue(output, output.contains("Transaction successfully committed."));
    assertTrue(output, output.contains("Printing all singers and their albums..."));
    assertTrue(output, output.contains("Singer: "));
    assertTrue(output, output.contains("Creating venue, concert, and ticket sale..."));
    assertTrue(output, output.contains("Successfully sold ticket. Generated serial ID returned:"));
    assertTrue(output, output.contains("Executing a stale read"));
    assertTrue(output, output.contains("Stale read returned singer:"));
    assertTrue(
        output,
        output.contains(
            "Note: Drizzle Relational Queries (db.query) are not supported on Cloud Spanner"));
  }

  private static String runSample(File directory, int port, String database)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    builder.command("npm", "start");
    builder.directory(directory);
    builder.environment().put("AUTO_START_PGADAPTER", "false");
    builder
        .environment()
        .put("DATABASE_URL", String.format("postgresql://localhost:%d/%s", port, database));

    Process process = builder.start();
    InputStream inputStream = process.getInputStream();
    InputStream errorStream = process.getErrorStream();
    boolean finished = process.waitFor(120L, TimeUnit.SECONDS);

    String output = readAll(inputStream);
    String errors = readAll(errorStream);
    assertEquals("", errors);
    assertTrue(finished);
    assertEquals(errors, 0, process.exitValue());

    return output;
  }

  private static String readAll(InputStream inputStream) {
    StringBuilder result = new StringBuilder();
    try (Scanner scanner = new Scanner(new InputStreamReader(inputStream))) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        if (!line.startsWith(">")) {
          result.append(line).append("\n");
        }
      }
    }
    return result.toString();
  }
}
