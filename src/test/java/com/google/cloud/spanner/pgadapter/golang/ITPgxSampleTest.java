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

package com.google.cloud.spanner.pgadapter.golang;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITPgxSampleTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  @BeforeClass
  public static void setup() throws Exception {
    assumeTrue("This test only works on the default Spanner URL", testEnv.getSpannerUrl() == null);
    assumeTrue(
        "This test only works on the default Spanner URL",
        System.getenv("SPANNER_EMULATOR_HOST") == null);

    String currentPath = new java.io.File(".").getCanonicalPath();
    String relativeSampleFilePath = "samples/golang/pgx/pgx_sample.go";
    String sampleFilePath = String.format("%s/%s", currentPath, relativeSampleFilePath);
    File testFile = new File(sampleFilePath);
    File directory = testFile.getParentFile();
    // Compile the Go code to ensure that we always have the most recent sample code.
    ProcessBuilder builder = new ProcessBuilder();
    String[] compileCommand = String.format("go build %s", testFile.getName()).split(" ");
    builder.command(compileCommand);
    builder.directory(directory);
    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);

    testEnv.setUp();
    database = testEnv.createDatabase(ImmutableList.of());
  }

  @AfterClass
  public static void teardown() {
    testEnv.cleanUp();
  }

  @Test
  public void testPgxSample() throws Exception {
    String currentPath = new java.io.File(".").getCanonicalPath();
    String relativeSampleFilePath = "samples/golang/pgx/pgx_sample";
    String sampleFilePath = String.format("%s/%s", currentPath, relativeSampleFilePath);
    ProcessBuilder builder = new ProcessBuilder();
    builder.directory(new File(sampleFilePath).getParentFile());
    builder.command(
        String.format(
                "./pgx_sample -project %s -instance %s -database %s",
                database.getId().getInstanceId().getProject(),
                database.getId().getInstanceId().getInstance(),
                database.getId().getDatabase())
            .split(" "));
    Process process = builder.start();
    InputStream inputStream = process.getInputStream();
    InputStream errorStream = process.getErrorStream();
    boolean finished = process.waitFor(120L, TimeUnit.SECONDS);

    String output = readAll(inputStream);
    String errors = readAll(errorStream);
    assertTrue(finished);
    assertEquals(errors, 0, process.exitValue());
    assertTrue(output, output.contains("Greeting from Cloud Spanner PostgreSQL: Hello world!"));
  }

  static String readAll(InputStream inputStream) {
    StringBuilder result = new StringBuilder();
    try (Scanner scanner = new Scanner(new InputStreamReader(inputStream))) {
      while (scanner.hasNextLine()) {
        result.append(scanner.nextLine()).append("\n");
      }
    }
    return result.toString();
  }
}
