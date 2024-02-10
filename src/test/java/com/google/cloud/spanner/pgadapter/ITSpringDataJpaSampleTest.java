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

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITSpringDataJpaSampleTest implements IntegrationTest {
  private static final String SPRING_DATA_JPA_SAMPLE_DIRECTORY = "samples/java/spring-data-jpa";
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static DatabaseId databaseId;

  @BeforeClass
  public static void setup() throws ClassNotFoundException, IOException, SQLException {
    assumeTrue(
        "Sample test is not made compatible with emulator",
        System.getenv("SPANNER_EMULATOR_HOST") == null);

    assumeFalse("Spring Data sample requires at least Java 9", OptionsMetadata.isJava8());
    assumeTrue("This test only works on the default Spanner URL", testEnv.getSpannerUrl() == null);

    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    testEnv.setUp();
    Database database = testEnv.createDatabase(ImmutableList.of());
    databaseId = database.getId();
  }

  @AfterClass
  public static void teardown() throws IOException {
    testEnv.cleanUp();
  }

  @Test
  public void testRunApplication() throws IOException, InterruptedException {
    String expectedOutput = "concerts using a stale read.";
    String output = runApplication(expectedOutput, databaseId);
    assertTrue(output, output.contains(expectedOutput));
  }

  String runApplication(String expectedOutput, DatabaseId databaseId)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    ImmutableList<String> runCommand =
        ImmutableList.<String>builder().add("mvn", "spring-boot:run").build();
    builder.command(runCommand);
    builder.environment().put("SPANNER_PROJECT", databaseId.getInstanceId().getProject());
    builder.environment().put("SPANNER_INSTANCE", databaseId.getInstanceId().getInstance());
    builder.environment().put("SPANNER_DATABASE", databaseId.getDatabase());
    builder.directory(new File(SPRING_DATA_JPA_SAMPLE_DIRECTORY));
    Process process = builder.start();

    StringBuilder outputBuilder = new StringBuilder();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        outputBuilder.append(line).append("\n");
        if (line.contains(expectedOutput)) {
          process.destroy();
          break;
        }
      }
    }
    if (!process.waitFor(5L, TimeUnit.MINUTES)) {
      process.destroyForcibly();
    }
    String output = outputBuilder.toString();
    assertTrue(output, output.contains(expectedOutput));

    return output;
  }
}
