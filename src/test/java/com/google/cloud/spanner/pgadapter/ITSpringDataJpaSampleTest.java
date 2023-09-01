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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITSpringDataJpaSampleTest {
  private static final String SPRING_DATA_JPA_SAMPLE_DIRECTORY = "samples/java/spring-data-jpa";
  private static final String APPLICATION_PROPERTIES_FILE =
      SPRING_DATA_JPA_SAMPLE_DIRECTORY + "/src/main/resources/application.properties";
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static String originalApplicationProperties;

  @BeforeClass
  public static void setup() throws ClassNotFoundException, IOException, SQLException {
    assumeFalse("Spring Data sample requires at least Java 9", OptionsMetadata.isJava8());

    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    testEnv.setUp();
    Database database = testEnv.createDatabase(ImmutableList.of());

    // Write application.properties
    StringBuilder original = new StringBuilder();
    try (Scanner scanner = new Scanner(new FileReader(APPLICATION_PROPERTIES_FILE))) {
      while (scanner.hasNextLine()) {
        original.append(scanner.nextLine()).append("\n");
      }
    }
    originalApplicationProperties = original.toString();
    try (FileWriter writer = new FileWriter(APPLICATION_PROPERTIES_FILE)) {
      String properties =
          originalApplicationProperties
              .replace(
                  "spanner.project=my-project",
                  "spanner.project=" + database.getId().getInstanceId().getProject())
              .replace(
                  "spanner.instance=my-instance",
                  "spanner.instance=" + database.getId().getInstanceId().getInstance())
              .replace(
                  "spanner.database=my-database",
                  "spanner.database=" + database.getId().getDatabase());
      writer.write(properties);
      writer.flush();
    }
  }

  @AfterClass
  public static void teardown() throws IOException {
    try (FileWriter writer = new FileWriter(APPLICATION_PROPERTIES_FILE)) {
      writer.write(originalApplicationProperties);
    }
    testEnv.cleanUp();
  }

  @Test
  public void testRunApplication() throws IOException, InterruptedException {
    String expectedOutput = "concerts using a stale read.";
    String output = runApplication(expectedOutput);
    assertTrue(output, output.contains(expectedOutput));
  }

  String runApplication(String expectedOutput) throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    ImmutableList<String> runCommand =
        ImmutableList.<String>builder().add("mvn", "spring-boot:run").build();
    builder.command(runCommand);
    builder.directory(new File(SPRING_DATA_JPA_SAMPLE_DIRECTORY));
    Process process = builder.start();

    String errors;
    StringBuilder output = new StringBuilder();

    try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        output.append(line).append("\n");
        if (line.contains(expectedOutput)) {
          process.destroy();
        }
      }
      errors = errorReader.lines().collect(Collectors.joining("\n"));
    }
    assertTrue(process.waitFor(10L, TimeUnit.MINUTES));
    assertEquals(errors + "\n\n" + output, 0, process.exitValue());

    return output.toString();
  }
}
