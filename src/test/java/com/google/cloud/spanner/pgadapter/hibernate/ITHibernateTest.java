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

package com.google.cloud.spanner.pgadapter.hibernate;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ITHibernateTest {
  private static final Logger LOGGER = Logger.getLogger(ITHibernateTest.class.getName());
  private static final String HIBERNATE_SAMPLE_DIRECTORY = "samples/java/hibernate";
  private static final String HIBERNATE_PROPERTIES_FILE =
      HIBERNATE_SAMPLE_DIRECTORY + "/src/main/resources/hibernate.properties";
  private static final String HIBERNATE_DB_CHANGELOG_DDL_FILE =
      HIBERNATE_SAMPLE_DIRECTORY + "/src/main/resources/sample-schema-sql";
  private static final String HIBERNATE_DEFAULT_URL =
      "jdbc:postgresql://localhost:5432/test-database";
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;
  private static String originalHibernateProperties;

  @BeforeClass
  public static void setup()
      throws ClassNotFoundException, IOException, SQLException, InterruptedException {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    testEnv.setUp();
    database = testEnv.createDatabase(ImmutableList.of());
    testEnv.startPGAdapterServer(ImmutableList.of());
    // Create databasechangelog and databasechangeloglock tables.
    StringBuilder builder = new StringBuilder();
    try (Scanner scanner = new Scanner(new FileReader(HIBERNATE_DB_CHANGELOG_DDL_FILE))) {
      while (scanner.hasNextLine()) {
        builder.append(scanner.nextLine()).append("\n");
      }
    }
    // Note: We know that all semicolons in this file are outside of literals etc.
    String[] ddl = builder.toString().split(";");
    String url =
        String.format(
            "jdbc:postgresql://localhost:%d/%s",
            testEnv.getPGAdapterPort(), database.getId().getDatabase());
    try (Connection connection = DriverManager.getConnection(url)) {
      try (Statement statement = connection.createStatement()) {
        for (String sql : ddl) {
          LOGGER.info("Executing " + sql);
          statement.execute(sql);
        }
      }
    }

    // Write hibernate.properties
    StringBuilder original = new StringBuilder();
    try (Scanner scanner = new Scanner(new FileReader(HIBERNATE_PROPERTIES_FILE))) {
      while (scanner.hasNextLine()) {
        original.append(scanner.nextLine()).append("\n");
      }
    }
    originalHibernateProperties = original.toString();
    String updatesHibernateProperties = original.toString().replace(HIBERNATE_DEFAULT_URL, url);
    try (FileWriter writer = new FileWriter(HIBERNATE_PROPERTIES_FILE)) {
      LOGGER.info("Using Hibernate properties:\n" + updatesHibernateProperties);
      writer.write(updatesHibernateProperties);
    }
    buildHibernateSample();
  }

  @AfterClass
  public static void teardown() throws IOException {
    try (FileWriter writer = new FileWriter(HIBERNATE_PROPERTIES_FILE)) {
      writer.write(originalHibernateProperties);
    }
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Test
  public void testHibernateUpdate() throws IOException, InterruptedException, SQLException {
    System.out.println("Running hibernate test");
    ImmutableList<String> hibernateCommand =
        ImmutableList.<String>builder()
            .add(
                "mvn",
                "exec:java",
                "-Dexec.mainClass=com.google.cloud.postgres.HibernateSampleTest")
            .build();
    runCommand(hibernateCommand);
    System.out.println("Hibernate Test Ended");
  }

  static void buildHibernateSample() throws IOException, InterruptedException {
    System.out.println("Building Hibernate Sample.");
    ImmutableList<String> hibernateCommand =
        ImmutableList.<String>builder().add("mvn", "clean", "package").build();
    runCommand(hibernateCommand);
    System.out.println("Hibernate Sample build complete.");
  }

  static void runCommand(ImmutableList<String> commands) throws IOException, InterruptedException {
    System.out.println(
        "Executing commands: " + commands + ". Sample Directory: " + HIBERNATE_SAMPLE_DIRECTORY);
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(commands);
    builder.directory(new File(HIBERNATE_SAMPLE_DIRECTORY));
    Process process = builder.start();

    String errors;
    String output;

    try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
      System.out.println("Hibernate Command. Output: " + output + ". Error: " + errors);
    }

    LOGGER.warning(errors);
    LOGGER.info(output);

    int res = process.waitFor();
    assertEquals(errors, 0, res);
  }
}
