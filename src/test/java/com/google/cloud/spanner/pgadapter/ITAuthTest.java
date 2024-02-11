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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.gson.GsonFactory;
import com.google.cloud.spanner.Database;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ITAuthTest implements IntegrationTest {
  // Create a test environment without any credentials.
  private static final PgAdapterTestEnv testEnv =
      new PgAdapterTestEnv() {
        @Override
        public String getCredentials() {
          return null;
        }
      };
  private static Database database;

  @BeforeClass
  public static void setup() throws ClassNotFoundException {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    testEnv.setUp();
    database = testEnv.createDatabase(PgAdapterTestEnv.DEFAULT_DATA_MODEL);
    // Start PGAdapter with authentication mode enabled.
    testEnv.startPGAdapterServer(ImmutableList.of("-a"));
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  private String getConnectionUrl() {
    return String.format(
        "jdbc:postgresql://%s/%s",
        testEnv.getPGAdapterHostAndPort(), database.getId().getDatabase());
  }

  private static boolean isGcloudAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] gcloudCommand = new String[] {"gcloud", "--version"};
    builder.command(gcloudCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  @Test
  public void testConnectFailsWithoutAuth() {
    // The server is started with authentication required. Trying to obtain a connection without any
    // credentials will fail.
    SQLException exception =
        assertThrows(SQLException.class, () -> DriverManager.getConnection(getConnectionUrl()));
    assertTrue(
        exception.getMessage(),
        exception
            .getMessage()
            .contains(
                "The server requested password-based authentication, but no password was provided"));
  }

  @Test
  public void testConnectFailsWithRandomAuth() {
    SQLException exception =
        assertThrows(
            SQLException.class,
            () -> DriverManager.getConnection(getConnectionUrl(), "foo", "bar"));
    assertEquals(AuthMockServerTest.CREDENTIALS_ERROR, exception.getMessage());
  }

  @Test
  public void testConnectWithOAuth2Token() throws Exception {
    IntegrationTest.skipOnEmulator("Credentials are never used with the emulator");
    String credentialsFile = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
    assumeTrue("Auth tests require default (service account) credentials", credentialsFile != null);
    assumeTrue("OAuth2 test requires gcloud", isGcloudAvailable());

    ProcessBuilder builder = new ProcessBuilder();
    String[] gcloudCommand =
        new String[] {"gcloud", "auth", "application-default", "print-access-token", "--quiet"};
    builder.command(gcloudCommand);
    Process process = builder.start();
    String errors;
    String output;

    try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    assertEquals("", errors);
    assertNotEquals("", output);
    int res = process.waitFor();
    assertEquals(0, res);

    try (Connection connection =
        DriverManager.getConnection(getConnectionUrl(), "oauth2", output)) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("SELECT 'Hello World!'")) {
        assertTrue(resultSet.next());
        assertEquals("Hello World!", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testConnectWithCredentials() throws Exception {
    IntegrationTest.skipOnEmulator("Credentials are never used with the emulator");
    String credentialsFile = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
    assumeTrue("Auth tests require default (service account) credentials", credentialsFile != null);

    JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
    JsonObjectParser parser = new JsonObjectParser(jsonFactory);
    GenericJson fileContents =
        parser.parseAndClose(
            new FileInputStream(credentialsFile), StandardCharsets.UTF_8, GenericJson.class);
    String password = fileContents.toPrettyString();
    try (Connection connection =
        DriverManager.getConnection(getConnectionUrl(), "any-user-name", password)) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("SELECT 'Hello World!'")) {
        assertTrue(resultSet.next());
        assertEquals("Hello World!", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testConnectWithPrivateKey() throws Exception {
    IntegrationTest.skipOnEmulator("Credentials are never used with the emulator");
    String credentialsFile = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
    assumeTrue("Auth tests require default (service account) credentials", credentialsFile != null);

    JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
    JsonObjectParser parser = new JsonObjectParser(jsonFactory);
    GenericJson fileContents =
        parser.parseAndClose(
            new FileInputStream(credentialsFile), StandardCharsets.UTF_8, GenericJson.class);

    // The username must be the client_email of the service account when connecting using only the
    // private key of a service account.
    String username = (String) fileContents.get("client_email");
    String password = (String) fileContents.get("private_key");
    try (Connection connection =
        DriverManager.getConnection(getConnectionUrl(), username, password)) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("SELECT 'Hello World!'")) {
        assertTrue(resultSet.next());
        assertEquals("Hello World!", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }
}
