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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DomainSocketsTest extends AbstractMockServerTest {

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    // Hide the implementation in the base class to prevent PGAdapter to be started for the test
    // class.
    assumeFalse(
        "Domain sockets are disabled by default on Windows",
        System.getProperty("os.name", "").toLowerCase().startsWith("windows"));
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @Before
  @Override
  public void clearRequests() {}

  private String createUrl(String socketFile) {
    return String.format(
        "jdbc:postgresql://localhost/?"
            + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
            + "&socketFactoryArg="
            + socketFile
            + "&preferQueryMode=simple",
        pgServer.getLocalPort());
  }

  @Test
  public void testDefaultDomainSocketFile() throws Exception {
    doStartMockSpannerAndPgAdapterServers(null, builder -> {});

    String sql = "SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl("/tmp/.s.PGSQL.%d"))) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    stopMockSpannerAndPgAdapterServers();
  }

  @Test
  public void testCustomDomainSocketFile() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        null, builder -> builder.setUnixDomainSocketDirectory("./"));

    String sql = "SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl("./.s.PGSQL.%d"))) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    stopMockSpannerAndPgAdapterServers();
  }

  @Test
  public void testConnectionFailsForIncorrectDomainSocketFile() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        null, builder -> builder.setUnixDomainSocketDirectory("./sockets"));

    assertThrows(SQLException.class, () -> DriverManager.getConnection(createUrl("./.s.PGSQL.%d")));

    stopMockSpannerAndPgAdapterServers();
  }

  @Test
  public void testPGAdapterStartFailsWithInvalidSocketFile() throws Exception {
    File file = new File("/test.txt");
    boolean canCreate = false;
    try {
      Files.createFile(file.toPath());
      canCreate = true;
    } catch (IOException ignored) {
    }
    assumeFalse(
        "Skip test if the test container allows creating files in the root directory", canCreate);

    doStartMockSpannerAndPgAdapterServers(
        null, builder -> builder.setUnixDomainSocketDirectory("/"));

    // Verify that we cannot connect to the invalid (not permitted) domain socket.
    assertThrows(SQLException.class, () -> DriverManager.getConnection(createUrl("/.s.PGSQL.%d")));

    // Verify that the TCP socket does work.
    String sql = "SELECT 1";
    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:postgresql://localhost:%d/my-db?preferQueryMode=simple",
                pgServer.getLocalPort()))) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    stopMockSpannerAndPgAdapterServers();
  }
}
