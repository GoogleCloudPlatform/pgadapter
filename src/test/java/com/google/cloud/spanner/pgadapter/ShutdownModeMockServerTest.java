// Copyright 2024 Google LLC
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

import com.google.api.core.ApiService.State;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.pgadapter.ProxyServer.ShutdownMode;
import com.google.cloud.spanner.pgadapter.metadata.TestOptionsMetadataBuilder;
import com.google.common.base.Stopwatch;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ShutdownModeMockServerTest extends AbstractMockServerTest {

  @Parameter public boolean useSqlStatement;

  @Parameters(name = "useSqlStatement = {0}")
  public static Object[] data() {
    return new Object[] {false, true};
  }

  private ProxyServer proxyServer;

  @Before
  public void createProxyServer() {
    TestOptionsMetadataBuilder builder = new TestOptionsMetadataBuilder();
    builder.setProject("p").setInstance("i").setDatabase("d");
    builder
        .enableDebugMode()
        .setUsePlainText()
        .setEndpoint(String.format("localhost:%d", spannerServer.getPort()))
        .setCredentials(NoCredentials.getInstance())
        .setAllowShutdownStatement(true)
        .setEnableEndToEndTracing(true);
    proxyServer = new ProxyServer(builder.build(), OpenTelemetry.noop());
    proxyServer.startServer();
  }

  @After
  public void stopProxyServer() {
    if (proxyServer.state() != State.TERMINATED) {
      proxyServer.stopServer();
    }
  }

  private String createUrl() {
    return String.format("jdbc:postgresql://localhost:%d/d", proxyServer.getLocalPort());
  }

  @Test
  public void testSmartShutdown() throws SQLException {
    String sql = "SELECT 1";

    ShutdownHandler shutdownHandler = createShutdownHandler();
    // Verify that the server is running.
    assertEquals(State.RUNNING, proxyServer.state());

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Verify that the connection is valid.
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      // Initiate a smart shutdown.
      if (useSqlStatement) {
        connection.createStatement().execute("shutdown smart");
      } else {
        shutdownHandler.shutdown(ShutdownMode.SMART);
      }
      // Wait until the server port is no longer open.
      Stopwatch stopwatch = Stopwatch.createStarted();
      while (isPortListening(proxyServer.getLocalPort())
          && stopwatch.elapsed(TimeUnit.MILLISECONDS) < 2000) {
        Thread.yield();
      }
      assertEquals(State.STOPPING, proxyServer.state());
      // Verify that we can still use the existing connection.
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      // Verify that we cannot open a new connection.
      SQLException exception =
          assertThrows(SQLException.class, () -> DriverManager.getConnection(createUrl()));
      assertEquals(
          String.format(
              "Connection to localhost:%d refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.",
              proxyServer.getLocalPort()),
          exception.getMessage());
      // Verify that the original connection is still valid.
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      // Verify that the server is still stopping, but not yet terminated.
      assertEquals(State.STOPPING, proxyServer.state());
    }
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (proxyServer.state() == State.STOPPING
        && stopwatch.elapsed(TimeUnit.MILLISECONDS) < 2000) {
      Thread.yield();
    }
    // Verify that closing the last connection terminates the server.
    assertEquals(State.TERMINATED, proxyServer.state());
  }

  @Test
  public void testSmartShutdownWithoutConnections() throws SQLException {
    String sql = "SELECT 1";

    ShutdownHandler shutdownHandler = createShutdownHandler();
    // Verify that the server is running.
    assertEquals(State.RUNNING, proxyServer.state());

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Verify that the connection is valid.
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
    // Initiate a smart shutdown.
    if (useSqlStatement) {
      try (Connection connection = DriverManager.getConnection(createUrl());
          Statement statement = connection.createStatement()) {
        statement.execute("shutdown smart");
      }
    } else {
      shutdownHandler.shutdown(ShutdownMode.SMART);
    }
    // Wait until the server port is no longer open.
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (isPortListening(proxyServer.getLocalPort())
        && stopwatch.elapsed(TimeUnit.MILLISECONDS) < 2000) {
      Thread.yield();
    }
    // Verify that we cannot open a new connection.
    SQLException exception =
        assertThrows(SQLException.class, () -> DriverManager.getConnection(createUrl()));
    assertEquals(
        String.format(
            "Connection to localhost:%d refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.",
            proxyServer.getLocalPort()),
        exception.getMessage());

    Stopwatch.createStarted();
    while (proxyServer.state() == State.STOPPING
        && stopwatch.elapsed(TimeUnit.MILLISECONDS) < 2000) {
      Thread.yield();
    }
    // Verify that the server terminates.
    assertEquals(State.TERMINATED, proxyServer.state());
  }

  @Test
  public void testFastShutdown() throws SQLException {
    String sql = "SELECT 1";

    ShutdownHandler shutdownHandler = createShutdownHandler();
    // Verify that the server is running.
    assertEquals(State.RUNNING, proxyServer.state());

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Verify that the connection is valid.
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      // Initiate a fast shutdown.
      if (useSqlStatement) {
        connection.createStatement().execute("shutdown fast");
      } else {
        shutdownHandler.shutdown(ShutdownMode.FAST);
      }
      // Verify that the server stops.
      Stopwatch stopwatch = Stopwatch.createStarted();
      while (proxyServer.state() != State.TERMINATED
          && stopwatch.elapsed(TimeUnit.MILLISECONDS) < 2000) {
        Thread.yield();
      }
      assertEquals(State.TERMINATED, proxyServer.state());

      // Verify that existing connection has been invalidated.
      SQLException exception =
          assertThrows(SQLException.class, () -> connection.createStatement().executeQuery(sql));
      assertEquals("An I/O error occurred while sending to the backend.", exception.getMessage());

      // Verify that we cannot open a new connection.
      exception = assertThrows(SQLException.class, () -> DriverManager.getConnection(createUrl()));
      assertEquals(
          String.format(
              "Connection to localhost:%d refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.",
              proxyServer.getLocalPort()),
          exception.getMessage());
    }
  }

  @Test
  public void testSmartShutdownFollowedByFastShutdown() throws SQLException {
    String sql = "SELECT 1";

    ShutdownHandler shutdownHandler = createShutdownHandler();
    // Verify that the server is running.
    assertEquals(State.RUNNING, proxyServer.state());

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Verify that the connection is valid.
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      // Initiate a smart shutdown.
      if (useSqlStatement) {
        connection.createStatement().execute("shutdown smart");
      } else {
        shutdownHandler.shutdown(ShutdownMode.SMART);
      }
      // Wait until the server port is no longer open.
      Stopwatch stopwatch = Stopwatch.createStarted();
      while (isPortListening(proxyServer.getLocalPort())
          && stopwatch.elapsed(TimeUnit.MILLISECONDS) < 2000) {
        Thread.yield();
      }
      // Verify that we can still use the existing connection.
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      // Verify that the server is stopping, but not yet terminated.
      assertEquals(State.STOPPING, proxyServer.state());
      // Verify that we cannot open a new connection.
      SQLException exception =
          assertThrows(SQLException.class, () -> DriverManager.getConnection(createUrl()));
      assertEquals(
          String.format(
              "Connection to localhost:%d refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.",
              proxyServer.getLocalPort()),
          exception.getMessage());
      // Verify that the server is still stopping, but not yet terminated.
      assertEquals(State.STOPPING, proxyServer.state());

      // Now force the server to stop by initiating a fast shutdown.
      if (useSqlStatement) {
        connection.createStatement().execute("shutdown fast");
      } else {
        shutdownHandler.shutdown(ShutdownMode.FAST);
      }
      stopwatch = Stopwatch.createStarted();
      while (proxyServer.state() != State.TERMINATED
          && stopwatch.elapsed(TimeUnit.MILLISECONDS) < 2000) {
        Thread.yield();
      }
      assertEquals(State.TERMINATED, proxyServer.state());

      // Verify that existing connection has been invalidated.
      exception =
          assertThrows(SQLException.class, () -> connection.createStatement().executeQuery(sql));
      assertEquals("An I/O error occurred while sending to the backend.", exception.getMessage());
    }
  }

  ShutdownHandler createShutdownHandler() {
    return useSqlStatement ? null : ShutdownHandler.createForServer(this.proxyServer);
  }

  boolean isPortListening(int port) {
    try (Socket ignored = new Socket("localhost", port)) {
      return true;
    } catch (SocketException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
