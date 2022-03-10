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

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.TimeZone;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.jdbc.TimestampUtils;

/**
 * Tests the native PG JDBC driver in simple query mode. This is similar to the protocol that is
 * used by psql, and for example allows batches to be given as semicolon-separated strings.
 */
@RunWith(JUnit4.class)
public class JdbcMetadataMockServerTest extends AbstractMockServerTest {
  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.

  }

  private String createUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/", pgServer.getLocalPort());
  }

  @Test
  public void testGetTables()
      throws SQLException, MalformedURLException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
    Class<?> initialDriverClass = Class.forName("org.postgresql.Driver");
    // First run the test using the default driver.
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null)) {
        while (resultSet.next()) {
          // ignore
        }
      } catch (SQLException e) {
        // ignore
      }
    }

    // Deregister the default driver and try to load a different version.
    org.postgresql.Driver.deregister();

    ClassLoader defaultClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader classLoader = new URLClassLoader(new URL[]{
        new URL("https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.25/postgresql-42.2.25.jar"),
        JdbcMetadataMockServerTest.class.getProtectionDomain().getCodeSource().getLocation()
    }, defaultClassLoader.getParent());
//    Thread.currentThread().setContextClassLoader(classLoader);
    Class<?> driverClass = classLoader.loadClass("org.postgresql.Driver");
    Class<?> runnerClass = classLoader.loadClass(TestRunner.class.getName());
    Constructor<?> constructor = runnerClass.getDeclaredConstructor();
    Object runner = constructor.newInstance();
    Method runMethod = runner.getClass().getDeclaredMethod("run", String.class);
    runMethod.invoke(runner, createUrl());
  }

  public static class TestRunner {
    public void run(String url) throws SQLException {
      try (Connection connection = DriverManager.getConnection(url)) {
        try (ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null)) {
          while (resultSet.next()) {
            // ignore
          }
        } catch (SQLException e) {
          // ignore
        }
      }
    }
  }
}
