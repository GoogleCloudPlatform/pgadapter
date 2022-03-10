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

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JdbcMetadataMockServerTest extends AbstractMockServerTest {
  private static final com.google.spanner.v1.ResultSet EMPTY_RESULT_SET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .setMetadata(
              ResultSetMetadata.newBuilder().setRowType(StructType.newBuilder().build()).build())
          .build();
  private static final String[] VERSIONS =
      new String[] {
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.25/postgresql-42.2.25.jar",
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.24/postgresql-42.2.24.jar"
      };

  private String createUrl() {
    return String.format("jdbc:postgresql://localhost:%d/", pgServer.getLocalPort());
  }

  @Test
  public void testGetTables() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'PARTITIONED TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'P' then 'PARTITIONED INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS,  '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN (select * from (select 0 as objoid, 0 as classoid, 0 as objsubid, '' as description) pg_description where false) d ON (c.oid = d.objoid AND d.objsubid = 0  and d.classoid = 0)  WHERE c.relnamespace = n.oid  ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME"),
            EMPTY_RESULT_SET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN (select * from (select 0 as objoid, 0 as classoid, 0 as objsubid, '' as description) pg_description where false) d ON (c.oid = d.objoid AND d.objsubid = 0)  LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')  WHERE c.relnamespace = n.oid  ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME"),
            EMPTY_RESULT_SET));

    runForAllVersions(
        connection -> {
          try (ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null)) {
            while (resultSet.next()) {
              // ignore
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testGetSchemas() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "select schema_name as TABLE_SCHEM, catalog_name AS TABLE_CATALOG from information_schema.schemata WHERE true  ORDER BY schema_name"),
            EMPTY_RESULT_SET));
    runForAllVersions(
        connection -> {
          try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            while (resultSet.next()) {
              // ignore
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  private void runForAllVersions(Consumer<Connection> runnable) throws Exception {
    for (String version : VERSIONS) {
      ClassLoader defaultClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        ClassLoader classLoader =
            new URLClassLoader(
                new URL[] {
                  new URL(version),
                  JdbcMetadataMockServerTest.class
                      .getProtectionDomain()
                      .getCodeSource()
                      .getLocation()
                },
                defaultClassLoader.getParent());
        Thread.currentThread().setContextClassLoader(classLoader);
        Class<?> runnerClass = classLoader.loadClass(TestRunner.class.getName());
        Constructor<?> constructor = runnerClass.getDeclaredConstructor();
        Object runner = constructor.newInstance();
        Method runMethod = runner.getClass().getDeclaredMethod("run", String.class, Consumer.class);
        runMethod.invoke(runner, createUrl(), runnable);
      } finally {
        Thread.currentThread().setContextClassLoader(defaultClassLoader);
      }
    }
  }

  public static class TestRunner {
    public void run(String url, Consumer<Connection> runnable) throws Exception {
      Class.forName("org.postgresql.Driver");
      try (Connection connection = DriverManager.getConnection(url)) {
        runnable.accept(connection);
      }
      Driver driver = DriverManager.getDriver(url);
      DriverManager.deregisterDriver(driver);
    }
  }
}
