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
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITJdbcMetadataTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static ProxyServer server;
  private static Database database;
  private static final String[] VERSIONS =
      new String[] {
        "42.3.3", "42.3.2", "42.3.1", "42.3.0", "42.2.25", "42.2.24", "42.2.23", "42.2.22",
        "42.2.21", "42.2.20", "42.2.19", "42.2.18", "42.2.17", "42.2.16", "42.2.15", "42.2.14",
        "42.2.13", "42.2.12", "42.2.11", "42.2.10", "42.2.9", "42.2.8", "42.2.7", "42.2.6",
        "42.2.5", "42.2.4", "42.2.3", "42.2.2", "42.2.1", "42.2.0", "42.1.4", "42.1.3", "42.1.2",
        "42.1.1", "42.1.0", "42.0.0"
      };

  @BeforeClass
  public static void setup() throws Exception {
    // TODO: Refactor the integration tests to use a common subclass, as this is repeated in each
    // class.
    testEnv.setUp();
    if (testEnv.isUseExistingDb()) {
      database = testEnv.getExistingDatabase();
    } else {
      database = testEnv.createDatabase();
      testEnv.updateDdl(
          database.getId().getDatabase(),
          Arrays.asList(
              "create table numbers (num int not null primary key, name varchar(100))",
              "create unique index idx_numbers_name on numbers (name)",
              "create table all_types ("
                  + "col_bigint bigint not null primary key, "
                  + "col_bool bool, "
                  + "col_bytea bytea, "
                  + "col_float8 float8, "
                  + "col_int int, "
                  + "col_numeric numeric, "
                  + "col_timestamptz timestamptz, "
                  + "col_varchar varchar(100))",
              "create index idx_col_varchar_int on all_types (col_varchar, col_int)",
              "create table singers (singer_id int8 not null primary key, name varchar(200))",
              "create table albums (\n"
                  + "\talbum_id int8 not null primary key,\n"
                  + "\tsinger_id int8 not null references singers (singer_id),\n"
                  + "\ttitle varchar(100)\n"
                  + ")",
              "create table tracks (\n"
                  + "\talbum_id int8 not null references albums (album_id),\n"
                  + "\ttrack_number int8 not null,\n"
                  + "\ttitle varchar(100),\n"
                  + "\tprimary key (album_id, track_number)\n"
                  + ")",
              "create table recording_attempt (\n"
                  + "\talbum int8 not null references albums (album_id),\n"
                  + "\ttrack int8 not null,\n"
                  + "\tattempt int8 not null,\n"
                  + "\trecording_time timestamptz not null,\n"
                  + "\tprimary key (album, track, attempt),\n"
                  + "\tforeign key (album, track) references tracks (album_id, track_number)\n"
                  + ")"));
    }
    String credentials = testEnv.getCredentials();
    ImmutableList.Builder<String> argsListBuilder =
        ImmutableList.<String>builder()
            .add(
                "-p",
                testEnv.getProjectId(),
                "-i",
                testEnv.getInstanceId(),
                "-d",
                database.getId().getDatabase(),
                "-s",
                String.valueOf(testEnv.getPort()),
                "-e",
                testEnv.getUrl().getHost());
    if (credentials != null) {
      argsListBuilder.add("-c", testEnv.getCredentials());
    }
    String[] args = argsListBuilder.build().toArray(new String[0]);
    server = new ProxyServer(new OptionsMetadata(args));
    server.startServer();
  }

  @AfterClass
  public static void teardown() {
    if (server != null) {
      server.stopServer();
    }
    testEnv.cleanUp();
  }

  private String createUrl() {
    return String.format("jdbc:postgresql://localhost:%d/", testEnv.getPort());
  }

  private void runForAllVersions(Consumer<Connection> runnable) throws Exception {
    for (String version : VERSIONS) {
      String mavenUrl =
          String.format(
              "https://repo1.maven.org/maven2/org/postgresql/postgresql/%s/postgresql-%s.jar",
              version, version);
      // Create a class loader that will search the PostgreSQL JDBC driver jar that we download from
      // Maven and this class file for classes. In addition, we set the parent class loader of this
      // new class loader to be the same as the parent of the default class loader. This is the
      // class loader that will be used to load system classes. We do this because:
      // 1. We want to use a specific JDBC driver version that we download from Maven.
      // 2. We want to have access to the classes in this file.
      // 3. We do not want to have access to any of the other classes that are defined in this
      // repository (specifically: we do not want to have access to the PostgreSQL driver that is
      // included in the repository as a dependency). This is why we set the parent class loader to
      // be the system class loader (i.e. the parent of the default class loader), and we do not use
      // the default class loader as the parent for our custom class loader.
      ClassLoader defaultClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        ClassLoader classLoader =
            new URLClassLoader(
                new URL[] {
                  new URL(mavenUrl),
                  ITJdbcMetadataTest.class.getProtectionDomain().getCodeSource().getLocation()
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
      // Dynamically load the PG JDBC driver using the defautl class loader of the current thread.
      Class.forName("org.postgresql.Driver");
      try (Connection connection = DriverManager.getConnection(url)) {
        runnable.accept(connection);
      }
      // Deregister the current driver from the DriverManager to prevent 'the driver has already
      // been registered' errors.
      Driver driver = DriverManager.getDriver(url);
      DriverManager.deregisterDriver(driver);
    }
  }

  @Test
  public void testDatabaseMetaDataVersion() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals(
                server.getOptions().getServerVersion(), metadata.getDatabaseProductVersion());
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTables() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tables =
                metadata.getTables(null, "public", null, new String[] {"TABLE"})) {
              assertTrue(tables.next());
              assertEquals("albums", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("all_types", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("numbers", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("recording_attempt", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("singers", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("tracks", tables.getString("TABLE_NAME"));

              assertFalse(tables.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTables_FilteredByName() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tables =
                metadata.getTables(null, "public", "%a%", new String[] {"TABLE"})) {
              assertTrue(tables.next());
              assertEquals("albums", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("all_types", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("recording_attempt", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("tracks", tables.getString("TABLE_NAME"));

              assertFalse(tables.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataColumns() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet columns = metadata.getColumns(null, "public", "all_types", null)) {
              assertTrue(columns.next());
              assertEquals("col_bigint", columns.getString("COLUMN_NAME"));
              assertEquals(Types.BIGINT, columns.getInt("DATA_TYPE"));
              assertEquals("int8", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_bool", columns.getString("COLUMN_NAME"));
              assertEquals(Types.BIT, columns.getInt("DATA_TYPE"));
              assertEquals("bool", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_bytea", columns.getString("COLUMN_NAME"));
              assertEquals(Types.BINARY, columns.getInt("DATA_TYPE"));
              assertEquals("bytea", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_float8", columns.getString("COLUMN_NAME"));
              assertEquals(Types.DOUBLE, columns.getInt("DATA_TYPE"));
              assertEquals("float8", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_int", columns.getString("COLUMN_NAME"));
              assertEquals(Types.BIGINT, columns.getInt("DATA_TYPE"));
              assertEquals("int8", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_numeric", columns.getString("COLUMN_NAME"));
              assertEquals(Types.NUMERIC, columns.getInt("DATA_TYPE"));
              assertEquals("numeric", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_timestamptz", columns.getString("COLUMN_NAME"));
              assertEquals(Types.TIMESTAMP, columns.getInt("DATA_TYPE"));
              String name = columns.getString("TYPE_NAME");
              assertTrue("timestamptz".equals(name) || "timestamp with time zone".equals(name));

              assertTrue(columns.next());
              assertEquals("col_varchar", columns.getString("COLUMN_NAME"));
              assertEquals(Types.VARCHAR, columns.getInt("DATA_TYPE"));
              assertEquals("varchar", columns.getString("TYPE_NAME"));

              assertFalse(columns.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataColumns_FilteredByName() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet columns = metadata.getColumns(null, "public", "all_types", "%a%")) {
              assertTrue(columns.next());
              assertEquals("col_bytea", columns.getString("COLUMN_NAME"));
              assertEquals(Types.BINARY, columns.getInt("DATA_TYPE"));
              assertEquals("bytea", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_float8", columns.getString("COLUMN_NAME"));
              assertEquals(Types.DOUBLE, columns.getInt("DATA_TYPE"));
              assertEquals("float8", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_timestamptz", columns.getString("COLUMN_NAME"));
              assertEquals(Types.TIMESTAMP, columns.getInt("DATA_TYPE"));
              String name = columns.getString("TYPE_NAME");
              assertTrue("timestamptz".equals(name) || "timestamp with time zone".equals(name));

              assertTrue(columns.next());
              assertEquals("col_varchar", columns.getString("COLUMN_NAME"));
              assertEquals(Types.VARCHAR, columns.getInt("DATA_TYPE"));
              assertEquals("varchar", columns.getString("TYPE_NAME"));

              assertFalse(columns.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataIndexInfo() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet indexInfo =
                metadata.getIndexInfo(null, "public", "all_types", false, false)) {
              assertTrue(indexInfo.next());
              assertEquals("col_varchar", indexInfo.getString("COLUMN_NAME"));
              assertTrue(indexInfo.next());
              assertEquals("col_int", indexInfo.getString("COLUMN_NAME"));

              assertFalse(indexInfo.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataIndexInfo_Unique() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet indexInfo =
                metadata.getIndexInfo(null, "public", "numbers", true, false)) {
              assertTrue(indexInfo.next());
              assertEquals("name", indexInfo.getString("COLUMN_NAME"));

              assertFalse(indexInfo.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataPrimaryKeys() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet primaryKeys = metadata.getPrimaryKeys(null, "public", "all_types")) {
              assertTrue(primaryKeys.next());
              assertEquals("col_bigint", primaryKeys.getString("COLUMN_NAME"));
              assertEquals(1, primaryKeys.getShort("KEY_SEQ"));
              assertFalse(primaryKeys.next());
            }
            try (ResultSet primaryKeys = metadata.getPrimaryKeys(null, "public", "tracks")) {
              assertTrue(primaryKeys.next());
              assertEquals("album_id", primaryKeys.getString("COLUMN_NAME"));
              assertEquals(1, primaryKeys.getShort("KEY_SEQ"));
              assertTrue(primaryKeys.next());
              assertEquals("track_number", primaryKeys.getString("COLUMN_NAME"));
              assertEquals(2, primaryKeys.getShort("KEY_SEQ"));
              assertFalse(primaryKeys.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataExportedKeys() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet exportedKeys = metadata.getExportedKeys(null, "public", "singers")) {
              assertTrue(exportedKeys.next());
              assertEquals("singers", exportedKeys.getString("PKTABLE_NAME"));
              assertEquals("singer_id", exportedKeys.getString("PKCOLUMN_NAME"));
              assertEquals("albums", exportedKeys.getString("FKTABLE_NAME"));
              assertEquals("singer_id", exportedKeys.getString("FKCOLUMN_NAME"));
              assertEquals(1, exportedKeys.getShort("KEY_SEQ"));
              assertFalse(exportedKeys.next());
            }
            try (ResultSet exportedKeys = metadata.getExportedKeys(null, "public", "albums")) {
              assertTrue(exportedKeys.next());
              assertEquals("albums", exportedKeys.getString("PKTABLE_NAME"));
              assertEquals("album_id", exportedKeys.getString("PKCOLUMN_NAME"));
              assertEquals("recording_attempt", exportedKeys.getString("FKTABLE_NAME"));
              assertEquals("album", exportedKeys.getString("FKCOLUMN_NAME"));
              assertEquals(1, exportedKeys.getShort("KEY_SEQ"));

              assertTrue(exportedKeys.next());
              assertEquals("albums", exportedKeys.getString("PKTABLE_NAME"));
              assertEquals("album_id", exportedKeys.getString("PKCOLUMN_NAME"));
              assertEquals("tracks", exportedKeys.getString("FKTABLE_NAME"));
              assertEquals("album_id", exportedKeys.getString("FKCOLUMN_NAME"));
              assertEquals(1, exportedKeys.getShort("KEY_SEQ"));
              assertFalse(exportedKeys.next());
            }
            try (ResultSet exportedKeys = metadata.getExportedKeys(null, "public", "tracks")) {
              assertTrue(exportedKeys.next());
              assertEquals("tracks", exportedKeys.getString("PKTABLE_NAME"));
              assertEquals("album_id", exportedKeys.getString("PKCOLUMN_NAME"));
              assertEquals("recording_attempt", exportedKeys.getString("FKTABLE_NAME"));
              assertEquals("album", exportedKeys.getString("FKCOLUMN_NAME"));
              assertEquals(1, exportedKeys.getShort("KEY_SEQ"));
              assertTrue(exportedKeys.next());
              assertEquals("tracks", exportedKeys.getString("PKTABLE_NAME"));
              assertEquals("track_number", exportedKeys.getString("PKCOLUMN_NAME"));
              assertEquals("recording_attempt", exportedKeys.getString("FKTABLE_NAME"));
              assertEquals("track", exportedKeys.getString("FKCOLUMN_NAME"));
              assertEquals(2, exportedKeys.getShort("KEY_SEQ"));

              assertFalse(exportedKeys.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataImportedKeys() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet importedKeys = metadata.getImportedKeys(null, "public", "albums")) {
              assertTrue(importedKeys.next());
              assertEquals("singers", importedKeys.getString("PKTABLE_NAME"));
              assertEquals("singer_id", importedKeys.getString("PKCOLUMN_NAME"));
              assertEquals("albums", importedKeys.getString("FKTABLE_NAME"));
              assertEquals("singer_id", importedKeys.getString("FKCOLUMN_NAME"));
              assertEquals(1, importedKeys.getShort("KEY_SEQ"));
              assertFalse(importedKeys.next());
            }
            try (ResultSet importedKeys = metadata.getImportedKeys(null, "public", "tracks")) {
              assertTrue(importedKeys.next());
              assertEquals("albums", importedKeys.getString("PKTABLE_NAME"));
              assertEquals("album_id", importedKeys.getString("PKCOLUMN_NAME"));
              assertEquals("tracks", importedKeys.getString("FKTABLE_NAME"));
              assertEquals("album_id", importedKeys.getString("FKCOLUMN_NAME"));
              assertEquals(1, importedKeys.getShort("KEY_SEQ"));
              assertFalse(importedKeys.next());
            }
            try (ResultSet importedKeys =
                metadata.getImportedKeys(null, "public", "recording_attempt")) {
              assertTrue(importedKeys.next());
              assertEquals("albums", importedKeys.getString("PKTABLE_NAME"));
              assertEquals("album_id", importedKeys.getString("PKCOLUMN_NAME"));
              assertEquals("recording_attempt", importedKeys.getString("FKTABLE_NAME"));
              assertEquals("album", importedKeys.getString("FKCOLUMN_NAME"));
              assertEquals(1, importedKeys.getShort("KEY_SEQ"));

              assertTrue(importedKeys.next());
              assertEquals("tracks", importedKeys.getString("PKTABLE_NAME"));
              assertEquals("album_id", importedKeys.getString("PKCOLUMN_NAME"));
              assertEquals("recording_attempt", importedKeys.getString("FKTABLE_NAME"));
              assertEquals("album", importedKeys.getString("FKCOLUMN_NAME"));
              assertEquals(1, importedKeys.getShort("KEY_SEQ"));
              assertTrue(importedKeys.next());
              assertEquals("tracks", importedKeys.getString("PKTABLE_NAME"));
              assertEquals("track_number", importedKeys.getString("PKCOLUMN_NAME"));
              assertEquals("recording_attempt", importedKeys.getString("FKTABLE_NAME"));
              assertEquals("track", importedKeys.getString("FKCOLUMN_NAME"));
              assertEquals(2, importedKeys.getShort("KEY_SEQ"));

              assertFalse(importedKeys.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataCrossReference() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            // Retrieve the references from albums (foreign table) to singers (parent table).
            try (ResultSet references =
                metadata.getCrossReference(null, "public", "singers", null, "public", "albums")) {
              assertTrue(references.next());
              assertEquals("singers", references.getString("PKTABLE_NAME"));
              assertEquals("singer_id", references.getString("PKCOLUMN_NAME"));
              assertEquals("albums", references.getString("FKTABLE_NAME"));
              assertEquals("singer_id", references.getString("FKCOLUMN_NAME"));
              assertEquals(1, references.getShort("KEY_SEQ"));
              assertFalse(references.next());
            }
            // Retrieve the references from tracks to albums.
            try (ResultSet references =
                metadata.getCrossReference(null, "public", "albums", null, "public", "tracks")) {
              assertTrue(references.next());
              assertEquals("albums", references.getString("PKTABLE_NAME"));
              assertEquals("album_id", references.getString("PKCOLUMN_NAME"));
              assertEquals("tracks", references.getString("FKTABLE_NAME"));
              assertEquals("album_id", references.getString("FKCOLUMN_NAME"));
              assertEquals(1, references.getShort("KEY_SEQ"));
              assertFalse(references.next());
            }
            // Retrieve the references from recording_attempt to albums.
            try (ResultSet references =
                metadata.getCrossReference(
                    null, "public", "albums", null, "public", "recording_attempt")) {
              assertTrue(references.next());
              assertEquals("albums", references.getString("PKTABLE_NAME"));
              assertEquals("album_id", references.getString("PKCOLUMN_NAME"));
              assertEquals("recording_attempt", references.getString("FKTABLE_NAME"));
              assertEquals("album", references.getString("FKCOLUMN_NAME"));
              assertEquals(1, references.getShort("KEY_SEQ"));

              assertFalse(references.next());
            }
            // Retrieve the references from recording_attempt to tracks.
            try (ResultSet references =
                metadata.getCrossReference(
                    null, "public", "tracks", null, "public", "recording_attempt")) {
              assertTrue(references.next());
              assertEquals("tracks", references.getString("PKTABLE_NAME"));
              assertEquals("album_id", references.getString("PKCOLUMN_NAME"));
              assertEquals("recording_attempt", references.getString("FKTABLE_NAME"));
              assertEquals("album", references.getString("FKCOLUMN_NAME"));
              assertEquals(1, references.getShort("KEY_SEQ"));
              assertTrue(references.next());
              assertEquals("tracks", references.getString("PKTABLE_NAME"));
              assertEquals("track_number", references.getString("PKCOLUMN_NAME"));
              assertEquals("recording_attempt", references.getString("FKTABLE_NAME"));
              assertEquals("track", references.getString("FKCOLUMN_NAME"));
              assertEquals(2, references.getShort("KEY_SEQ"));

              assertFalse(references.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTypeInfo() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            Map<String, Integer> results = new HashMap<>();
            try (ResultSet types = metadata.getTypeInfo()) {
              while (types.next()) {
                results.put(types.getString("TYPE_NAME"), types.getInt("DATA_TYPE"));
              }
            }
            assertTrue(results.containsKey("bool"));
            assertEquals(Types.BIT, results.get("bool").intValue());
            assertTrue(results.containsKey("bytea"));
            assertEquals(Types.BINARY, results.get("bytea").intValue());
            assertTrue(results.containsKey("float8"));
            assertEquals(Types.DOUBLE, results.get("float8").intValue());
            assertTrue(results.containsKey("int8"));
            assertEquals(Types.BIGINT, results.get("int8").intValue());
            assertTrue(results.containsKey("numeric"));
            assertEquals(Types.NUMERIC, results.get("numeric").intValue());
            assertTrue(results.containsKey("timestamptz"));
            assertEquals(Types.TIMESTAMP, results.get("timestamptz").intValue());
            assertTrue(results.containsKey("varchar"));
            assertEquals(Types.VARCHAR, results.get("varchar").intValue());
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testDatabaseMetaDataSuperTypes() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            String.format("jdbc:postgresql://localhost:%d/", testEnv.getPort()))) {
      DatabaseMetaData metadata = connection.getMetaData();
      // This method is not supported by the PG JDBC driver.
      try (ResultSet types = metadata.getSuperTypes(null, null, null)) {
        assertFalse(types.next());
      }
    }
  }

  @Test
  public void testDatabaseMetaDataTableTypes() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            boolean hasTable = false;
            boolean hasView = false;
            try (ResultSet tableTypes = metadata.getTableTypes()) {
              while (tableTypes.next()) {
                hasTable = hasTable || "TABLE".equals(tableTypes.getString("TABLE_TYPE"));
                hasView = hasView || "VIEW".equals(tableTypes.getString("TABLE_TYPE"));
              }
            }
            assertTrue(hasTable);
            assertTrue(hasView);
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTablePrivileges() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tablePrivileges = metadata.getTablePrivileges(null, null, null)) {
              assertFalse(tablePrivileges.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataColumnPrivileges() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet columnPrivileges =
                metadata.getColumnPrivileges(null, null, null, null)) {
              assertFalse(columnPrivileges.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testDatabaseMetaDataAttributes() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            String.format("jdbc:postgresql://localhost:%d/", testEnv.getPort()))) {
      DatabaseMetaData metadata = connection.getMetaData();
      // This method is not supported by the PG JDBC driver.
      try (ResultSet attributes = metadata.getAttributes(null, null, null, null)) {
        assertFalse(attributes.next());
      }
    }
  }

  @Test
  public void testDatabaseMetaDataBestRowIdentifier() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            // PG just returns the primary key of each table for this method.
            try (ResultSet rowIdentifiers =
                metadata.getBestRowIdentifier(
                    null, "public", "singers", DatabaseMetaData.bestRowTransaction, false)) {
              assertTrue(rowIdentifiers.next());
              assertEquals("singer_id", rowIdentifiers.getString("COLUMN_NAME"));
              assertEquals("int8", rowIdentifiers.getString("TYPE_NAME"));
              assertFalse(rowIdentifiers.next());
            }
            try (ResultSet rowIdentifiers =
                metadata.getBestRowIdentifier(
                    null, "public", "tracks", DatabaseMetaData.bestRowTransaction, false)) {
              assertTrue(rowIdentifiers.next());
              assertEquals("album_id", rowIdentifiers.getString("COLUMN_NAME"));
              assertEquals("int8", rowIdentifiers.getString("TYPE_NAME"));
              assertTrue(rowIdentifiers.next());
              assertEquals("track_number", rowIdentifiers.getString("COLUMN_NAME"));
              assertEquals("int8", rowIdentifiers.getString("TYPE_NAME"));
              assertFalse(rowIdentifiers.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataCatalogs() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet catalogs = metadata.getCatalogs()) {
              assertTrue(catalogs.next());
              assertEquals("", catalogs.getString("TABLE_CAT"));
              assertFalse(catalogs.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataSchemas() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet schemas = metadata.getSchemas()) {
              assertTrue(schemas.next());
              assertEquals(database.getId().getDatabase(), schemas.getString("TABLE_CATALOG"));
              assertEquals("information_schema", schemas.getString("TABLE_SCHEM"));

              assertTrue(schemas.next());
              assertEquals(database.getId().getDatabase(), schemas.getString("TABLE_CATALOG"));
              assertEquals("pg_catalog", schemas.getString("TABLE_SCHEM"));

              assertTrue(schemas.next());
              assertEquals(database.getId().getDatabase(), schemas.getString("TABLE_CATALOG"));
              assertEquals("public", schemas.getString("TABLE_SCHEM"));

              assertTrue(schemas.next());
              assertEquals(database.getId().getDatabase(), schemas.getString("TABLE_CATALOG"));
              assertEquals("spanner_sys", schemas.getString("TABLE_SCHEM"));

              assertFalse(schemas.next());
            }

            try (ResultSet schemas = metadata.getSchemas(null, "public")) {
              assertTrue(schemas.next());
              assertEquals(database.getId().getDatabase(), schemas.getString("TABLE_CATALOG"));
              assertEquals("public", schemas.getString("TABLE_SCHEM"));

              assertFalse(schemas.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataClientInfoProperties() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet clientInfoProperties = metadata.getClientInfoProperties()) {
              // Ignore values, just check that it does not fail.
              clientInfoProperties.next();
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataFunctions() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet functions = metadata.getFunctions(null, null, null)) {
              assertFalse(functions.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataFunctionColumns() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet functionColumns = metadata.getFunctionColumns(null, null, null, null)) {
              assertFalse(functionColumns.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataProcedures() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet procedures = metadata.getProcedures(null, null, null)) {
              assertFalse(procedures.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataProcedureColumns() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet procedureColumns =
                metadata.getProcedureColumns(null, null, null, null)) {
              assertFalse(procedureColumns.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testDatabaseMetaDataPseudoColumns() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            String.format("jdbc:postgresql://localhost:%d/", testEnv.getPort()))) {
      DatabaseMetaData metadata = connection.getMetaData();
      try (ResultSet pseudoColumns = metadata.getPseudoColumns(null, null, null, null)) {
        assertFalse(pseudoColumns.next());
      }
    }
  }

  @Test
  public void testDatabaseMetaDataVersionColumns() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet versionColumns = metadata.getVersionColumns(null, null, null)) {
              // Just ensure it does not fail, ignore any results.
              assertTrue(versionColumns.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataUDTs() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet types = metadata.getUDTs(null, null, null, null)) {
              assertFalse(types.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataMaxNameLength() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals(128, metadata.getMaxColumnNameLength());
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataSQLKeywords() throws Exception {
    runForAllVersions(
        connection -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue(metadata.getSQLKeywords().length() > 0);
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }
}
