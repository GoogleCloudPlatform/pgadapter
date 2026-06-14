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

import static com.google.cloud.spanner.pgadapter.ITJdbcMetadataTest.getDdlStatements;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Database;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITPgClassTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();

  @BeforeClass
  public static void setup() {
    testEnv.setUp();
    Database database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), ImmutableList.of());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  private String getConnectionUrl() {
    return String.format("jdbc:postgresql://%s/", testEnv.getPGAdapterHostAndPort());
  }

  private static class PgClassRow {
    final String relname;
    final String relkind;
    final int relnatts;
    // Relchecks is not implemented and always returns zero.
    final int relchecks;

    PgClassRow(String relname, String relkind, int relnatts, int relchecks) {
      this.relname = relname;
      this.relkind = relkind;
      this.relnatts = relnatts;
      this.relchecks = relchecks;
    }
  }

  @Test
  public void testPgClass() throws SQLException {
    java.util.List<PgClassRow> expectedRows =
        new java.util.ArrayList<>(
            java.util.Arrays.asList(
                new PgClassRow("albums", "r", 3, 0),
                new PgClassRow("IDX_albums_singer_id_%", "i", 1, 0),
                new PgClassRow("PRIMARY_KEY", "i", 1, 0),
                new PgClassRow("all_types", "r", 22, 0),
                new PgClassRow("PRIMARY_KEY", "i", 1, 0),
                new PgClassRow("idx_col_varchar_int", "i", 2, 0),
                new PgClassRow("numbers", "r", 2, 0),
                new PgClassRow("PRIMARY_KEY", "i", 1, 0),
                new PgClassRow("idx_numbers_name", "i", 1, 0),
                new PgClassRow("recording_attempt", "r", 4, 0),
                new PgClassRow("PRIMARY_KEY", "i", 3, 0),
                new PgClassRow("singers", "r", 2, 0),
                new PgClassRow("PRIMARY_KEY", "i", 1, 0),
                new PgClassRow("tracks", "r", 3, 0),
                new PgClassRow("PRIMARY_KEY", "i", 2, 0)));
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "select oid, relname, relkind, relnatts, relchecks "
                      + "from pg_class "
                      + "where relnamespace=2200 "
                      + "order by oid")) {
        while (resultSet.next()) {
          assertTrue(resultSet.getLong("oid") != 0);
          String relname = resultSet.getString("relname");
          String relkind = resultSet.getString("relkind");
          int relnatts = resultSet.getInt("relnatts");
          int relchecks = resultSet.getInt("relchecks");
          boolean found = false;
          for (int i = 0; i < expectedRows.size(); i++) {
            PgClassRow expected = expectedRows.get(i);
            boolean nameMatch =
                expected.relname.endsWith("%")
                    ? relname.startsWith(
                        expected.relname.substring(0, expected.relname.length() - 1))
                    : relname.equals(expected.relname);
            if (nameMatch
                && relkind.equals(expected.relkind)
                && relnatts == expected.relnatts
                && relchecks == expected.relchecks) {
              expectedRows.remove(i);
              found = true;
              break;
            }
          }
          assertTrue("Unexpected row: " + relname, found);
        }
        assertTrue("Missing rows", expectedRows.isEmpty());
      }
    }
  }

  private static class PgIndexRow {

    final int indnatts;
    final int indnkeyatts;
    final boolean indisunique;
    final boolean indnullsnotdistinct;
    final boolean indisprimary;
    final String indpred;

    PgIndexRow(
        int indnatts,
        int indnkeyatts,
        boolean indisunique,
        boolean indnullsnotdistinct,
        boolean indisprimary,
        String indpred) {
      this.indnatts = indnatts;
      this.indnkeyatts = indnkeyatts;
      this.indisunique = indisunique;
      this.indnullsnotdistinct = indnullsnotdistinct;
      this.indisprimary = indisprimary;
      this.indpred = indpred;
    }
  }

  @Test
  public void testPgIndex() throws SQLException {
    String sql =
        "select indexrelid, indrelid, indnatts, indnkeyatts, indisunique, "
            + "indnullsnotdistinct, indisprimary, indpred "
            + "from pg_index "
            + "order by indexrelid";
    java.util.List<PgIndexRow> expectedRows =
        new java.util.ArrayList<>(
            java.util.Arrays.asList(
                new PgIndexRow(1, 1, false, false, false, null),
                new PgIndexRow(1, 1, true, true, true, null),
                new PgIndexRow(1, 1, true, true, true, null),
                new PgIndexRow(2, 2, false, false, false, null),
                new PgIndexRow(1, 1, true, true, true, null),
                new PgIndexRow(1, 1, true, true, false, "name IS NOT NULL"),
                new PgIndexRow(3, 3, true, true, true, null),
                new PgIndexRow(1, 1, true, true, true, null),
                new PgIndexRow(2, 2, true, true, true, null)));
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          assertTrue(resultSet.getLong("indexrelid") != 0);
          assertTrue(resultSet.getLong("indrelid") != 0);
          int indnatts = resultSet.getInt("indnatts");
          int indnkeyatts = resultSet.getInt("indnkeyatts");
          boolean indisunique = resultSet.getBoolean("indisunique");
          boolean indnullsnotdistinct = resultSet.getBoolean("indnullsnotdistinct");
          boolean indisprimary = resultSet.getBoolean("indisprimary");
          String indpred = resultSet.getString("indpred");

          boolean found = false;
          for (int i = 0; i < expectedRows.size(); i++) {
            PgIndexRow expected = expectedRows.get(i);
            boolean predMatch =
                expected.indpred == null ? indpred == null : expected.indpred.equals(indpred);
            if (expected.indnatts == indnatts
                && expected.indnkeyatts == indnkeyatts
                && expected.indisunique == indisunique
                && expected.indnullsnotdistinct == indnullsnotdistinct
                && expected.indisprimary == indisprimary
                && predMatch) {
              expectedRows.remove(i);
              found = true;
              break;
            }
          }
          assertTrue("Unexpected index row: " + indnatts + ", " + indisprimary, found);
        }
        assertTrue("Missing index rows", expectedRows.isEmpty());
      }
    }
  }

  @Test
  public void testPgAttribute() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "select count(*) " + "from pg_attribute " + "where attrelid=?")) {
        try (ResultSet pgClass =
            connection
                .createStatement()
                .executeQuery(
                    "select oid, relnatts " + "from pg_class " + "where relnamespace=2200")) {
          while (pgClass.next()) {
            statement.setObject(1, pgClass.getObject(1));
            try (ResultSet numAttributes = statement.executeQuery()) {
              assertTrue(numAttributes.next());
              assertEquals(pgClass.getLong(2), numAttributes.getLong(1));
              assertFalse(numAttributes.next());
            }
          }
        }
      }
    }
  }

  @Test
  public void testEmulatePgAttribute() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      connection.createStatement().execute("set spanner.emulate_pg_class_tables=true");
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from pg_attribute limit 1")) {
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
      }
      connection.createStatement().execute("set spanner.emulate_pg_class_tables=false");
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from pg_attribute limit 1")) {
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testPgCollation() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet collations =
          connection.createStatement().executeQuery("select * from pg_collation")) {
        assertTrue(collations.next());
        assertEquals(100, collations.getInt("oid"));
        assertEquals("default", collations.getString("collname"));
        assertFalse(collations.next());
      }
    }
  }

  @Test
  public void testPgExtension() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet extensions =
          connection.createStatement().executeQuery("select * from pg_extension")) {
        assertEquals(8, extensions.getMetaData().getColumnCount());
        assertFalse(extensions.next());
      }
    }
  }

  @Test
  public void testPgType() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      for (boolean emulate : new boolean[] {true, false}) {
        connection.createStatement().execute("set spanner.emulate_pg_class_tables=" + emulate);
        try (ResultSet types = connection.createStatement().executeQuery("select * from pg_type")) {
          int count = 0;
          while (types.next()) {
            assertEquals(0, types.getInt("typrelid"));
            count++;
          }
          assertEquals(31, count);
        }
      }
    }
  }

  @Test
  public void testPgAttrdef() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from pg_attrdef")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getLong("oid") != 0);
        assertTrue(resultSet.getLong("adrelid") != 0);
        assertEquals(4, resultSet.getInt("adnum"));
        assertEquals("now()", resultSet.getString("adbin"));
        assertFalse(resultSet.next());
      }
    }
  }

  private static class PgConstraintRow {
    final String conname;
    final char contype;
    final String confupdtype;
    final String confdeltype;
    final String confmatchtype;
    final Long[] conkey;
    final Long[] confkey;
    final String conbin;

    private PgConstraintRow(
        String conname,
        char contype,
        String confupdtype,
        String confdeltype,
        String confmatchtype,
        Long[] conkey,
        Long[] confkey,
        String conbin) {
      this.conname = conname;
      this.contype = contype;
      this.confupdtype = confupdtype;
      this.confdeltype = confdeltype;
      this.confmatchtype = confmatchtype;
      this.conkey = conkey;
      this.confkey = confkey;
      this.conbin = conbin;
    }
  }

  @Test
  public void testPgConstraint() throws SQLException {
    java.util.List<PgConstraintRow> expectedRows =
        new java.util.ArrayList<>(
            java.util.Arrays.asList(
                new PgConstraintRow(
                    "FK_albums_singers_%",
                    'f', "a", "a", "s", new Long[] {2L}, new Long[] {1L}, null),
                new PgConstraintRow(
                    "FK_recording_attempt_albums_%",
                    'f', "a", "a", "s", new Long[] {1L}, new Long[] {1L}, null),
                new PgConstraintRow(
                    "FK_recording_attempt_tracks_%",
                    'f', "a", "a", "s", new Long[] {1L, 2L}, new Long[] {1L, 2L}, null),
                new PgConstraintRow(
                    "FK_tracks_albums_%",
                    'f', "a", "a", "s", new Long[] {1L}, new Long[] {1L}, null),
                new PgConstraintRow("PK_albums", 'p', null, null, "s", new Long[] {1L}, null, null),
                new PgConstraintRow(
                    "PK_all_types", 'p', null, null, "s", new Long[] {1L}, null, null),
                new PgConstraintRow(
                    "PK_numbers", 'p', null, null, "s", new Long[] {1L}, null, null),
                new PgConstraintRow(
                    "PK_recording_attempt",
                    'p',
                    null,
                    null,
                    "s",
                    new Long[] {1L, 2L, 3L},
                    null,
                    null),
                new PgConstraintRow(
                    "PK_singers", 'p', null, null, "s", new Long[] {1L}, null, null),
                new PgConstraintRow(
                    "PK_tracks", 'p', null, null, "s", new Long[] {1L, 2L}, null, null),
                new PgConstraintRow(
                    "recording_attempt_greater_than_zero",
                    'c',
                    null,
                    null,
                    "s",
                    new Long[] {3L},
                    null,
                    "(attempt > '0'::bigint)")));

    String sql =
        "select oid, conname, contype, conrelid, conindid, confrelid, confupdtype, confdeltype, "
            + "confmatchtype, conkey, confkey, conbin "
            + "from pg_constraint "
            + "order by oid";
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          assertTrue(resultSet.getLong("oid") != 0);
          String conname = resultSet.getString("conname");
          char contype = resultSet.getString("contype").charAt(0);
          String confupdtype = resultSet.getString("confupdtype");
          String confdeltype = resultSet.getString("confdeltype");
          String confmatchtype = resultSet.getString("confmatchtype");
          Long[] conkey =
              IntegrationTest.isRunningOnEmulator()
                  ? null
                  : (Long[]) resultSet.getArray("conkey").getArray();
          Long[] confkey =
              resultSet.getArray("confkey") == null
                  ? null
                  : (Long[]) resultSet.getArray("confkey").getArray();
          String conbin = resultSet.getString("conbin");

          boolean found = false;
          for (int i = 0; i < expectedRows.size(); i++) {
            PgConstraintRow expected = expectedRows.get(i);
            boolean nameMatch =
                expected.conname.endsWith("%")
                    ? conname.startsWith(
                        expected.conname.substring(0, expected.conname.length() - 1))
                    : conname.equals(expected.conname);
            boolean keyMatch =
                IntegrationTest.isRunningOnEmulator()
                    || java.util.Arrays.equals(expected.conkey, conkey);
            boolean fkeyMatch = java.util.Arrays.equals(expected.confkey, confkey);
            boolean strMatch = true;
            if (expected.confupdtype != null && !expected.confupdtype.equals(confupdtype))
              strMatch = false;
            if (expected.confupdtype == null && confupdtype != null) strMatch = false;
            if (expected.confdeltype != null && !expected.confdeltype.equals(confdeltype))
              strMatch = false;
            if (expected.confdeltype == null && confdeltype != null) strMatch = false;
            if (expected.confmatchtype != null && !expected.confmatchtype.equals(confmatchtype))
              strMatch = false;
            if (expected.confmatchtype == null && confmatchtype != null) strMatch = false;
            if (expected.conbin != null && !expected.conbin.equals(conbin)) strMatch = false;
            if (expected.conbin == null && conbin != null) strMatch = false;
            if (nameMatch && contype == expected.contype && keyMatch && fkeyMatch && strMatch) {
              expectedRows.remove(i);
              found = true;
              break;
            }
          }
          assertTrue("Unexpected constraint row: " + conname, found);
        }
        assertTrue("Missing constraint rows", expectedRows.isEmpty());
      }
    }
  }
}
