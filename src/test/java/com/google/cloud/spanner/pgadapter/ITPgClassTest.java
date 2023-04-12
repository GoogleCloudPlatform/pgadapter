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
import static org.junit.Assert.assertArrayEquals;
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
    final String oid;
    final String relname;
    final String relkind;
    final int relnatts;
    // Relchecks is not implemented and always returns zero.
    final int relchecks;

    PgClassRow(String oid, String relname, String relkind, int relnatts, int relchecks) {
      this.oid = oid;
      this.relname = relname;
      this.relkind = relkind;
      this.relnatts = relnatts;
      this.relchecks = relchecks;
    }
  }

  @Test
  public void testPgClass() throws SQLException {
    ImmutableList<PgClassRow> expectedRows =
        ImmutableList.of(
            new PgClassRow("'\"public\".\"albums\"'", "albums", "r", 3, 0),
            new PgClassRow(
                "'\"public\".\"albums\".\"IDX_albums_singer_id_%",
                "IDX_albums_singer_id_%", "i", 1, 0),
            new PgClassRow("'\"public\".\"albums\".\"PRIMARY_KEY\"'", "PRIMARY_KEY", "i", 1, 0),
            new PgClassRow("'\"public\".\"all_types\"'", "all_types", "r", 20, 0),
            new PgClassRow("'\"public\".\"all_types\".\"PRIMARY_KEY\"'", "PRIMARY_KEY", "i", 1, 0),
            new PgClassRow(
                "'\"public\".\"all_types\".\"idx_col_varchar_int\"'",
                "idx_col_varchar_int",
                "i",
                2,
                0),
            new PgClassRow("'\"public\".\"numbers\"'", "numbers", "r", 2, 0),
            new PgClassRow("'\"public\".\"numbers\".\"PRIMARY_KEY\"'", "PRIMARY_KEY", "i", 1, 0),
            new PgClassRow(
                "'\"public\".\"numbers\".\"idx_numbers_name\"'", "idx_numbers_name", "i", 1, 0),
            new PgClassRow("'\"public\".\"recording_attempt\"'", "recording_attempt", "r", 4, 0),
            new PgClassRow(
                "'\"public\".\"recording_attempt\".\"PRIMARY_KEY\"'", "PRIMARY_KEY", "i", 3, 0),
            new PgClassRow("'\"public\".\"singers\"'", "singers", "r", 2, 0),
            new PgClassRow("'\"public\".\"singers\".\"PRIMARY_KEY\"'", "PRIMARY_KEY", "i", 1, 0),
            new PgClassRow("'\"public\".\"tracks\"'", "tracks", "r", 3, 0),
            new PgClassRow("'\"public\".\"tracks\".\"PRIMARY_KEY\"'", "PRIMARY_KEY", "i", 2, 0));
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "select oid, relname, relkind, relnatts, relchecks "
                      + "from pg_class "
                      + "where relnamespace=2200 "
                      + "order by oid")) {
        int index = 0;
        while (resultSet.next()) {
          assertTrue(index < expectedRows.size());
          PgClassRow expected = expectedRows.get(index);
          if (expected.oid.endsWith("%")) {
            assertTrue(
                resultSet.getString("oid"),
                resultSet
                    .getString("oid")
                    .startsWith(expected.oid.substring(0, expected.oid.length() - 1)));
            assertTrue(
                resultSet.getString("relname"),
                resultSet
                    .getString("relname")
                    .startsWith(expected.relname.substring(0, expected.relname.length() - 1)));
          } else {
            assertEquals(expected.oid, resultSet.getString("oid"));
            assertEquals(expected.relname, resultSet.getString("relname"));
          }
          assertEquals(expected.oid, expected.relkind, resultSet.getString("relkind"));
          assertEquals(expected.oid, expected.relnatts, resultSet.getInt("relnatts"));
          assertEquals(expected.oid, expected.relchecks, resultSet.getInt("relchecks"));
          index++;
        }
        assertEquals(index, expectedRows.size());
      }
    }
  }

  private static class PgIndexRow {
    final String indexrelid;
    final String indrelid;
    final int indnatts;
    final int indnkeyatts;
    final boolean indisunique;
    final boolean indnullsnotdistinct;
    final boolean indisprimary;
    final String indpred;

    PgIndexRow(
        String indexrelid,
        String indrelid,
        int indnatts,
        int indnkeyatts,
        boolean indisunique,
        boolean indnullsnotdistinct,
        boolean indisprimary,
        String indpred) {
      this.indexrelid = indexrelid;
      this.indrelid = indrelid;
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
    ImmutableList<PgIndexRow> expectedRows =
        ImmutableList.of(
            new PgIndexRow(
                "'\"public\".\"albums\".\"IDX_albums_singer_id_%",
                "'\"public\".\"albums\"'", 1, 1, false, false, false, null),
            new PgIndexRow(
                "'\"public\".\"albums\".\"PRIMARY_KEY\"'",
                "'\"public\".\"albums\"'",
                1,
                1,
                true,
                true,
                true,
                null),
            new PgIndexRow(
                "'\"public\".\"all_types\".\"PRIMARY_KEY\"'",
                "'\"public\".\"all_types\"'",
                1,
                1,
                true,
                true,
                true,
                null),
            new PgIndexRow(
                "'\"public\".\"all_types\".\"idx_col_varchar_int\"'",
                "'\"public\".\"all_types\"'",
                2,
                2,
                false,
                false,
                false,
                null),
            new PgIndexRow(
                "'\"public\".\"numbers\".\"PRIMARY_KEY\"'",
                "'\"public\".\"numbers\"'",
                1,
                1,
                true,
                true,
                true,
                null),
            new PgIndexRow(
                "'\"public\".\"numbers\".\"idx_numbers_name\"'",
                "'\"public\".\"numbers\"'",
                1,
                1,
                true,
                true,
                false,
                "name IS NOT NULL"),
            new PgIndexRow(
                "'\"public\".\"recording_attempt\".\"PRIMARY_KEY\"'",
                "'\"public\".\"recording_attempt\"'",
                3,
                3,
                true,
                true,
                true,
                null),
            new PgIndexRow(
                "'\"public\".\"singers\".\"PRIMARY_KEY\"'",
                "'\"public\".\"singers\"'",
                1,
                1,
                true,
                true,
                true,
                null),
            new PgIndexRow(
                "'\"public\".\"tracks\".\"PRIMARY_KEY\"'",
                "'\"public\".\"tracks\"'",
                2,
                2,
                true,
                true,
                true,
                null));
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        int index = 0;
        while (resultSet.next()) {
          assertTrue(index < expectedRows.size());
          PgIndexRow expected = expectedRows.get(index);
          if (expected.indexrelid.endsWith("%")) {
            assertTrue(
                resultSet.getString("indexrelid"),
                resultSet
                    .getString("indexrelid")
                    .startsWith(
                        expected.indexrelid.substring(0, expected.indexrelid.length() - 1)));
          } else {
            assertEquals(expected.indexrelid, resultSet.getString("indexrelid"));
          }
          assertEquals(expected.indexrelid, expected.indrelid, resultSet.getString("indrelid"));
          assertEquals(expected.indexrelid, expected.indnatts, resultSet.getInt("indnatts"));
          assertEquals(expected.indexrelid, expected.indnkeyatts, resultSet.getInt("indnkeyatts"));
          assertEquals(
              expected.indexrelid, expected.indisunique, resultSet.getBoolean("indisunique"));
          assertEquals(
              expected.indexrelid,
              expected.indnullsnotdistinct,
              resultSet.getBoolean("indnullsnotdistinct"));
          assertEquals(
              expected.indexrelid, expected.indisprimary, resultSet.getBoolean("indisprimary"));
          assertEquals(expected.indexrelid, expected.indpred, resultSet.getString("indpred"));
          index++;
        }
        assertEquals(index, expectedRows.size());
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
          assertEquals(28, count);
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
        assertEquals(
            "'\"public\".\"recording_attempt\".\"recording_time\"'", resultSet.getString("oid"));
        assertEquals("'\"public\".\"recording_attempt\"'", resultSet.getString("adrelid"));
        assertEquals(4, resultSet.getInt("adnum"));
        assertEquals("now()", resultSet.getString("adbin"));
        assertFalse(resultSet.next());
      }
    }
  }

  private static class PgConstraintRow {
    final String oid;
    final String conname;
    final char contype;
    final String conrelid;
    final String conindid; // Currently not implemented
    final String confrelid;
    final String confupdtype;
    final String confdeltype;
    final String confmatchtype;
    final Long[] conkey;
    final Long[] confkey;
    final String conbin;

    private PgConstraintRow(
        String oid,
        String conname,
        char contype,
        String conrelid,
        String conindid,
        String confrelid,
        String confupdtype,
        String confdeltype,
        String confmatchtype,
        Long[] conkey,
        Long[] confkey,
        String conbin) {
      this.oid = oid;
      this.conname = conname;
      this.contype = contype;
      this.conrelid = conrelid;
      this.conindid = conindid;
      this.confrelid = confrelid;
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
    ImmutableList<PgConstraintRow> expectedRows =
        ImmutableList.of(
            new PgConstraintRow(
                "'\"public\".\"FK_albums_singers_%",
                "FK_albums_singers_%",
                'f',
                "'\"public\".\"albums\"'",
                "0",
                "'\"public\".\"singers\"'",
                "a",
                "a",
                "s",
                new Long[] {2L},
                new Long[] {1L},
                null),
            new PgConstraintRow(
                "'\"public\".\"FK_recording_attempt_albums_%",
                "FK_recording_attempt_albums_%",
                'f',
                "'\"public\".\"recording_attempt\"'",
                "0",
                "'\"public\".\"albums\"'",
                "a",
                "a",
                "s",
                new Long[] {1L},
                new Long[] {1L},
                null),
            new PgConstraintRow(
                "'\"public\".\"FK_recording_attempt_tracks_%",
                "FK_recording_attempt_tracks_%",
                'f',
                "'\"public\".\"recording_attempt\"'",
                "0",
                "'\"public\".\"tracks\"'",
                "a",
                "a",
                "s",
                new Long[] {1L, 2L},
                new Long[] {1L, 2L},
                null),
            new PgConstraintRow(
                "'\"public\".\"FK_tracks_albums_%",
                "FK_tracks_albums_%",
                'f',
                "'\"public\".\"tracks\"'",
                "0",
                "'\"public\".\"albums\"'",
                "a",
                "a",
                "s",
                new Long[] {1L},
                new Long[] {1L},
                null),
            new PgConstraintRow(
                "'\"public\".\"PK_albums\"'",
                "PK_albums",
                'p',
                "'\"public\".\"albums\"'",
                "0",
                null,
                null,
                null,
                "s",
                new Long[] {1L},
                null,
                null),
            new PgConstraintRow(
                "'\"public\".\"PK_all_types\"'",
                "PK_all_types",
                'p',
                "'\"public\".\"all_types\"'",
                "0",
                null,
                null,
                null,
                "s",
                new Long[] {1L},
                null,
                null),
            new PgConstraintRow(
                "'\"public\".\"PK_numbers\"'",
                "PK_numbers",
                'p',
                "'\"public\".\"numbers\"'",
                "0",
                null,
                null,
                null,
                "s",
                new Long[] {1L},
                null,
                null),
            new PgConstraintRow(
                "'\"public\".\"PK_recording_attempt\"'",
                "PK_recording_attempt",
                'p',
                "'\"public\".\"recording_attempt\"'",
                "0",
                null,
                null,
                null,
                "s",
                new Long[] {1L, 2L, 3L},
                null,
                null),
            new PgConstraintRow(
                "'\"public\".\"PK_singers\"'",
                "PK_singers",
                'p',
                "'\"public\".\"singers\"'",
                "0",
                null,
                null,
                null,
                "s",
                new Long[] {1L},
                null,
                null),
            new PgConstraintRow(
                "'\"public\".\"PK_tracks\"'",
                "PK_tracks",
                'p',
                "'\"public\".\"tracks\"'",
                "0",
                null,
                null,
                null,
                "s",
                new Long[] {1L, 2L},
                null,
                null),
            new PgConstraintRow(
                "'\"public\".\"recording_attempt_greater_than_zero\"'",
                "recording_attempt_greater_than_zero",
                'c',
                "'\"public\".\"recording_attempt\"'",
                "0",
                null,
                null,
                null,
                "s",
                new Long[] {3L},
                null,
                "(attempt > '0'::bigint)"));

    String sql =
        "select oid, conname, contype, conrelid, conindid, confrelid, confupdtype, confdeltype, "
            + "confmatchtype, conkey, confkey, conbin "
            + "from pg_constraint "
            + "order by oid";
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        int index = 0;
        while (resultSet.next()) {
          assertTrue(resultSet.getString("oid"), index < expectedRows.size());
          PgConstraintRow expected = expectedRows.get(index);
          if (expected.oid.endsWith("%")) {
            assertTrue(
                expected.oid,
                resultSet
                    .getString("oid")
                    .startsWith(expected.oid.substring(0, expected.oid.length() - 1)));
            assertTrue(
                expected.conname,
                resultSet
                    .getString("conname")
                    .startsWith(expected.conname.substring(0, expected.conname.length() - 1)));
          } else {
            assertEquals(expected.oid, resultSet.getString("oid"));
            assertEquals(expected.conname, resultSet.getString("conname"));
          }
          assertEquals(expected.contype, resultSet.getString("contype").charAt(0));
          assertEquals(expected.conrelid, resultSet.getString("conrelid"));
          assertEquals(expected.conindid, resultSet.getString("conindid"));
          assertEquals(expected.confrelid, resultSet.getString("confrelid"));
          assertEquals(expected.confupdtype, resultSet.getString("confupdtype"));
          assertEquals(expected.confdeltype, resultSet.getString("confdeltype"));
          assertEquals(expected.confmatchtype, resultSet.getString("confmatchtype"));
          assertArrayEquals(expected.conkey, (Long[]) resultSet.getArray("conkey").getArray());
          assertArrayEquals(
              expected.confkey,
              resultSet.getArray("confkey") == null
                  ? null
                  : (Long[]) resultSet.getArray("confkey").getArray());
          assertEquals(expected.conbin, resultSet.getString("conbin"));
          index++;
        }
        assertEquals(expectedRows.size(), index);
      }
    }
  }
}
