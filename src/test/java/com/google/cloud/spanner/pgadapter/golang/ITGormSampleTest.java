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

package com.google.cloud.spanner.pgadapter.golang;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITGormSampleTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static GormSampleTest gormSampleTest;

  @BeforeClass
  public static void setup() throws Exception {
    try {
      gormSampleTest =
          GolangTest.compile("../../../samples/golang/gorm/sample.go", GormSampleTest.class);
    } catch (UnsatisfiedLinkError unsatisfiedLinkError) {
      // This probably means that there is a version mismatch for GLIBC (or no GLIBC at all
      // installed).
      assumeFalse(
          "Skipping ecosystem test because of missing dependency",
          System.getProperty("allowSkipUnsupportedEcosystemTest", "false")
              .equalsIgnoreCase("true"));
      throw unsatisfiedLinkError;
    }

    testEnv.setUp();
    Database database = testEnv.createDatabase(ImmutableList.of());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  private GoString createConnString() {
    return new GoString(String.format("host=/tmp port=%d", testEnv.getServer().getLocalPort()));
  }

  @Test
  public void testGormSample() throws Exception {
    assertNull(
        gormSampleTest.TestRunSample(
            createConnString(),
            new GoString(new java.io.File(".").getCanonicalPath() + "/samples/golang/gorm")));

    // Verify that the data model that was created is as expected.
    String jdbcUrl =
        String.format(
            "jdbc:postgresql://localhost/%s?"
                + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
                + "&socketFactoryArg=/tmp/.s.PGSQL.%d",
            testEnv.getDatabaseId(), testEnv.getPGAdapterPort());
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
      try (ResultSet resultSet = connection.createStatement().executeQuery("show database ddl")) {
        StringBuilder builder = new StringBuilder();
        while (resultSet.next()) {
          builder.append(resultSet.getString(1)).append("\n");
        }
        assertEquals(
            "CREATE TABLE albums (\n"
                + "  id character varying NOT NULL,\n"
                + "  title character varying NOT NULL,\n"
                + "  marketing_budget numeric,\n"
                + "  release_date date,\n"
                + "  cover_picture bytea,\n"
                + "  singer_id character varying NOT NULL,\n"
                + "  created_at timestamp with time zone,\n"
                + "  updated_at timestamp with time zone,\n"
                + "  PRIMARY KEY(id)\n"
                + ");\n"
                + "CREATE TABLE tracks (\n"
                + "  id character varying NOT NULL,\n"
                + "  track_number bigint NOT NULL,\n"
                + "  title character varying NOT NULL,\n"
                + "  sample_rate double precision NOT NULL,\n"
                + "  created_at timestamp with time zone,\n"
                + "  updated_at timestamp with time zone,\n"
                + "  PRIMARY KEY(id, track_number)\n"
                + ") INTERLEAVE IN PARENT albums ON DELETE CASCADE;\n"
                + "CREATE TABLE concerts (\n"
                + "  id character varying NOT NULL,\n"
                + "  venue_id character varying NOT NULL,\n"
                + "  singer_id character varying NOT NULL,\n"
                + "  name character varying NOT NULL,\n"
                + "  start_time timestamp with time zone NOT NULL,\n"
                + "  end_time timestamp with time zone NOT NULL,\n"
                + "  created_at timestamp with time zone,\n"
                + "  updated_at timestamp with time zone,\n"
                + "  PRIMARY KEY(id),\n"
                + "  CONSTRAINT chk_end_time_after_start_time CHECK((end_time > start_time))\n"
                + ");\n"
                + "CREATE TABLE singers (\n"
                + "  id character varying NOT NULL,\n"
                + "  first_name character varying,\n"
                + "  last_name character varying NOT NULL,\n"
                + "  full_name character varying(300) GENERATED ALWAYS AS (CASE WHEN (first_name IS NULL) THEN last_name WHEN (last_name IS NULL) THEN first_name ELSE ((first_name || ' '::text) || last_name) END) STORED,\n"
                + "  active boolean,\n"
                + "  created_at timestamp with time zone,\n"
                + "  updated_at timestamp with time zone,\n"
                + "  PRIMARY KEY(id)\n"
                + ");\n"
                + "ALTER TABLE albums ADD CONSTRAINT fk_albums_singers FOREIGN KEY (singer_id) REFERENCES singers(id);\n"
                + "ALTER TABLE concerts ADD CONSTRAINT fk_concerts_singers FOREIGN KEY (singer_id) REFERENCES singers(id);\n"
                + "CREATE TABLE venues (\n"
                + "  id character varying NOT NULL,\n"
                + "  name character varying NOT NULL,\n"
                + "  description character varying NOT NULL,\n"
                + "  created_at timestamp with time zone,\n"
                + "  updated_at timestamp with time zone,\n"
                + "  PRIMARY KEY(id)\n"
                + ");\n"
                + "ALTER TABLE concerts ADD CONSTRAINT fk_concerts_venues FOREIGN KEY (venue_id) REFERENCES venues(id);\n",
            builder.toString());
      }

      // This should comment out any Cloud Spanner specific statements like interleaved tables.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show database ddl for postgresql")) {
        StringBuilder builder = new StringBuilder();
        while (resultSet.next()) {
          builder.append(resultSet.getString(1)).append("\n");
        }
        assertEquals(
            "CREATE TABLE albums (\n"
                + "  id character varying NOT NULL,\n"
                + "  title character varying NOT NULL,\n"
                + "  marketing_budget numeric,\n"
                + "  release_date date,\n"
                + "  cover_picture bytea,\n"
                + "  singer_id character varying NOT NULL,\n"
                + "  created_at timestamp with time zone,\n"
                + "  updated_at timestamp with time zone,\n"
                + "  PRIMARY KEY(id)\n"
                + ");\n"
                + "CREATE TABLE tracks (\n"
                + "  id character varying NOT NULL,\n"
                + "  track_number bigint NOT NULL,\n"
                + "  title character varying NOT NULL,\n"
                + "  sample_rate double precision NOT NULL,\n"
                + "  created_at timestamp with time zone,\n"
                + "  updated_at timestamp with time zone,\n"
                + "  PRIMARY KEY(id, track_number)\n"
                + ") /* INTERLEAVE IN PARENT albums ON DELETE CASCADE */;\n"
                + "CREATE TABLE concerts (\n"
                + "  id character varying NOT NULL,\n"
                + "  venue_id character varying NOT NULL,\n"
                + "  singer_id character varying NOT NULL,\n"
                + "  name character varying NOT NULL,\n"
                + "  start_time timestamp with time zone NOT NULL,\n"
                + "  end_time timestamp with time zone NOT NULL,\n"
                + "  created_at timestamp with time zone,\n"
                + "  updated_at timestamp with time zone,\n"
                + "  PRIMARY KEY(id),\n"
                + "  CONSTRAINT chk_end_time_after_start_time CHECK((end_time > start_time))\n"
                + ");\n"
                + "CREATE TABLE singers (\n"
                + "  id character varying NOT NULL,\n"
                + "  first_name character varying,\n"
                + "  last_name character varying NOT NULL,\n"
                + "  full_name character varying(300) GENERATED ALWAYS AS (CASE WHEN (first_name IS NULL) THEN last_name WHEN (last_name IS NULL) THEN first_name ELSE ((first_name || ' '::text) || last_name) END) STORED,\n"
                + "  active boolean,\n"
                + "  created_at timestamp with time zone,\n"
                + "  updated_at timestamp with time zone,\n"
                + "  PRIMARY KEY(id)\n"
                + ");\n"
                + "ALTER TABLE albums ADD CONSTRAINT fk_albums_singers FOREIGN KEY (singer_id) REFERENCES singers(id);\n"
                + "ALTER TABLE concerts ADD CONSTRAINT fk_concerts_singers FOREIGN KEY (singer_id) REFERENCES singers(id);\n"
                + "CREATE TABLE venues (\n"
                + "  id character varying NOT NULL,\n"
                + "  name character varying NOT NULL,\n"
                + "  description character varying NOT NULL,\n"
                + "  created_at timestamp with time zone,\n"
                + "  updated_at timestamp with time zone,\n"
                + "  PRIMARY KEY(id)\n"
                + ");\n"
                + "ALTER TABLE concerts ADD CONSTRAINT fk_concerts_venues FOREIGN KEY (venue_id) REFERENCES venues(id);\n",
            builder.toString());
      }
    }
  }
}
