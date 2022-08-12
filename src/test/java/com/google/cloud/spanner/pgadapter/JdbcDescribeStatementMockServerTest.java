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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.jdbc.PgStatement;

@RunWith(JUnit4.class)
public class JdbcDescribeStatementMockServerTest extends AbstractMockServerTest {
  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(null, Collections.emptyList());
  }

  private String createConnectionUrl() {
    return String.format("jdbc:postgresql://localhost:%d/my-db", pgServer.getLocalPort());
  }

  @Test
  public void testSelectWithParameters() throws SQLException {
    String sql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar "
            + "from all_types "
            + "where col_bigint=? "
            + "and col_bool=? "
            + "and col_bytea=? "
            + "and col_float8=? "
            + "and col_int=? "
            + "and col_numeric=? "
            + "and col_timestamptz=? "
            + "and col_date=? "
            + "and col_varchar=?";
    String pgSql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_int, "
            + "col_numeric, col_timestamptz, col_date, col_varchar "
            + "from all_types "
            + "where col_bigint=$1 and col_bool=$2 and col_bytea=$3 and col_float8=$4 "
            + "and col_int=$5 and col_numeric=$6 and col_timestamptz=$7 and col_date=$8 and col_varchar=$9";
    // mockSpanner.putStatementResult();

    try (Connection connection = DriverManager.getConnection(createConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        // This forces the PG JDBC driver to use binary transfer mode for the results, and will
        // also cause it to send a DescribeStatement message.
        statement.unwrap(PgStatement.class).setPrepareThreshold(-1);

        int index = 0;
        statement.setLong(++index, 1);
        statement.setBoolean(++index, true);
        statement.setBytes(++index, "test".getBytes(StandardCharsets.UTF_8));
        statement.setDouble(++index, 3.14d);
        statement.setInt(++index, 1);
        statement.setBigDecimal(++index, new BigDecimal("3.14"));
        statement.setTimestamp(
            ++index, Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00").toSqlTimestamp());
        statement.setDate(++index, Date.valueOf("2022-04-29"));
        statement.setString(++index, "test");

        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());

          index = 0;
          assertEquals(1, resultSet.getLong(++index));
          assertTrue(resultSet.getBoolean(++index));
          assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(++index));
          assertEquals(3.14d, resultSet.getDouble(++index), 0.0d);
          assertEquals(1, resultSet.getInt(++index));
          assertEquals(new BigDecimal("3.14"), resultSet.getBigDecimal(++index));
          assertEquals(
              Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00").toSqlTimestamp(),
              resultSet.getTimestamp(++index));
          assertEquals(Date.valueOf("2022-04-29"), resultSet.getDate(++index));
          assertEquals("test", resultSet.getString(++index));

          assertFalse(resultSet.next());
        }
      }
    }
  }
}
