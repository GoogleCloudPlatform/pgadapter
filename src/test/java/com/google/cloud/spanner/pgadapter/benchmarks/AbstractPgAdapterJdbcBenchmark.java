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

package com.google.cloud.spanner.pgadapter.benchmarks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.openjdk.jmh.annotations.Benchmark;

public abstract class AbstractPgAdapterJdbcBenchmark extends AbstractBenchmark {
  static {
    // Make sure the PG JDBC driver is loaded.
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      // ignore
    }
  }

  protected abstract String createUrl(PgAdapterBenchmarkState server);

  @Benchmark
  public void testSelect1(PgAdapterBenchmarkState server) throws SQLException {
    String sql = "SELECT 1";

    //        try (Connection connection = DriverManager.getConnection(createUrl(server))) {
    try (Connection connection = server.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Benchmark
  public void testSelectOneRow(PgAdapterBenchmarkState server) throws SQLException {
    String sql = "select * from all_types limit 1";

    //    try (Connection connection = DriverManager.getConnection(createUrl(server))) {
    try (Connection connection = server.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            resultSet.getObject(i);
          }
          assertFalse(resultSet.next());
        }
      }
    }
  }
}
