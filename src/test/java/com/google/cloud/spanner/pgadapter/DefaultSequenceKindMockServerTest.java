// Copyright 2025 Google LLC
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

import com.google.protobuf.AbstractMessage;
import com.google.rpc.Code;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DefaultSequenceKindMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  protected String createUrl() {
    return String.format("jdbc:postgresql://localhost:%d/d", pgServer.getLocalPort());
  }

  @Test
  public void testRetryWithDefaultSequenceKind() throws SQLException {
    addDdlErrorResponse(
        com.google.rpc.Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage(
                "The sequence kind of an identity column id is not specified. "
                    + "Please specify the sequence kind explicitly or set the database option `default_sequence_kind`.")
            .build());
    addDdlResponseToSpannerAdmin();
    addDdlResponseToSpannerAdmin();

    String sql = "create table test (id serial primary key, value varchar)";
    try (Connection connection = DriverManager.getConnection(createUrl());
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
    List<AbstractMessage> requests = mockDatabaseAdmin.getRequests();
    assertEquals(3, requests.size());
    assertEquals(sql, ((UpdateDatabaseDdlRequest) requests.get(0)).getStatements(0));
    assertEquals(
        "alter database \"d\" set spanner.default_sequence_kind = 'bit_reversed_positive'",
        ((UpdateDatabaseDdlRequest) requests.get(1)).getStatements(0));
    assertEquals(sql, ((UpdateDatabaseDdlRequest) requests.get(2)).getStatements(0));
  }

  @Test
  public void testRetryBatchWithDefaultSequenceKind() throws SQLException {
    addDdlErrorResponse(
        com.google.rpc.Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage(
                "The sequence kind of an identity column id is not specified. "
                    + "Please specify the sequence kind explicitly or set the database option `default_sequence_kind`.")
            .build());
    addDdlResponseToSpannerAdmin();
    addDdlResponseToSpannerAdmin();

    String sql1 = "create table test1 (id serial primary key, value varchar)";
    String sql2 = "create table test2 (id serial primary key, value varchar)";
    try (Connection connection = DriverManager.getConnection(createUrl());
        Statement statement = connection.createStatement()) {
      statement.addBatch(sql1);
      statement.addBatch(sql2);
      statement.executeBatch();
    }
    List<AbstractMessage> requests = mockDatabaseAdmin.getRequests();
    assertEquals(3, requests.size());
    assertEquals(sql1, ((UpdateDatabaseDdlRequest) requests.get(0)).getStatements(0));
    assertEquals(sql2, ((UpdateDatabaseDdlRequest) requests.get(0)).getStatements(1));
    assertEquals(
        "alter database \"d\" set spanner.default_sequence_kind = 'bit_reversed_positive'",
        ((UpdateDatabaseDdlRequest) requests.get(1)).getStatements(0));
    assertEquals(sql1, ((UpdateDatabaseDdlRequest) requests.get(2)).getStatements(0));
    assertEquals(sql2, ((UpdateDatabaseDdlRequest) requests.get(2)).getStatements(1));
  }
}
