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

package com.google.cloud.spanner.pgadapter.statements.local;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptionsHelper;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@InternalApi
public class ListDatabasesStatement implements LocalStatement {
  public static final String LIST_DATABASES_SQL = "select reachable databases";
  private final ConnectionHandler connectionHandler;

  public ListDatabasesStatement(ConnectionHandler connectionHandler) {
    this.connectionHandler = Preconditions.checkNotNull(connectionHandler);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ListDatabasesStatement)) {
      return false;
    }
    return Objects.equals(
        this.connectionHandler, ((ListDatabasesStatement) other).connectionHandler);
  }

  @Override
  public int hashCode() {
    return this.connectionHandler.hashCode();
  }

  @Override
  public String[] getSql() {
    return new String[] {LIST_DATABASES_SQL};
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    Connection connection = backendConnection.getSpannerConnection();
    Spanner spanner = ConnectionOptionsHelper.getSpanner(connection);
    InstanceId defaultInstanceId =
        connectionHandler.getServer().getOptions().getDefaultInstanceId();
    ResultSet resultSet =
        ResultSets.forRows(
            Type.struct(
                StructField.of("Name", Type.string()),
                StructField.of("Owner", Type.string()),
                StructField.of("Encoding", Type.string()),
                StructField.of("Collate", Type.string()),
                StructField.of("Ctype", Type.string()),
                StructField.of("Access privileges", Type.string())),
            listDatabases(spanner).stream()
                .map(
                    database ->
                        Struct.newBuilder()
                            .set("Name")
                            .to(
                                database.getId().getInstanceId().equals(defaultInstanceId)
                                    ? database.getId().getDatabase()
                                    : database.getId().toString())
                            .set("Owner")
                            .to("")
                            .set("Encoding")
                            .to("UTF8")
                            .set("Collate")
                            .to("")
                            .set("Ctype")
                            .to("")
                            .set("Access privileges")
                            .to("")
                            .build())
                .sorted(Comparator.comparing(row -> row.getString("Name")))
                .collect(Collectors.toList()));
    return new QueryResult(resultSet);
  }

  /**
   * Returns the databases that are:
   *
   * <ol>
   *   <li>The same as the current database
   *   <li>On the default instance of this instance of PGAdapter
   *   <li>On the same instance as the current connection
   * </ol>
   */
  private List<Database> listDatabases(Spanner spanner) {
    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();
    if (connectionHandler.getServer().getOptions().hasDefaultConnectionUrl()) {
      // Only return the current database, as that is the only one that is reachable.
      return ImmutableList.of(
          databaseAdminClient
              .newDatabaseBuilder(connectionHandler.getServer().getOptions().getDefaultDatabaseId())
              .setDialect(Dialect.POSTGRESQL)
              .build());
    }
    // Return all the databases on the same instance as the database that we are currently connected
    // to.
    return StreamSupport.stream(
            databaseAdminClient
                .listDatabases(connectionHandler.getDatabaseId().getInstanceId().getInstance())
                .iterateAll()
                .spliterator(),
            false)
        .filter(database -> database.getDialect() == Dialect.POSTGRESQL)
        .collect(Collectors.toList());
  }
}
