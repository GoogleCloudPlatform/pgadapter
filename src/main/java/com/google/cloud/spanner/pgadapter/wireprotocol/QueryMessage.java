// Copyright 2020 Google LLC
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

package com.google.cloud.spanner.pgadapter.wireprotocol;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.statements.SimpleQueryStatement;
import com.google.common.collect.ImmutableList;
import java.text.MessageFormat;

/** Executes a simple statement. */
@InternalApi
public class QueryMessage extends ControlMessage {
  protected static final char IDENTIFIER = 'Q';
  public static final String COPY = "COPY";
  public static final String PREPARE = "PREPARE";
  public static final String EXECUTE = "EXECUTE";
  public static final String DEALLOCATE = "DEALLOCATE";
  public static final String VACUUM = "VACUUM";
  public static final String TRUNCATE = "TRUNCATE";
  public static final String SAVEPOINT = "SAVEPOINT";
  public static final String RELEASE = "RELEASE";
  public static final String ROLLBACK = "ROLLBACK";
  public static final String DECLARE = "DECLARE";
  public static final String FETCH = "FETCH";
  public static final String MOVE = "MOVE";
  public static final String CLOSE = "CLOSE";
  public static final ImmutableList<String> SHOW_DATABASE_DDL =
      ImmutableList.of("SHOW", "DATABASE", "DDL");
  private final Statement originalStatement;
  private final SimpleQueryStatement simpleQueryStatement;

  public QueryMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    connection.getExtendedQueryProtocolHandler().maybeStartSpan(true);
    this.originalStatement = Statement.of(this.readAll());
    connection.maybeDetermineWellKnownClient(this.originalStatement);
    this.simpleQueryStatement =
        new SimpleQueryStatement(
            connection.getServer().getOptions(), this.originalStatement, this.connection);
  }

  @Override
  protected void sendPayload() throws Exception {
    this.simpleQueryStatement.execute();
  }

  @Override
  protected String getMessageName() {
    return "Query";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, SQL: {1}")
        .format(new Object[] {this.length, this.originalStatement.getSql()});
  }

  @Override
  public String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public Statement getStatement() {
    return this.originalStatement;
  }

  public SimpleQueryStatement getSimpleQueryStatement() {
    return this.simpleQueryStatement;
  }
}
