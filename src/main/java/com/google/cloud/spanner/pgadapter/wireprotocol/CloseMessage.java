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
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.statements.InvalidStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.CloseCompleteResponse;
import java.text.MessageFormat;
import javax.annotation.Nullable;

/** Close the designated statement. */
@InternalApi
public class CloseMessage extends AbstractQueryProtocolMessage {

  protected static final char IDENTIFIER = 'C';

  private final PreparedType type;
  private final String name;
  @Nullable private final IntermediateStatement statement;

  public CloseMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.type = PreparedType.prepareType((char) this.inputStream.readUnsignedByte());
    this.name = this.readAll();
    IntermediateStatement statement = null;
    if (this.type == PreparedType.Statement) {
      try {
        statement = this.connection.getStatement(this.name);
      } catch (PGException ignore) {
      }
    } else {
      try {
        statement = this.connection.getPortal(this.name);
      } catch (PGException ignore) {
      }
    }
    this.statement = statement;
  }

  @Override
  void buffer(BackendConnection backendConnection) throws Exception {
    // Unregister the statement or portal immediately from the connection map so that subsequent
    // messages in the same pipeline (e.g. a Parse or Bind reusing the same name) will see the name
    // available, or attempts to use this closed statement will fail.
    // Resource cleanup (statement.close()) is deferred until flush() so that:
    // 1. If an earlier message in the pipeline fails, abort() can restore the statement.
    // 2. Preceding messages in the pipeline (e.g. Execute) can finish streaming rows before
    // closure.
    if (this.statement != null) {
      if (this.type == PreparedType.Portal) {
        this.connection.unregisterPortal(this.name);
      } else {
        this.connection.unregisterStatement(this.name);
      }
    }
  }

  @Override
  public void flush() throws Exception {
    try {
      if (this.statement != null) {
        this.statement.close();
      }
      CloseCompleteResponse.send(this.outputStream);
    } catch (Exception exception) {
      handleError(exception);
    }
  }

  @Override
  public void abort() {
    // Only restore prepared statements that did not fail during creation (an InvalidStatement
    // created by a failed Parse message must not be resurrected).
    // Portals are never restored: in PostgreSQL, any error in a pipeline drops all portals.
    if (this.type == PreparedType.Statement
        && this.statement instanceof IntermediatePreparedStatement
        && !(this.statement instanceof InvalidStatement)) {
      this.connection.registerStatement(this.name, (IntermediatePreparedStatement) this.statement);
    }
  }

  @Override
  public String getSql() {
    return this.statement == null ? "" : this.statement.getSql();
  }

  @Override
  protected String getMessageName() {
    return "Close";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Name: {1}, " + "Type: {2}")
        .format(new Object[] {this.length, this.name, this.type.toString()});
  }

  @Override
  public String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public String getName() {
    return this.name;
  }

  public PreparedType getType() {
    return this.type;
  }

  @Override
  protected int getHeaderLength() {
    return 5;
  }
}
