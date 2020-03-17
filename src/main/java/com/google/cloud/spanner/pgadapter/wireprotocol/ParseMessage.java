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

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.PGWireProtocol;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.utils.StatementParser;
import com.google.common.base.Strings;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates a prepared statement.
 */
public class ParseMessage extends WireMessage {

  private String name;
  private IntermediatePreparedStatement statement;
  private List<Integer> parameterDataTypes;

  public ParseMessage(ConnectionHandler connection, DataInputStream input) throws Exception {
    super(connection, input);
    this.name = PGWireProtocol.readString(input);
    String queryString = StatementParser.removeCommentsAndTrim(PGWireProtocol.readString(input));
    this.parameterDataTypes = new ArrayList<>();
    short numberOfParameters = input.readShort();
    for (int i = 0; i < numberOfParameters; i++) {
      this.parameterDataTypes.add(input.readInt());
    }
    this.statement = new IntermediatePreparedStatement(
        queryString,
        connection.getJdbcConnection());
    this.statement.setParameterDataTypes(this.parameterDataTypes);
  }

  @Override
  public void send() throws Exception {
    if (!Strings.isNullOrEmpty(this.name) && this.connection.hasStatement(this.name)) {
      throw new IllegalStateException("Must close statement before reusing name.");
    }
    this.connection.registerStatement(this.name, this.statement);
    this.connection.handleParse();
  }

  public IntermediatePreparedStatement getStatement() {
    return this.statement;
  }

  public String getName() {
    return this.name;
  }
}
