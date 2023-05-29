// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.statements;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.ManuallyCreatedToken;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.common.collect.ImmutableList;

/**
 * MOVE is the same as FETCH, except it just skips the results instead of actually sending the rows
 * back to the client.
 */
public class MoveStatement extends AbstractFetchOrMoveStatement {
  static final String MOVE_COMMAND_TAG = "MOVE";

  public MoveStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(
        name,
        new IntermediatePreparedStatement(
            connectionHandler,
            options,
            name,
            NO_PARAMETER_TYPES,
            parsedStatement,
            originalStatement),
        NO_PARAMS,
        ImmutableList.of(),
        ImmutableList.of(),
        parse(originalStatement.getSql(), "move", ParsedMoveStatement.class));
  }

  @Override
  public String getCommandTag() {
    return MOVE_COMMAND_TAG;
  }

  @Override
  protected void execute(BackendConnection backendConnection, int maxRows) throws Exception {
    new ExecuteMessage(
            connectionHandler,
            fetchOrMoveStatement.name,
            maxRows,
            MOVE_COMMAND_TAG,
            false,
            ManuallyCreatedToken.MANUALLY_CREATED_TOKEN)
        .send();
  }

  static class ParsedMoveStatement extends ParsedFetchOrMoveStatement {
    ParsedMoveStatement(String name, Direction direction, Integer count) {
      super(name, direction, count);
    }
  }
}
