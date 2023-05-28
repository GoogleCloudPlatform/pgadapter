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
import com.google.cloud.spanner.pgadapter.utils.Converter;
import com.google.cloud.spanner.pgadapter.wireoutput.WireOutput;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.ManuallyCreatedToken;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;

/**
 * MOVE is the same as FETCH, except it just skips the results instead of actually sending the rows
 * back to the client.
 */
public class MoveStatement extends AbstractFetchOrMoveStatement {

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
    return "MOVE";
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    if (!this.executed) {
      try {
        new ExecuteMessage(
                connectionHandler,
                fetchOrMoveStatement.name,
                fetchOrMoveStatement.count == null ? 1 : fetchOrMoveStatement.count,
                "MOVE",
                false,
                ManuallyCreatedToken.MANUALLY_CREATED_TOKEN)
            .send();
        // Set a null result to indicate that this statement should not return any result.
        setFutureStatementResult(Futures.immediateFuture(null));
      } catch (Exception exception) {
        setFutureStatementResult(Futures.immediateFailedFuture(exception));
      }
    }
  }

  @Override
  public WireOutput createDataRowResponse(Converter converter) {
    // MOVE should not return any data, just skip it.
    return null;
  }

  static class ParsedMoveStatement extends ParsedFetchOrMoveStatement {
    ParsedMoveStatement(String name, Direction direction, Integer count) {
      super(name, direction, count);
    }
  }
}
