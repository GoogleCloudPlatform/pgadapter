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
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.PreparedType;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;

public class FetchStatement extends AbstractFetchOrMoveStatement {

  public FetchStatement(
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
        parse(originalStatement.getSql(), "fetch", ParsedFetchStatement.class));
  }

  @Override
  public String getCommandTag() {
    return "FETCH";
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    if (!this.executed) {
      try {
        new DescribeMessage(
                connectionHandler,
                PreparedType.Portal,
                fetchOrMoveStatement.name,
                ManuallyCreatedToken.MANUALLY_CREATED_TOKEN)
            .send();
        new ExecuteMessage(
                connectionHandler,
                fetchOrMoveStatement.name,
                fetchOrMoveStatement.count == null ? 1 : fetchOrMoveStatement.count,
                "FETCH",
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

  static class ParsedFetchStatement extends ParsedFetchOrMoveStatement {
    ParsedFetchStatement(String name, Direction direction, Integer count) {
      super(name, direction, count);
    }
  }
}
