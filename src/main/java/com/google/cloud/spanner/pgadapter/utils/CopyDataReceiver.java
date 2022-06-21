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

package com.google.cloud.spanner.pgadapter.utils;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement.ResultNotReadyBehavior;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyInResponse;
import java.util.concurrent.Callable;

@InternalApi
public class CopyDataReceiver implements Callable<Void> {
  private final CopyStatement copyStatement;
  private final ConnectionHandler connectionHandler;

  public CopyDataReceiver(CopyStatement copyStatement, ConnectionHandler connectionHandler) {
    this.copyStatement = copyStatement;
    this.connectionHandler = connectionHandler;
  }

  @Override
  public Void call() throws Exception {
    handleCopy();
    return null;
  }

  /**
   * Sends a {@link CopyInResponse} to the client and then waits for copy data, done and fail
   * messages. The incoming data messages are fed into the {@link MutationWriter} that is associated
   * with this {@link CopyStatement}. The method blocks until it sees a {@link
   * com.google.cloud.spanner.pgadapter.wireprotocol.CopyDoneMessage} or {@link
   * com.google.cloud.spanner.pgadapter.wireprotocol.CopyFailMessage}, or until the {@link
   * MutationWriter} changes the status of the connection to a non-COPY_IN status, for example as a
   * result of an error while copying the data to Cloud Spanner.
   */
  private void handleCopy() throws Exception {
    if (copyStatement.hasException()) {
      throw copyStatement.getException();
    } else {
      this.connectionHandler.addActiveStatement(copyStatement);
      new CopyInResponse(
              this.connectionHandler.getConnectionMetadata().getOutputStream(),
              copyStatement.getTableColumns().size(),
              copyStatement.getFormatCode())
          .send();
      ConnectionStatus initialConnectionStatus = this.connectionHandler.getStatus();
      try {
        this.connectionHandler.setStatus(ConnectionStatus.COPY_IN);
        // Loop here until COPY_IN mode has finished.
        while (this.connectionHandler.getStatus() == ConnectionStatus.COPY_IN) {
          this.connectionHandler.handleMessages();
        }
        if (copyStatement.hasException(ResultNotReadyBehavior.BLOCK)
            || this.connectionHandler.getStatus() == ConnectionStatus.COPY_FAILED) {
          if (copyStatement.hasException(ResultNotReadyBehavior.BLOCK)) {
            throw copyStatement.getException();
          } else {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INTERNAL, "Copy failed with unknown reason");
          }
        }
      } finally {
        this.connectionHandler.removeActiveStatement(copyStatement);
        this.copyStatement.close();
        this.connectionHandler.setStatus(initialConnectionStatus);
      }
    }
  }
}
