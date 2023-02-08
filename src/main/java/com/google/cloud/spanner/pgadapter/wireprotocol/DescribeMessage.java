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
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.DescribeResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.NoDataResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ParameterDescriptionResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.RowDescriptionResponse;
import com.google.common.annotations.VisibleForTesting;
import java.text.MessageFormat;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** Calls describe on a portal or prepared statement. */
@InternalApi
public class DescribeMessage extends AbstractQueryProtocolMessage {

  protected static final char IDENTIFIER = 'D';

  private final PreparedType type;
  private final String name;
  private final IntermediateStatement statement;
  private Future<StatementResult> describePortalMetadata;

  public DescribeMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.type = PreparedType.prepareType((char) this.inputStream.readUnsignedByte());
    this.name = this.readAll();
    if (this.type == PreparedType.Portal) {
      this.statement = this.connection.getPortal(this.name);
    } else {
      this.statement = this.connection.getStatement(this.name);
    }
  }

  /** Constructor for manually created Describe messages from the simple query protocol. */
  public DescribeMessage(ConnectionHandler connection, ManuallyCreatedToken manuallyCreatedToken) {
    this(connection, PreparedType.Portal, "", manuallyCreatedToken);
  }

  /** Constructor for manually created Describe messages from PREPARE statements. */
  public DescribeMessage(
      ConnectionHandler connection,
      PreparedType type,
      String name,
      ManuallyCreatedToken manuallyCreatedToken) {
    super(connection, 4, manuallyCreatedToken);
    this.type = type;
    this.name = name;
    if (this.type == PreparedType.Portal) {
      this.statement = this.connection.getPortal(this.name);
    } else {
      this.statement = this.connection.getStatement(this.name);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  void buffer(BackendConnection backendConnection) {
    if (this.type == PreparedType.Portal && this.statement.containsResultSet()) {
      describePortalMetadata = this.statement.describeAsync(backendConnection);
    } else if (this.type == PreparedType.Statement) {
      describePortalMetadata = this.statement.describeAsync(backendConnection);
    }
  }

  @Override
  public void flush() throws Exception {
    try {
      if (this.type == PreparedType.Portal) {
        this.handleDescribePortal();
      } else {
        this.handleDescribeStatement();
      }
    } catch (Exception e) {
      handleError(e);
    }
  }

  @Override
  protected String getMessageName() {
    return "Describe";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Type: {1}, " + "Name: {2}")
        .format(new Object[] {this.length, this.type.toString(), this.name});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public String getName() {
    return this.name;
  }

  @Override
  public String getSql() {
    return this.statement.getSql();
  }

  public PreparedType getType() {
    return type;
  }

  @Override
  protected int getHeaderLength() {
    return 5;
  }

  /**
   * Called when a describe message of type 'P' is received.
   *
   * @throws Exception if sending the message back to the client causes an error.
   */
  @VisibleForTesting
  void handleDescribePortal() throws Exception {
    if (this.statement.hasException()) {
      throw this.statement.getException();
    } else {
      if (this.statement.containsResultSet()) {
        // COPY TO STDOUT statements do not contain any metadata.
        if (getPortalMetadata() != null) {
          try {
            new RowDescriptionResponse(
                    this.outputStream,
                    this.statement,
                    getPortalMetadata().getResultSet().getType(),
                    this.connection.getServer().getOptions(),
                    this.queryMode)
                .send(false);
          } catch (Exception exception) {
            this.handleError(exception);
          }
        }
      } else {
        // The simple query protocol does not expect a NoData response in case of a non-query
        // statement.
        if (isExtendedProtocol()) {
          new NoDataResponse(this.outputStream).send(false);
        }
      }
    }
  }

  @VisibleForTesting
  StatementResult getPortalMetadata() {
    if (!this.describePortalMetadata.isDone()) {
      throw new IllegalStateException("Trying to get Portal Metadata before it has been described");
    }
    try {
      return this.describePortalMetadata.get();
    } catch (ExecutionException executionException) {
      throw SpannerExceptionFactory.asSpannerException(executionException.getCause());
    } catch (InterruptedException interruptedException) {
      throw PGExceptionFactory.newQueryCancelledException();
    }
  }

  /**
   * Called when a describe message of type 'S' is received.
   *
   * @throws Exception if sending the message back to the client causes an error.
   */
  public void handleDescribeStatement() throws Exception {
    if (this.statement.hasException()) {
      throw this.statement.getException();
    } else {
      if (isExtendedProtocol()) {
        DescribeResult metadata = this.statement.describe();
        new ParameterDescriptionResponse(this.outputStream, metadata.getParameters()).send(false);
        if (metadata.getColumns() != null && metadata.getColumns().getStructFields().size() > 0) {
          new RowDescriptionResponse(
                  this.outputStream,
                  this.statement,
                  metadata.getColumns(),
                  this.connection.getServer().getOptions(),
                  this.queryMode)
              .send(false);
        } else {
          new NoDataResponse(this.outputStream).send(false);
        }
      }
    }
  }
}
