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

import static com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement.NO_FORMAT_CODES;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.InvalidStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.BindCompleteResponse;
import com.google.common.base.Preconditions;
import java.text.MessageFormat;
import java.util.Arrays;

/**
 * Message of type bind (meaning that it is a message called to a prepared statement to complete it
 * with missing data to prepare for execution). A bound prepared statement always yields a portal
 * unless it fails.
 */
@InternalApi
public class BindMessage extends AbstractQueryProtocolMessage {
  protected static final char IDENTIFIER = 'B';
  private static final String MESSAGE_NAME = "Bind";
  // 4-byte length + 2 null terminators (portal, statement) + 3 x 2-byte format/parameter counts.
  private static final int MIN_MESSAGE_LENGTH = 12;
  private static final String RECEIVED_EVENT_DESCRIPTION = "Received message: '" + IDENTIFIER + "'";

  private final String portalName;
  private final String statementName;
  private final short[] formatCodes;
  private final short[] resultFormatCodes;
  private final byte[][] parameters;
  private final IntermediatePortalStatement statement;
  private final boolean hasBindException;

  /** Constructor for Bind messages that are received from the front-end. */
  public BindMessage(ConnectionHandler connection) throws Exception {
    super(connection, MIN_MESSAGE_LENGTH, MESSAGE_NAME);
    this.portalName = readBoundedString(7, "bind message");
    this.statementName = readBoundedString(6, "bind message");
    this.formatCodes = readFormatCodes(4);
    this.parameters = readParameters(2);
    this.resultFormatCodes = readFormatCodes(0);
    checkRemainingBytes(0, "bind message");
    this.statement = createPortalStatement(connection);
    this.hasBindException = this.statement instanceof InvalidStatement;
    this.connection.registerPortal(this.portalName, this.statement);
  }

  private IntermediatePortalStatement createPortalStatement(ConnectionHandler connection) {
    try {
      IntermediatePreparedStatement preparedStatement = connection.getStatement(this.statementName);
      return preparedStatement.createPortal(
          this.portalName, this.parameters, this.formatCodes, this.resultFormatCodes);
    } catch (Exception exception) {
      return new InvalidStatement(
          connection, connection.getServer().getOptions(), this.portalName, exception);
    }
  }

  /** Constructor for Bind messages that are constructed to execute a Query message. */
  public BindMessage(ConnectionHandler connection, ManuallyCreatedToken manuallyCreatedToken) {
    this(connection, "", "", new byte[0][], manuallyCreatedToken);
  }

  /** Constructor for Bind messages that are created by EXECUTE statements. */
  public BindMessage(
      ConnectionHandler connection,
      String statementName,
      String portalName,
      byte[][] parameters,
      ManuallyCreatedToken manuallyCreatedToken) {
    super(connection, 4, manuallyCreatedToken);
    this.portalName = portalName;
    this.statementName = statementName;
    this.formatCodes = NO_FORMAT_CODES;
    this.resultFormatCodes = NO_FORMAT_CODES;
    this.parameters = Preconditions.checkNotNull(parameters);
    this.hasBindException = false;
    IntermediatePreparedStatement statement = connection.getStatement(statementName);
    this.statement =
        statement.createPortal(
            this.portalName, this.parameters, this.formatCodes, this.resultFormatCodes);
    this.connection.registerPortal(this.portalName, this.statement);
  }

  boolean hasParameterValues() {
    return this.parameters.length > 0;
  }

  @Override
  void buffer(BackendConnection backendConnection) {
    if (this.hasBindException && this.statement instanceof InvalidStatement) {
      backendConnection.execute((InvalidStatement) this.statement);
      return;
    }
    if (isExtendedProtocol() && !this.statement.getPreparedStatement().isDescribed()) {
      try {
        // Make sure all parameters have been described, so we always send typed parameters to Cloud
        // Spanner.
        this.statement
            .getPreparedStatement()
            .autoDescribeParameters(this.parameters, backendConnection);
      } catch (Throwable ignore) {
        // Ignore any error messages while describing the parameters, and let the following
        // DescribePortal or execute message handle any errors that are caused by invalid
        // statements.
      }
    }
  }

  @Override
  public void flush() throws Exception {
    if (this.hasBindException) {
      if (this.connection.hasPortal(this.portalName)
          && this.connection.getPortal(this.portalName) == this.statement) {
        this.connection.closePortal(this.portalName);
      }
      handleError(this.statement.getException());
    } else if (isExtendedProtocol()) {
      // The simple query protocol does not expect a BindComplete response.
      BindCompleteResponse.send(this.outputStream);
    }
  }

  @Override
  public void abort() throws Exception {
    if (this.connection.hasPortal(this.portalName)
        && this.connection.getPortal(this.portalName) == this.statement) {
      this.connection.closePortal(this.portalName);
    }
  }

  @Override
  protected String getMessageName() {
    return MESSAGE_NAME;
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat(
            "Length: {0}, "
                + "Portal Name: {1}, "
                + "Statement Name: {2}, "
                + "Format Codes: {3}, "
                + "Parameters: {4}, "
                + "ResultFormatCodes: {5}")
        .format(
            new Object[] {
              this.length,
              this.portalName,
              this.statementName,
              this.formatCodes,
              Arrays.toString(this.parameters),
              this.resultFormatCodes
            });
  }

  @Override
  public String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  @Override
  public String receivedEventDescription() {
    return RECEIVED_EVENT_DESCRIPTION;
  }

  public String getPortalName() {
    return this.portalName;
  }

  public String getStatementName() {
    return this.statementName;
  }

  @Override
  public String getSql() {
    return this.statement == null ? "" : this.statement.getSql();
  }

  public byte[][] getParameters() {
    return this.parameters;
  }

  public short[] getFormatCodes() {
    return this.formatCodes;
  }

  public short[] getResultFormatCodes() {
    return this.resultFormatCodes;
  }
}
