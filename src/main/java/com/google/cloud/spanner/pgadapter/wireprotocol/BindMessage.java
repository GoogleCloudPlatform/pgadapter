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
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.BindCompleteResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

/**
 * Message of type bind (meaning that it is a message called to a prepared statement to complete it
 * with missing data to prepare for execution). A bound prepared statement always yields a portal
 * unless it fails.
 */
@InternalApi
public class BindMessage extends AbstractQueryProtocolMessage {
  protected static final char IDENTIFIER = 'B';

  private final String portalName;
  private final String statementName;
  private final List<Short> formatCodes;
  private final List<Short> resultFormatCodes;
  private final byte[][] parameters;
  private final IntermediatePreparedStatement statement;

  /** Constructor for Bind messages that are received from the front-end. */
  public BindMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.portalName = this.readString();
    this.statementName = this.readString();
    this.formatCodes = getFormatCodes(this.inputStream);
    this.parameters = getParameters(this.inputStream);
    this.resultFormatCodes = getFormatCodes(this.inputStream);
    this.statement = connection.getStatement(this.statementName);
  }

  /** Constructor for Bind messages that are constructed to execute a Query message. */
  public BindMessage(ConnectionHandler connection, ManuallyCreatedToken manuallyCreatedToken) {
    this(connection, "", new byte[0][], manuallyCreatedToken);
  }

  /** Constructor for Bind messages that are created by EXECUTE statements. */
  public BindMessage(
      ConnectionHandler connection,
      String statementName,
      byte[][] parameters,
      ManuallyCreatedToken manuallyCreatedToken) {
    super(connection, 4, manuallyCreatedToken);
    this.portalName = "";
    this.statementName = statementName;
    this.formatCodes = ImmutableList.of();
    this.resultFormatCodes = ImmutableList.of();
    this.parameters = Preconditions.checkNotNull(parameters);
    this.statement = connection.getStatement(statementName);
  }

  boolean hasParameterValues() {
    return this.parameters.length > 0;
  }

  @Override
  void buffer(BackendConnection backendConnection) {
    if (isExtendedProtocol() && hasParameterValues() && !this.statement.isDescribed()) {
      try {
        // Make sure all parameters have been described, so we always send typed parameters to Cloud
        // Spanner.
        this.statement.describeParameters(this.parameters, true);
      } catch (Throwable ignore) {
        // Ignore any error messages while describing the parameters, and let the following
        // DescribePortal or execute message handle any errors that are caused by invalid
        // statements.
      }
    }
    this.connection.registerPortal(
        this.portalName,
        this.statement.bind(
            this.portalName, this.parameters, this.formatCodes, this.resultFormatCodes));
  }

  @Override
  public void flush() throws Exception {
    if (isExtendedProtocol()) {
      // The simple query protocol does not expect a BindComplete response.
      new BindCompleteResponse(this.outputStream).send(false);
    }
  }

  @Override
  protected String getMessageName() {
    return "Bind";
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
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public String getPortalName() {
    return this.portalName;
  }

  public String getStatementName() {
    return this.statementName;
  }

  @Override
  public String getSql() {
    return this.statement.getSql();
  }

  public byte[][] getParameters() {
    return this.parameters;
  }

  public List<Short> getFormatCodes() {
    return this.formatCodes;
  }

  public List<Short> getResultFormatCodes() {
    return this.resultFormatCodes;
  }
}
