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
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.metadata.SendResultSetState;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.CommandCompleteResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.DataRowResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse.State;
import com.google.cloud.spanner.pgadapter.wireoutput.PortalSuspendedResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic representation for a control wire message: that is, a message which does not handle any
 * form of start-up, but reather general communications.
 */
public abstract class ControlMessage extends WireMessage {

  public ControlMessage(ConnectionHandler connection) throws IOException {
    super(connection, connection.getConnectionMetadata().getInputStream().readInt());
  }

  /**
   * Factory method to create the message from the specific command type char.
   *
   * @param connection The connection handler object setup with the ability to send/receive.
   * @return The constructed wire message given the input message.
   * @throws Exception If construction or reading fails.
   */
  public static ControlMessage create(ConnectionHandler connection) throws Exception {
    char nextMsg = (char) connection.getConnectionMetadata().getInputStream().readUnsignedByte();
    if (connection.getStatus() == ConnectionStatus.COPY_IN) {
      switch (nextMsg) {
        case CopyDoneMessage.IDENTIFIER:
          return new CopyDoneMessage(connection);
        case CopyDataMessage.IDENTIFIER:
          return new CopyDataMessage(connection);
        default:
          throw new IllegalStateException("Expected 0 or more Copy Data messages.");
      }
    } else {
      switch (nextMsg) {
        case QueryMessage.IDENTIFIER:
          return new QueryMessage(connection);
        case ParseMessage.IDENTIFIER:
          return new ParseMessage(connection);
        case BindMessage.IDENTIFIER:
          return new BindMessage(connection);
        case DescribeMessage.IDENTIFIER:
          return new DescribeMessage(connection);
        case ExecuteMessage.IDENTIFIER:
          return new ExecuteMessage(connection);
        case CloseMessage.IDENTIFIER:
          return new CloseMessage(connection);
        case SyncMessage.IDENTIFIER:
          return new SyncMessage(connection);
        case TerminateMessage.IDENTIFIER:
          return new TerminateMessage(connection);
        case CopyDoneMessage.IDENTIFIER:
          return new CopyDoneMessage(connection);
        case CopyDataMessage.IDENTIFIER:
          return new CopyDataMessage(connection);
        case CopyFailMessage.IDENTIFIER:
          return new CopyFailMessage(connection);
        case FunctionCallMessage.IDENTIFIER:
          return new FunctionCallMessage(connection);
        case FlushMessage.IDENTIFIER:
          return new FlushMessage(connection);
        default:
          throw new IllegalStateException("Unknown message");
      }
    }
  }

  /**
   * Extract format codes from message (useful for both input and output format codes).
   *
   * @param input The data stream containing the user request.
   * @return A list of format codes.
   * @throws Exception If reading fails in any way.
   */
  protected static List<Short> getFormatCodes(DataInputStream input) throws Exception {
    List<Short> formatCodes = new ArrayList<>();
    short numberOfFormatCodes = input.readShort();
    for (int i = 0; i < numberOfFormatCodes; i++) {
      formatCodes.add(input.readShort());
    }
    return formatCodes;
  }

  public enum PreparedType {
    Portal,
    Statement;

    static PreparedType prepareType(char type) {
      switch (type) {
        case ('P'):
          return PreparedType.Portal;
        case ('S'):
          return PreparedType.Statement;
        default:
          throw new IllegalArgumentException("Unknown Statement type!");
      }
    }
  }

  /**
   * Takes an Exception Object and relates its results to the user within the client.
   *
   * @param e The exception to be related.
   * @throws Exception if there is some issue in the sending of the error messages.
   */
  protected void handleError(Exception e) throws Exception {
    new ErrorResponse(this.outputStream, e, State.InternalError).send();
    new ReadyResponse(this.outputStream, ReadyResponse.Status.IDLE).send();
  }

  /**
   * Sends the result of an execute or query to the client. The type of message depends on the type
   * of result of the statement. This method may also be called multiple times for one result if the
   * client has set a max number of rows to fetch for each execute message. The {@link
   * IntermediateStatement} will cache the result in between calls and continue serving rows from
   * the position it was left off after the last execute message.
   */
  public boolean sendSpannerResult(IntermediateStatement statement, QueryMode mode, long maxRows)
      throws Exception {
    String command = statement.getCommand();
    switch (statement.getResultType()) {
      case NO_RESULT:
        new CommandCompleteResponse(this.outputStream, command).send();
        return false;
      case RESULT_SET:
        SendResultSetState state = sendResultSet(statement, mode, maxRows);
        statement.setHasMoreData(state.hasMoreRows());
        if (state.hasMoreRows()) {
          new PortalSuspendedResponse(this.outputStream).send();
        } else {
          statement.getStatementResult().close();
          new CommandCompleteResponse(this.outputStream, "SELECT " + state.getNumberOfRowsSent())
              .send();
        }
        return state.hasMoreRows();
      case UPDATE_COUNT:
        // For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows
        // inserted. oid used to be the object ID of the inserted row if rows was 1 and the target
        // table had OIDs, but OIDs system columns are not supported anymore; therefore oid is
        // always 0.
        command += ("INSERT".equals(command) ? " 0 " : " ") + statement.getUpdateCount();
        new CommandCompleteResponse(this.outputStream, command).send();
        return false;
      default:
        throw new IllegalStateException("Unknown result type: " + statement.getResultType());
    }
  }

  /**
   * Simple Adapter, which takes specific results from Spanner, and packages them in a format
   * Postgres understands.
   *
   * @param describedResult Statement output by Spanner.
   * @param mode Specific Query Mode required for this specific message for Postgres
   * @param maxRows Maximum number of rows requested
   * @return An adapted representation with specific metadata which PG wire requires.
   * @throws Exception
   */
  public SendResultSetState sendResultSet(
      IntermediateStatement describedResult, QueryMode mode, long maxRows) throws Exception {
    long rows = 0;
    boolean hasData = describedResult.isHasMoreData();
    ResultSet resultSet = describedResult.getStatementResult();
    // TODO optimize loop
    while (hasData) {
      new DataRowResponse(
              this.outputStream, describedResult, this.connection.getServer().getOptions(), mode)
          .send();
      rows++;
      try {
        hasData = resultSet.next();
      } catch (Exception e) {
        System.err.println("Something went wrong with getting next!");
      }
      if (rows == maxRows) {
        break;
      }
    }
    return new SendResultSetState(rows, hasData);
  }
}
