package com.google.cloud.spanner.pgadapter.wireprotocol;

import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse.Status;
import java.text.MessageFormat;

public class InvalidMessage extends ControlMessage {
  private final Exception exception;

  public InvalidMessage(ConnectionHandler connection, Exception exception) throws Exception {
    super(connection);
    this.exception = exception;
    // Read the entire message to ensure the first character in the invalid message is not
    // interpreted as the beginning of the next message.
    this.readAll();
  }

  @Override
  protected void sendPayload() throws Exception {
    new ErrorResponse(this.outputStream, exception, ErrorResponse.State.InternalError).send();
    if (connection.getStatus() != ConnectionStatus.COPY_IN) {
      boolean inTransaction =
          connection.getJdbcConnection().unwrap(CloudSpannerJdbcConnection.class).isInTransaction();
      new ReadyResponse(this.outputStream, inTransaction ? Status.TRANSACTION : Status.IDLE).send();
    }
  }

  @Override
  protected String getMessageName() {
    return "Invalid";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}").format(new Object[] {this.length});
  }

  @Override
  protected String getIdentifier() {
    return "INVALID";
  }
}
