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

package com.google.cloud.spanner.pgadapter;

import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.SendResultSetState;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.TextFormat;
import com.google.cloud.spanner.pgadapter.metadata.User;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.utils.Converter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.util.ByteConverter;

/**
 * Reads and sends messages using the PostgreSQL wire protocol.
 */
public class PGWireProtocol {

  private static final String USER_KEY = "user";

  private static final Logger logger = Logger.getLogger(PGWireProtocol.class.getName());
  private static final Charset UTF8 = StandardCharsets.UTF_8;
  private final Converter converter = new Converter();
  private final ConnectionHandler connection;
  private final DataInputStream input;
  private final DataOutputStream output;
  private final TextFormat textFormat;
  private final boolean forceBinaryFormat;

  PGWireProtocol(ConnectionHandler connection, DataInputStream input, DataOutputStream output) {
    this.connection = connection;
    this.input = input;
    this.output = output;
    this.textFormat = connection.getServer().getOptions().getTextFormat();
    this.forceBinaryFormat = connection.getServer().getOptions().isBinaryFormat();
  }

  /**
   * Reads a fixed-length string from a {@link DataInputStream}. The string still needs to be
   * null-terminated, but the read is more efficient, as we can read the entire string in one go
   * instead of continuously checking for a null-terminator.
   *
   * @param input The {@link DataInputStream} to read from.
   * @param length The number of bytes to read.
   * @return the string.
   * @throws IOException if an error occurs while reading from the stream, or if no null-terminator
   * is found at the end of the string.
   */
  public static String readString(DataInputStream input, int length) throws IOException {
    byte[] buffer = new byte[length - 1];
    input.readFully(buffer);
    byte zero = input.readByte();
    if (zero != (byte) 0) {
      throw new IOException("String was not null-terminated");
    }
    return new String(buffer, UTF8);
  }

  /**
   * Reads a null-terminated string from a {@link DataInputStream}.
   *
   * @param input The {@link DataInputStream} to read from.
   * @return the string.
   * @throws IOException if an error occurs while reading from the stream, or if no null-terminator
   * is found before the end of the stream.
   */
  public static String readString(DataInputStream input) throws IOException {
    byte[] buffer = new byte[128];
    int index = 0;
    while (true) {
      byte b = input.readByte();
      if (b == 0) {
        break;
      }
      buffer[index] = b;
      index++;
      if (index == buffer.length) {
        buffer = Arrays.copyOf(buffer, buffer.length * 2);
      }
    }
    return new String(buffer, 0, index, UTF8);
  }

  /**
   * Used to initiate communication with the server if auth is not required.
   *
   * @return True if auth is not required. False otherwise.
   * @throws IOException if there are issues in message transmission
   */
  boolean dontAuthenticate() throws IOException {
    if (!this.connection.getServer().getOptions().shouldAuthenticate()) {
      receiveStartupMessage(false);
      sendAuthenticationOk();
      sendStartupMessages();
      sendReadyForQuery(Status.IDLE);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Used to negotiate auth when required. Currently not performing user auth until simple
   * user-facing mechanism for doing so is decided.
   *
   * @return True if auth is to be performed and was performed. False otherwise
   * @throws IOException if there are issues is message transmission
   */
  boolean doAuthenticate() throws IOException {
    // Assume that we are starting a new session, so we'll start to listen for a startup msg.
    Map<String, String> params = receiveStartupMessage(false);
    String username = params.get(USER_KEY);
    logger.log(Level.FINEST, "Authenticating user: " + username);
    // TODO add an actual way to auth
    if (this.connection.getServer().getOptions().shouldAuthenticate()) {
      // We received a startup message. Now request authentication.
      // sendRequestPlainTextAuthentication();
      sendRequestMd5Authentication(0);
      // Receive the password.
      if (receivePasswordMessage(null, false)) {
        // Ok, just accept the password.
        sendAuthenticationOk();
        // Send parameters.
        sendStartupMessages();
        // And then indicate that we are ready for queries.
        sendReadyForQuery(Status.IDLE);
        return true;
      }
    }
    sendError(new Exception("Authentication failed"));
    sendTerminate();
    return false;
  }

  /**
   * Wait for a startup message and returns the parameters sent by the client.
   */
  private Map<String, String> receiveStartupMessage(boolean sslRejected) throws IOException {
    // Read length of the entire message.
    int length = this.input.readInt();
    int protocol = this.input.readInt();
    // Length of params = length - 4 (length) - 4 (protocol)
    Map<String, String> params = new HashMap<>();
    if (length > 8) {
      String paramString = readString(this.input, length - 8);
      String[] paramArray = paramString.split(new String(new byte[]{(byte) 0}));
      for (int i = 0; i < paramArray.length; i = i + 2) {
        params.put(paramArray[i], paramArray[i + 1]);
      }
    }
    if (protocol == 80877103) {
      if (sslRejected) {
        // SSL connection has already been rejected. Kill the connection.
        throw new IOException("SSL not supported by server");
      } else {
        // SSL request. SSL is not supported, so respond with 'N'.
        this.output.writeByte('N');
        this.output.flush();
        return receiveStartupMessage(true);
      }
    } else if (protocol == 196608) {
      logger.log(Level.FINEST, "Received startup message on {0} with parameters {1}",
          new Object[]{connection.getName(), params});
      return params;
    } else {
      logger.log(Level.SEVERE, "Received startup message on {0} with unknown protocol: {1}",
          new Object[]{connection.getName(), protocol});
      throw new IOException("Unknown protocol: " + protocol);
    }
  }

  private boolean receivePasswordMessage(User user, boolean useAuthentication) throws IOException {
    try {
      char cmd = (char) this.input.readUnsignedByte();
      if (cmd != 'p') {
        throw new IOException("Unexpected response, expected 'p', but got: " + cmd);
      }
      int length = this.input.readInt();
      String password = readString(this.input, length - 4);
      logger.log(Level.FINEST, "Received password on {0}", connection.getName());
      return !useAuthentication || checkCredentials(user, password);
    } catch (EOFException e) {
      // Some clients close the connection after being denied an SSL connection.
      logger.log(Level.FINEST,
          "Client closed the connection before a password could be received on {0}",
          connection.getName());
      return false;
    }
  }

  private boolean checkCredentials(User user, String password) {
    return user.verify(password);
  }

  private void sendRequestMd5Authentication(int salt) throws IOException {
    byte[] saltBytes = new byte[4];
    ByteConverter.int4(saltBytes, 0, salt);
    this.output.writeByte('R');
    this.output.writeInt(12);
    this.output.writeInt(5);
    this.output.write(saltBytes);
    this.output.flush();
    logger.log(Level.FINEST, "AuthenticationRequest MD5 sent on {0}", connection.getName());
  }

  private void sendAuthenticationOk() throws IOException {
    this.output.writeByte('R');
    this.output.writeInt(8);
    this.output.writeInt(0);
    this.output.flush();
    logger.log(Level.FINEST, "AuthenticationOK sent on {0}", connection.getName());
  }

  private void sendStartupMessages() throws IOException {
    sendStartupMessage("integer_datetimes", "on");
    sendStartupMessage("client_encoding", "utf8");
    sendStartupMessage("DateStyle", "ISO");
  }

  private void sendStartupMessage(String param, String value) throws IOException {
    byte[] paramBytes = param.getBytes(UTF8);
    byte[] valueBytes = value.getBytes(UTF8);
    this.output.writeByte('S');
    this.output.writeInt(4 + paramBytes.length + 1 + valueBytes.length + 1);
    this.output.write(paramBytes);
    this.output.writeByte(0);
    this.output.write(valueBytes);
    this.output.writeByte(0);
  }

  void sendReadyForQuery(Status status) throws IOException {
    this.output.writeByte('Z');
    this.output.writeInt(5);
    this.output.writeByte(status.c);
    this.output.flush();
    logger.log(Level.FINEST, "ReadyForQuery sent with status {0} on {1}",
        new Object[]{status, connection.getName()});
  }

  void sendError(Exception e) throws IOException {
    this.output.writeByte('E');
    String severity = "ERROR";
    byte[] severityBytes = severity.getBytes(UTF8);
    String sqlState = "XX000";
    byte[] sqlStateBytes = sqlState.getBytes(UTF8);
    String msg = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
    byte[] msgBytes = msg.getBytes(UTF8);
    int length = 4 + 1 + (severityBytes.length + 1) + 1 + (sqlStateBytes.length + 1) + 1
        + (msgBytes.length + 1) + 1;
    this.output.writeInt(length);
    this.output.writeByte('S');
    this.output.write(severityBytes);
    this.output.writeByte(0);
    this.output.writeByte('C');
    this.output.write(sqlStateBytes);
    this.output.writeByte(0);
    this.output.writeByte('M');
    this.output.write(msgBytes);
    this.output.writeByte(0);
    this.output.writeByte(0);
    this.output.flush();
    logger.log(Level.FINEST, "Error with msg {0} sent on {1}",
        new Object[]{msg, connection.getName()});
  }

  void sendTerminate() throws IOException {
    this.output.writeByte('X');
    this.output.writeInt(4);
    this.output.flush();
    logger.log(Level.FINEST, "Terminate sent on {0}", connection.getName());
  }

  void sendParseComplete() throws IOException {
    this.output.writeByte('1');
    this.output.writeInt(4);
    this.output.flush();
    logger.log(Level.FINEST, "ParseComplete sent on {0}", connection.getName());
  }

  void sendBindComplete() throws IOException {
    this.output.writeByte('2');
    this.output.writeInt(4);
    this.output.flush();
    logger.log(Level.FINEST, "BindComplete sent on {0}", connection.getName());
  }

  void sendParameterDescription(List<Integer> parameters)
      throws IOException {
    int length = 4 + 2 + parameters.size() * 4;
    this.output.writeByte('t');
    this.output.writeInt(length);
    this.output.writeShort(parameters.size());
    for (int parameter : parameters) {
      this.output.writeInt(parameter);
    }
  }

  void sendRowDescription(IntermediateStatement statement, ResultSetMetaData metadata,
      TextFormat defaultTextFormat,
      QueryMode mode) throws Exception {
    int length = 4 + 2; // length + number of columns
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      length += metadata.getColumnName(i).length() + 1;
      length += 4; // table OID
      length += 2; // column index
      length += 4; // data type OID
      length += 2; // data type size
      length += 4; // type modifier
      length += 2; // format code
    }
    this.output.writeByte('T');
    this.output.writeInt(length);
    this.output.writeShort(metadata.getColumnCount());
    DataFormat defaultFormat = getDataFormat(0, statement, defaultTextFormat, mode);
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      this.output.write(metadata.getColumnName(i).getBytes(UTF8));
      this.output.writeByte(0);
      this.output.writeInt(0);
      this.output.writeShort(0);
      this.output.writeInt(metadata.getColumnType(i));
      this.output.writeShort(metadata.getColumnType(i));
      this.output.writeInt(0);
      short format = statement == null ? defaultFormat.getCode()
          : statement.getResultFormatCode(i) == 0 ? defaultFormat.getCode() :
              statement.getResultFormatCode(i);
      this.output.writeShort(format);
    }
    this.output.flush();
    logger.log(Level.FINEST, "RowDescription sent on {0}", connection.getName());
  }

  void sendNoData() throws IOException {
    this.output.writeByte('n');
    this.output.writeInt(4); // length
    logger.log(Level.FINEST, "NoData sent on {0}", connection.getName());
  }

  SendResultSetState sendResultSet(IntermediateStatement describedResult, QueryMode mode,
      long maxRows) throws IOException, SQLException {
    long rows = 0;
    boolean hasData = describedResult.isHasMoreData();
    ResultSet resultSet = describedResult.getStatementResult();
    while (hasData) {
      sendDataRow(describedResult, mode);
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
    logger.log(Level.FINEST, "ResultSet sent on {0}", connection.getName());
    return new SendResultSetState(rows, hasData);
  }

  void sendPortalSuspended() throws IOException {
    this.output.writeByte('s');
    this.output.writeInt(4);
    logger.log(Level.FINEST, "PortalSuspended sent on {0}", connection.getName());
  }

  void sendCommandComplete(String command) throws IOException {
    this.output.writeByte('C');
    byte[] commandBytes = command.getBytes(UTF8);
    this.output.writeInt(4 + commandBytes.length + 1);
    this.output.write(commandBytes);
    this.output.writeByte(0);
    this.output.flush();
    logger.log(Level.FINEST, "CommandComplete sent on {0}", connection.getName());
  }

  void sendCloseComplete() throws IOException {
    this.output.writeByte('3');
    this.output.writeInt(4);
    logger.log(Level.FINEST, "CloseComplete sent on {0}", connection.getName());
  }

  private void sendDataRow(IntermediateStatement statement, QueryMode mode)
      throws IOException, SQLException {
    ResultSet rs = statement.getStatementResult();
    ResultSetMetaData metaData = rs.getMetaData();
    int length = 4 + 2;
    List<byte[]> columns = new ArrayList<>(metaData.getColumnCount());
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      length += 4;
      if (rs.getObject(i) == null) {
        columns.add(null);
      } else {
        DataFormat format = getDataFormat(i, statement, textFormat, mode);
        byte[] column = converter.parseData(rs, metaData, i, format);
        length += column.length;
        columns.add(column);
      }
    }
    this.output.writeByte('D');
    this.output.writeInt(length);
    this.output.writeShort(metaData.getColumnCount());
    for (byte[] val : columns) {
      if (val == null) {
        this.output.writeInt(-1);
      } else {
        this.output.writeInt(val.length);
        this.output.write(val);
      }
    }
    this.output.flush();
  }

  private DataFormat getDataFormat(int index, IntermediateStatement statement,
      TextFormat textFormat, QueryMode mode) {
    if (forceBinaryFormat) {
      return DataFormat.POSTGRESQL_BINARY;
    }
    if (mode == QueryMode.SIMPLE) {
      // Simple query mode is always text.
      return DataFormat.fromTextFormat(textFormat);
    } else {
      return DataFormat.byCode(statement.getResultFormatCode(index), textFormat);
    }
  }

  /**
   * Status of the session.
   */
  enum Status {
    IDLE('I'), TRANSACTION('T'), FAILED('E');
    private final char c;

    Status(char c) {
      this.c = c;
    }
  }

}
