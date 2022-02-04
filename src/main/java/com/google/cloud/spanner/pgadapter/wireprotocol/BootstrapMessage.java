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
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.wireoutput.AuthenticationOkResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.KeyDataResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ParameterStatusResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse.Status;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * This represents all messages which occur before {@link ControlMessage} type messages. Those
 * include encryption, admin (e.g.: cancellation) and start-up messages.
 */
public abstract class BootstrapMessage extends WireMessage {

  public BootstrapMessage(ConnectionHandler connection, int length) {
    super(connection, length);
  }

  /**
   * Factory method to create the bootstrap message from their designated format. Note this is
   * backwards from control messages where identifier is first and length second.
   *
   * @param connection The connection handler object setup with the ability to send/receive.
   * @return The constructed wire message given the input message.
   * @throws Exception If construction or reading fails.
   */
  public static BootstrapMessage create(ConnectionHandler connection) throws Exception {
    int length = connection.getConnectionMetadata().getInputStream().readInt();
    int protocol = connection.getConnectionMetadata().getInputStream().readInt();
    switch (protocol) {
      case SSLMessage.IDENTIFIER:
        return new SSLMessage(connection);
      case StartupMessage.IDENTIFIER:
        return new StartupMessage(connection, length);
      case CancelMessage.IDENTIFIER:
        return new CancelMessage(connection);
      default:
        throw new IllegalStateException("Unknown message");
    }
  }

  /**
   * Parses parameters specific to bootstrap messages. Those generally (unlike control parameters)
   * exclude metadata including length and are simple null (0) delimited.
   *
   * @param rawParameters The input string containing parameters (null delimited)
   * @return A KV map of the parameters
   */
  protected Map<String, String> parseParameters(String rawParameters) {
    Map<String, String> parameters = new HashMap<>();
    String[] paramArray = rawParameters.split(new String(new byte[] {(byte) 0}));
    for (int i = 0; i < paramArray.length; i = i + 2) {
      parameters.put(paramArray[i], paramArray[i + 1]);
    }
    return parameters;
  }

  /**
   * Expected PG start-up reply, including Auth approval, Key Data connection-specific info,
   * PGAdapter specific parameters, and a ready signal.
   *
   * @param output The data output stream to send results to.
   * @param connectionId The connection Id representing the current connection to send to client.
   * @param secret The secret apposite this connection
   * @throws Exception
   */
  public static void sendStartupMessage(
      DataOutputStream output, int connectionId, int secret, OptionsMetadata options)
      throws Exception {
    new AuthenticationOkResponse(output).send();
    new KeyDataResponse(output, connectionId, secret).send();
    new ParameterStatusResponse(
            output, "server_version".getBytes(), options.getServerVersion().getBytes())
        .send();
    new ParameterStatusResponse(output, "application_name".getBytes(), "PGAdapter".getBytes())
        .send();
    new ParameterStatusResponse(output, "is_superuser".getBytes(), "false".getBytes()).send();
    new ParameterStatusResponse(output, "session_authorization".getBytes(), "PGAdapter".getBytes())
        .send();
    new ParameterStatusResponse(output, "integer_datetimes".getBytes(), "on".getBytes()).send();
    new ParameterStatusResponse(output, "server_encoding".getBytes(), "UTF8".getBytes()).send();
    new ParameterStatusResponse(output, "client_encoding".getBytes(), "UTF8".getBytes()).send();
    new ParameterStatusResponse(output, "DateStyle".getBytes(), "ISO,YMD".getBytes()).send();
    new ParameterStatusResponse(output, "IntervalStyle".getBytes(), "iso_8601".getBytes()).send();
    new ParameterStatusResponse(output, "standard_conforming_strings".getBytes(), "on".getBytes())
        .send();
    new ParameterStatusResponse(
            output,
            "TimeZone".getBytes(),
            TimeZone.getDefault().getDisplayName(false, TimeZone.SHORT).getBytes())
        .send();
    new ReadyResponse(output, Status.IDLE).send();
  }
}
