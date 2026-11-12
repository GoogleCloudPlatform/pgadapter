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
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.wireoutput.AuthenticationOkResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.KeyDataResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.NoticeResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ParameterStatusResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.common.base.Strings;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This represents all messages which occur before {@link ControlMessage} type messages. Those
 * include encryption, admin (e.g.: cancellation) and start-up messages.
 */
@InternalApi
public abstract class BootstrapMessage extends WireMessage {

  static final int MAX_STARTUP_PACKET_LENGTH = 10_000;

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
    checkLengthRange(length, "startup packet", 8, MAX_STARTUP_PACKET_LENGTH);
    int protocol = connection.getConnectionMetadata().getInputStream().readInt();
    switch (protocol) {
      case SSLMessage.IDENTIFIER:
        checkLengthEquals(length, "SSL request", 8);
        return new SSLMessage(connection);
      case GSSENCRequestMessage.IDENTIFIER:
        checkLengthEquals(length, "GSSAPI request", 8);
        return new GSSENCRequestMessage(connection);
      case StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER:
      case StartupMessage.PROTOCOL_VERSION_3_2_IDENTIFIER:
        return new StartupMessage(connection, length, protocol);
      case CancelMessage.IDENTIFIER:
        checkLengthIn(length, "cancel request", 16, 44);
        return new CancelMessage(connection, length);
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
    if (Strings.isNullOrEmpty(rawParameters)) {
      return parameters;
    }
    String[] paramArray = rawParameters.split("\0", -1);
    for (int i = 0; i < paramArray.length - 1; i += 2) {
      if (!paramArray[i].isEmpty()) {
        parameters.put(paramArray[i], paramArray[i + 1]);
      }
    }
    return parameters;
  }

  /**
   * Parses the parameter keys from the given raw parameter string and returns these in the received
   * order. This can be used to detect the client that is connecting to PGAdapter.
   */
  protected List<String> parseParameterKeys(String rawParameters) {
    List<String> keys = new ArrayList<>();
    if (Strings.isNullOrEmpty(rawParameters)) {
      return keys;
    }
    String[] paramArray = rawParameters.split("\0", -1);
    for (int i = 0; i < paramArray.length - 1; i += 2) {
      if (!paramArray[i].isEmpty()) {
        keys.add(paramArray[i]);
      }
    }
    return keys;
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
      DataOutputStream output,
      int connectionId,
      byte[] secret,
      SessionState sessionState,
      Iterable<NoticeResponse> startupNotices)
      throws Exception {
    new AuthenticationOkResponse(output).send(false);
    new KeyDataResponse(output, connectionId, secret).send(false);
    new ParameterStatusResponse(
            output,
            "server_version".getBytes(StandardCharsets.UTF_8),
            sessionState.get(null, "server_version").getSetting().getBytes(StandardCharsets.UTF_8))
        .send(false);
    new ParameterStatusResponse(
            output,
            "application_name".getBytes(StandardCharsets.UTF_8),
            "PGAdapter".getBytes(StandardCharsets.UTF_8))
        .send(false);
    new ParameterStatusResponse(
            output,
            "is_superuser".getBytes(StandardCharsets.UTF_8),
            "false".getBytes(StandardCharsets.UTF_8))
        .send(false);
    new ParameterStatusResponse(
            output,
            "session_authorization".getBytes(StandardCharsets.UTF_8),
            "PGAdapter".getBytes(StandardCharsets.UTF_8))
        .send(false);
    new ParameterStatusResponse(
            output,
            "integer_datetimes".getBytes(StandardCharsets.UTF_8),
            "on".getBytes(StandardCharsets.UTF_8))
        .send(false);
    new ParameterStatusResponse(
            output,
            "server_encoding".getBytes(StandardCharsets.UTF_8),
            "UTF8".getBytes(StandardCharsets.UTF_8))
        .send(false);
    new ParameterStatusResponse(
            output,
            "client_encoding".getBytes(StandardCharsets.UTF_8),
            "UTF8".getBytes(StandardCharsets.UTF_8))
        .send(false);
    new ParameterStatusResponse(
            output,
            "DateStyle".getBytes(StandardCharsets.UTF_8),
            "ISO,YMD".getBytes(StandardCharsets.UTF_8))
        .send(false);
    new ParameterStatusResponse(
            output,
            "IntervalStyle".getBytes(StandardCharsets.UTF_8),
            "iso_8601".getBytes(StandardCharsets.UTF_8))
        .send(false);
    new ParameterStatusResponse(
            output,
            "standard_conforming_strings".getBytes(StandardCharsets.UTF_8),
            "on".getBytes(StandardCharsets.UTF_8))
        .send(false);
    ZoneId zoneId = sessionState.getTimezone();
    String timeZone = zoneId == null ? OptionsMetadata.DEFAULT_TIME_ZONE : zoneId.getId();
    new ParameterStatusResponse(
            output,
            "TimeZone".getBytes(StandardCharsets.UTF_8),
            timeZone.getBytes(StandardCharsets.UTF_8))
        .send(false);
    for (NoticeResponse noticeResponse : startupNotices) {
      noticeResponse.send(false);
    }
    ReadyResponse.sendIdleResponse(output);
    output.flush();
  }
}
