// Copyright 2024 Google LLC
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

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.pgadapter.wireprotocol.StartupMessage;
import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StartupMessageMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        createMockSpannerThatReturnsOneQueryPartition(),
        "d",
        configurator -> {},
        OpenTelemetry.noop());
  }

  private void sendStartupMessage(DataOutputStream outputStream, Map<String, String> parameters)
      throws IOException {
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    try (DataOutputStream payloadStream = new DataOutputStream(payload)) {
      payloadStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
      for (Map.Entry<String, String> entry : parameters.entrySet()) {
        payloadStream.writeBytes(entry.getKey());
        payloadStream.writeByte(0);
        payloadStream.writeBytes(entry.getValue());
        payloadStream.writeByte(0);
      }
      payloadStream.writeByte(0);
    }
    outputStream.writeInt(payload.size() + 4);
    outputStream.write(payload.toByteArray());
    outputStream.flush();
  }

  private Map<String, String> readParameterStatuses(DataInputStream inputStream)
      throws IOException {
    assertEquals('R', inputStream.readByte());
    assertEquals(8, inputStream.readInt());
    assertEquals(0, inputStream.readInt());

    assertEquals('K', inputStream.readByte());
    int keyLength = inputStream.readInt();
    byte[] keyBytes = new byte[keyLength - 4];
    inputStream.readFully(keyBytes);

    Map<String, String> parameterStatuses = new HashMap<>();
    while (true) {
      byte message = inputStream.readByte();
      int length = inputStream.readInt();
      byte[] contents = new byte[length - 4];
      inputStream.readFully(contents);
      if (message == 'Z') {
        break;
      } else if (message == 'S') {
        String statusPayload = new String(contents, StandardCharsets.UTF_8);
        String[] parts = statusPayload.split("\0");
        if (parts.length >= 2) {
          parameterStatuses.put(parts[0], parts[1]);
        }
      }
    }
    return parameterStatuses;
  }

  @Test
  public void testDefaultTimeZoneOnStartup() throws IOException {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataInputStream inputStream = new DataInputStream(socket.getInputStream());
          DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        sendStartupMessage(outputStream, ImmutableMap.of("user", "foo", "database", "bar"));
        Map<String, String> parameters = readParameterStatuses(inputStream);
        assertEquals("UTC", parameters.get("TimeZone"));
      }
    }
  }

  @Test
  public void testCustomTimeZoneOnStartup() throws IOException {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataInputStream inputStream = new DataInputStream(socket.getInputStream());
          DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        sendStartupMessage(
            outputStream,
            ImmutableMap.of("user", "foo", "database", "bar", "TimeZone", "Europe/Paris"));
        Map<String, String> parameters = readParameterStatuses(inputStream);
        assertEquals("Europe/Paris", parameters.get("TimeZone"));
      }
    }
  }
}
