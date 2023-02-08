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

package com.google.cloud.spanner.pgadapter.wireoutput;

import static com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse.calculateLength;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ErrorResponseTest {

  @Test
  public void testCalculateLength() {
    assertEquals(
        4
            + "test message".length()
            + /* Field header + null terminator */ 2
            + "ERROR".length()
            + 2
            + "P0001".length()
            + 2
            + 1,
        calculateLength(
            PGException.newBuilder("test message")
                .setSeverity(Severity.ERROR)
                .setSQLState(SQLState.RaiseException)
                .build(),
            WellKnownClient.UNSPECIFIED));
    assertEquals(
        4
            + "test message".length()
            + /* Field header + null terminator */ 2
            + "ERROR".length()
            + 2
            + "P0001".length()
            + 2
            + "test hint".length()
            + 2
            + 1,
        calculateLength(
            PGException.newBuilder("test message")
                .setSeverity(Severity.ERROR)
                .setSQLState(SQLState.RaiseException)
                .setHints("test hint")
                .build(),
            WellKnownClient.UNSPECIFIED));

    assertEquals(
        4
            + "test message".length()
            + /* Field header + null terminator */ 2
            + "ERROR".length()
            + 2
            + "P0001".length()
            + 2
            + ClientAutoDetector.PGBENCH_USAGE_HINT.length()
            + 2
            + 1,
        calculateLength(
            PGException.newBuilder("test message")
                .setSeverity(Severity.ERROR)
                .setSQLState(SQLState.RaiseException)
                .build(),
            WellKnownClient.PGBENCH));
    assertEquals(
        4
            + "test message".length()
            + /* Field header + null terminator */ 2
            + "ERROR".length()
            + 2
            + "P0001".length()
            + 2
            + "test hint\n".length()
            + ClientAutoDetector.PGBENCH_USAGE_HINT.length()
            + 2
            + 1,
        calculateLength(
            PGException.newBuilder("test message")
                .setSeverity(Severity.ERROR)
                .setSQLState(SQLState.RaiseException)
                .setHints("test hint")
                .build(),
            WellKnownClient.PGBENCH));
  }

  @Test
  public void testSendPayload() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(out);
    ConnectionMetadata metadata = mock(ConnectionMetadata.class);
    when(metadata.getOutputStream()).thenReturn(output);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    when(connectionHandler.getWellKnownClient()).thenReturn(WellKnownClient.UNSPECIFIED);
    when(connectionHandler.getConnectionMetadata()).thenReturn(metadata);
    ErrorResponse response =
        new ErrorResponse(
            connectionHandler,
            PGException.newBuilder("test message")
                .setSeverity(Severity.ERROR)
                .setSQLState(SQLState.RaiseException)
                .build());

    response.sendPayload();

    assertEquals("SERROR\0CP0001\0Mtest message\0\0", out.toString());
    assertEquals("Length: 33, Error Message: test message, Hints: ", response.getPayloadString());
  }

  @Test
  public void testSendPayloadWithHint() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(out);
    ConnectionMetadata metadata = mock(ConnectionMetadata.class);
    when(metadata.getOutputStream()).thenReturn(output);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    when(connectionHandler.getWellKnownClient()).thenReturn(WellKnownClient.UNSPECIFIED);
    when(connectionHandler.getConnectionMetadata()).thenReturn(metadata);
    ErrorResponse response =
        new ErrorResponse(
            connectionHandler,
            PGException.newBuilder("test message")
                .setSeverity(Severity.ERROR)
                .setSQLState(SQLState.RaiseException)
                .setHints("test hint\nline 2")
                .build());

    response.sendPayload();

    assertEquals("SERROR\0CP0001\0Mtest message\0Htest hint\nline 2\0\0", out.toString());
    assertEquals(
        "Length: 51, Error Message: test message, Hints: test hint\n" + "line 2",
        response.getPayloadString());
  }

  @Test
  public void testSendPayloadWithClientHint() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(out);
    ConnectionMetadata metadata = mock(ConnectionMetadata.class);
    when(metadata.getOutputStream()).thenReturn(output);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    when(connectionHandler.getWellKnownClient()).thenReturn(WellKnownClient.PGBENCH);
    when(connectionHandler.getConnectionMetadata()).thenReturn(metadata);
    ErrorResponse response =
        new ErrorResponse(
            connectionHandler,
            PGException.newBuilder("test message")
                .setSeverity(Severity.ERROR)
                .setSQLState(SQLState.RaiseException)
                .build());

    response.sendPayload();

    assertEquals(
        String.format(
            "SERROR\0CP0001\0Mtest message\0H%s\0\0", ClientAutoDetector.PGBENCH_USAGE_HINT),
        out.toString());
    assertEquals(
        "Length: 148, Error Message: test message, Hints: " + ClientAutoDetector.PGBENCH_USAGE_HINT,
        response.getPayloadString());
  }

  @Test
  public void testSendPayloadWithErrorAndClientHint() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(out);
    ConnectionMetadata metadata = mock(ConnectionMetadata.class);
    when(metadata.getOutputStream()).thenReturn(output);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    when(connectionHandler.getWellKnownClient()).thenReturn(WellKnownClient.PGBENCH);
    when(connectionHandler.getConnectionMetadata()).thenReturn(metadata);
    ErrorResponse response =
        new ErrorResponse(
            connectionHandler,
            PGException.newBuilder("test message")
                .setSeverity(Severity.ERROR)
                .setSQLState(SQLState.RaiseException)
                .setHints("test hint\nline 2")
                .build());

    response.sendPayload();

    assertEquals(
        String.format(
            "SERROR\0CP0001\0Mtest message\0Htest hint\nline 2\n%s\0\0",
            ClientAutoDetector.PGBENCH_USAGE_HINT),
        out.toString());
    assertEquals(
        "Length: 165, Error Message: test message, Hints: test hint\nline 2\n"
            + ClientAutoDetector.PGBENCH_USAGE_HINT,
        response.getPayloadString());
  }
}
