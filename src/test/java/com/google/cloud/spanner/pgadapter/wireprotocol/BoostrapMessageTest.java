// Copyright 2023 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BoostrapMessageTest {
  private ConnectionHandler connection;

  private DataOutputStream dataOutputStream;

  @Before
  public void setupConnection() throws IOException {
    connection = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    when(connection.getServer()).thenReturn(server);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(server.getOptions()).thenReturn(options);
    ConnectionMetadata metadata = mock(ConnectionMetadata.class);
    when(connection.getConnectionMetadata()).thenReturn(metadata);
    PipedInputStream pipedInputStream = new PipedInputStream(2048);
    DataInputStream dataInputStream = new DataInputStream(pipedInputStream);
    when(metadata.getInputStream()).thenReturn(dataInputStream);
    dataOutputStream = new DataOutputStream(new PipedOutputStream(pipedInputStream));
  }

  @After
  public void cleanup() throws IOException {
    dataOutputStream.close();
  }

  @Test
  public void testCreate() throws Exception {
    // length
    dataOutputStream.writeInt(1024);
    // identifier
    dataOutputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
    dataOutputStream.write(new byte[1024]);

    BootstrapMessage message = BootstrapMessage.create(connection);
    assertEquals(StartupMessage.class, message.getClass());
    assertEquals(1024, message.length);
  }

  @Test
  public void testCreate_lengthTooSmall() throws IOException {
    dataOutputStream.writeInt(7);

    PGException exception =
        assertThrows(PGException.class, () -> BootstrapMessage.create(connection));
    assertEquals(SQLState.ProtocolViolation, exception.getSQLState());
    assertEquals(Severity.FATAL, exception.getSeverity());
    assertTrue(exception.getMessage().contains("invalid length of startup packet"));
  }

  @Test
  public void testCreate_lengthTooLarge() throws IOException {
    dataOutputStream.writeInt(10_001);

    PGException exception =
        assertThrows(PGException.class, () -> BootstrapMessage.create(connection));
    assertEquals(SQLState.ProtocolViolation, exception.getSQLState());
    assertEquals(Severity.FATAL, exception.getSeverity());
    assertTrue(exception.getMessage().contains("invalid length of startup packet"));
  }

  @Test
  public void testCreate_lengthExtreme() throws IOException {
    dataOutputStream.writeInt(2_000_000_000);

    PGException exception =
        assertThrows(PGException.class, () -> BootstrapMessage.create(connection));
    assertEquals(SQLState.ProtocolViolation, exception.getSQLState());
    assertEquals(Severity.FATAL, exception.getSeverity());
    assertTrue(exception.getMessage().contains("invalid length of startup packet"));
  }

  @Test
  public void testCreateSsl() throws Exception {
    // length
    dataOutputStream.writeInt(8);
    // identifier
    dataOutputStream.writeInt(SSLMessage.IDENTIFIER);
    dataOutputStream.write(new byte[8]);

    BootstrapMessage message = BootstrapMessage.create(connection);
    assertEquals(SSLMessage.class, message.getClass());
    assertEquals(8, message.length);
  }

  @Test
  public void testCreateSsl_invalidLength() throws IOException {
    dataOutputStream.writeInt(12);
    dataOutputStream.writeInt(SSLMessage.IDENTIFIER);

    PGException exception =
        assertThrows(PGException.class, () -> BootstrapMessage.create(connection));
    assertEquals(SQLState.ProtocolViolation, exception.getSQLState());
    assertEquals(Severity.FATAL, exception.getSeverity());
    assertTrue(exception.getMessage().contains("invalid length of SSL request"));
  }

  @Test
  public void testCreateGssenc() throws Exception {
    dataOutputStream.writeInt(8);
    dataOutputStream.writeInt(GSSENCRequestMessage.IDENTIFIER);

    BootstrapMessage message = BootstrapMessage.create(connection);
    assertEquals(GSSENCRequestMessage.class, message.getClass());
    assertEquals(8, message.length);
  }

  @Test
  public void testCreateGssenc_invalidLength() throws IOException {
    dataOutputStream.writeInt(12);
    dataOutputStream.writeInt(GSSENCRequestMessage.IDENTIFIER);

    PGException exception =
        assertThrows(PGException.class, () -> BootstrapMessage.create(connection));
    assertEquals(SQLState.ProtocolViolation, exception.getSQLState());
    assertEquals(Severity.FATAL, exception.getSeverity());
    assertTrue(exception.getMessage().contains("invalid length of GSSAPI request"));
  }

  @Test
  public void testCreateCancel() throws Exception {
    // length
    dataOutputStream.writeInt(16);
    // identifier
    dataOutputStream.writeInt(CancelMessage.IDENTIFIER);
    dataOutputStream.write(new byte[16]);

    BootstrapMessage message = BootstrapMessage.create(connection);
    assertEquals(CancelMessage.class, message.getClass());
    assertEquals(16, message.length);
  }

  @Test
  public void testCreateCancel_protocolVersion32() throws Exception {
    dataOutputStream.writeInt(44);
    dataOutputStream.writeInt(CancelMessage.IDENTIFIER);
    dataOutputStream.write(new byte[44]);

    BootstrapMessage message = BootstrapMessage.create(connection);
    assertEquals(CancelMessage.class, message.getClass());
    assertEquals(44, message.length);
  }

  @Test
  public void testCreateCancel_invalidLength() throws IOException {
    dataOutputStream.writeInt(20);
    dataOutputStream.writeInt(CancelMessage.IDENTIFIER);

    PGException exception =
        assertThrows(PGException.class, () -> BootstrapMessage.create(connection));
    assertEquals(SQLState.ProtocolViolation, exception.getSQLState());
    assertEquals(Severity.FATAL, exception.getSeverity());
    assertTrue(exception.getMessage().contains("invalid length of cancel request"));
  }

  @Test
  public void testCreateUnknown() throws IOException {
    // length
    dataOutputStream.writeInt(1024);
    // Identifier (this is not a valid message type).
    dataOutputStream.writeInt(9999);
    dataOutputStream.write(new byte[1024]);

    assertThrows(IllegalStateException.class, () -> BootstrapMessage.create(connection));
  }

  @Test
  public void testParseParameters_empty() {
    BootstrapMessage message = new SSLMessage(connection);
    Map<String, String> parameters = message.parseParameters("");
    assertNotNull(parameters);
    assertTrue(parameters.isEmpty());

    parameters = message.parseParameters(null);
    assertNotNull(parameters);
    assertTrue(parameters.isEmpty());
  }

  @Test
  public void testParseParameters_oddTokens() {
    BootstrapMessage message = new SSLMessage(connection);
    Map<String, String> parameters =
        message.parseParameters("user\0postgres\0oddTokenWithoutValue");
    assertNotNull(parameters);
    assertEquals(1, parameters.size());
    assertEquals("postgres", parameters.get("user"));
  }

  @Test
  public void testParseParameterKeys_empty() {
    BootstrapMessage message = new SSLMessage(connection);
    List<String> keys = message.parseParameterKeys("");
    assertNotNull(keys);
    assertTrue(keys.isEmpty());

    keys = message.parseParameterKeys(null);
    assertNotNull(keys);
    assertTrue(keys.isEmpty());
  }

  @Test
  public void testParseParameterKeys_oddTokens() {
    BootstrapMessage message = new SSLMessage(connection);
    List<String> keys = message.parseParameterKeys("user\0postgres\0oddTokenWithoutValue");
    assertNotNull(keys);
    assertEquals(1, keys.size());
    assertEquals("user", keys.get(0));
  }

  @Test
  public void testParseParameters_emptyValue() {
    BootstrapMessage message = new SSLMessage(connection);
    Map<String, String> parameters =
        message.parseParameters("user\0postgres\0application_name\0\0");
    assertNotNull(parameters);
    assertEquals(2, parameters.size());
    assertEquals("postgres", parameters.get("user"));
    assertEquals("", parameters.get("application_name"));
  }

  @Test
  public void testParseParameterKeys_emptyValue() {
    BootstrapMessage message = new SSLMessage(connection);
    List<String> keys = message.parseParameterKeys("user\0postgres\0application_name\0\0");
    assertNotNull(keys);
    assertEquals(2, keys.size());
    assertEquals("user", keys.get(0));
    assertEquals("application_name", keys.get(1));
  }

  @Test
  public void testParseParameters_emptyKey() {
    BootstrapMessage message = new SSLMessage(connection);
    Map<String, String> parameters = message.parseParameters("\0someValue\0user\0postgres\0");
    assertNotNull(parameters);
    assertEquals(1, parameters.size());
    assertEquals("postgres", parameters.get("user"));
  }

  @Test
  public void testParseParameterKeys_emptyKey() {
    BootstrapMessage message = new SSLMessage(connection);
    List<String> keys = message.parseParameterKeys("\0someValue\0user\0postgres\0");
    assertNotNull(keys);
    assertEquals(1, keys.size());
    assertEquals("user", keys.get(0));
  }
}
