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

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BoostrapMessageTest {

  @Test
  public void testCreate() throws Exception {
    ConnectionHandler connection = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    when(connection.getServer()).thenReturn(server);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(server.getOptions()).thenReturn(options);
    ConnectionMetadata metadata = mock(ConnectionMetadata.class);
    when(connection.getConnectionMetadata()).thenReturn(metadata);
    PipedInputStream pipedInputStream = new PipedInputStream(2048);
    DataInputStream dataInputStream = new DataInputStream(pipedInputStream);
    when(metadata.getInputStream()).thenReturn(dataInputStream);
    DataOutputStream dataOutputStream =
        new DataOutputStream(new PipedOutputStream(pipedInputStream));
    // length
    dataOutputStream.writeInt(1024);
    // identifier
    dataOutputStream.writeInt(StartupMessage.IDENTIFIER);
    dataOutputStream.write(new byte[1024]);

    BootstrapMessage message = BootstrapMessage.create(connection);
    assertEquals(StartupMessage.class, message.getClass());
    assertEquals(message.length, 1024);
  }
}
