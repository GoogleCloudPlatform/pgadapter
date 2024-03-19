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

import com.google.cloud.spanner.pgadapter.metadata.ChannelOutputStream;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.wireprotocol.BootstrapMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class NonBlockingConnectionHandler extends ConnectionHandler {
  private static final int DEFAULT_BUFFER_CAPACITY = 1 << 13;

  private static final Logger logger =
      Logger.getLogger(NonBlockingConnectionHandler.class.getName());

  private final ByteBuffer headerBuffer = ByteBuffer.allocateDirect(5);

  private ByteBuffer messageBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_CAPACITY);

  private final BlockingQueue<BootstrapMessage> bootstrapMessages = new LinkedBlockingQueue<>();

  private final BlockingQueue<ControlMessage> controlMessages = new LinkedBlockingQueue<>();

  private final PipedInputStream connectionInputStream = new PipedInputStream();

  private final PipedOutputStream connectionInputStreamBuffer = new PipedOutputStream();

  private final SocketChannel channel;

  NonBlockingConnectionHandler(ProxyServer server, SocketChannel channel) throws IOException {
    super(server, channel.socket());
    this.channel = channel;
    this.connectionInputStream.connect(connectionInputStreamBuffer);
    this.connectionMetadata =
        new ConnectionMetadata(connectionInputStream, new ChannelOutputStream(channel));
  }

  ByteBuffer getHeaderBuffer() {
    this.headerBuffer.rewind();
    return this.headerBuffer;
  }

  ByteBuffer getMessageBuffer(int length) {
    if (this.messageBuffer.capacity() < length) {
      this.messageBuffer = ByteBuffer.allocateDirect(length);
    } else {
      this.messageBuffer.rewind();
      this.messageBuffer.limit(length);
    }
    return this.messageBuffer;
  }

  PipedOutputStream getConnectionInputStreamBuffer() {
    return this.connectionInputStreamBuffer;
  }

  @Override
  protected ConnectionMetadata createConnectionMetadata() {
    return this.connectionMetadata;
  }

  @Override
  protected void closeSocket() throws IOException {
    System.out.println("Closing channel");
    try {
      this.channel.close();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  void addBootstrapMessage(BootstrapMessage bootstrapMessage) {
    System.out.println("Adding bootstrap message " + bootstrapMessage);
    this.bootstrapMessages.add(bootstrapMessage);
  }

  @Override
  public BootstrapMessage readBootstrapMessage() throws Exception {
    return this.bootstrapMessages.take();
  }

  void addControlMessage(ControlMessage controlMessage) {
    this.controlMessages.add(controlMessage);
  }

  @Override
  public ControlMessage readControlMessage() throws Exception {
    return this.controlMessages.take();
  }
}
