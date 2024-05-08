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
import com.google.cloud.spanner.pgadapter.metadata.ForwardingInputStream;
import com.google.cloud.spanner.pgadapter.wireprotocol.BootstrapMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.FlushMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.SyncMessage;
import java.io.IOException;
import java.io.InputStream;
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

  private final ForwardingInputStream forwardingInputStream = new ForwardingInputStream();

  private final SocketChannel channel;

  NonBlockingConnectionHandler(ProxyServer server, SocketChannel channel) {
    super(server, channel.socket());
    this.channel = channel;
    setConnectionMetadata(
        new ConnectionMetadata(forwardingInputStream, new ChannelOutputStream(channel)));
  }

  @Override
  void createSSLSocket() throws IOException {
    throw new IOException("SSL is not supported for non-blocking connection handlers");
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

  void setRawInputStream(InputStream inputStream) {
    this.forwardingInputStream.setDelegate(inputStream);
  }

  @Override
  public boolean supportsPeekNextByte() {
    return false;
  }

  @Override
  protected ConnectionMetadata createConnectionMetadata() {
    return getConnectionMetadata();
  }

  @Override
  protected void closeSocket() throws IOException {
    try {
      this.channel.close();
    } catch (IOException ioException) {
      ioException.printStackTrace();
      throw ioException;
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  void addBootstrapMessage(BootstrapMessage bootstrapMessage) {
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
    if (getStatus() == ConnectionStatus.COPY_IN) {
      while (true) {
        ControlMessage message = this.controlMessages.take();
        if (message instanceof FlushMessage || message instanceof SyncMessage) {
          continue;
        }
        return message;
      }
    } else {
      return this.controlMessages.take();
    }
  }
}
