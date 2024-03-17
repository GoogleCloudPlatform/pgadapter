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

import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.wireprotocol.BootstrapMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.util.ByteConverter;

public class NonBlockingProxyServer extends ProxyServer {
  private static final Logger logger = Logger.getLogger(NonBlockingProxyServer.class.getName());

  private final Selector acceptSelector;

  private final Selector selector;

  private final ServerSocketChannel serverSocketChannel;

  public NonBlockingProxyServer(
      OptionsMetadata optionsMetadata,
      OpenTelemetry openTelemetry) throws IOException {
    this(optionsMetadata, openTelemetry, new Properties());
  }

  public NonBlockingProxyServer(
      OptionsMetadata optionsMetadata,
      OpenTelemetry openTelemetry, Properties properties) throws IOException {
    super(optionsMetadata, openTelemetry, properties);
    this.acceptSelector = Selector.open();
    // this.acceptSelector = AFUNIXSelectorProvider.provider().openSelector();
    this.selector = Selector.open();
    // this.selector = AFUNIXSelectorProvider.provider().openSelector();
    // this.serverSocketChannel = AFUNIXServerSocketChannel.open();
    this.serverSocketChannel = ServerSocketChannel.open();
    this.serverSocketChannel.configureBlocking(false);
    this.serverSocketChannel.register(this.acceptSelector, this.serverSocketChannel.validOps(), null);
    ServerSocket socket = this.serverSocketChannel.socket();

    // File file = new File("/Users/loite/latency_test.tmp");
    // socket.bind(AFUNIXSocketAddress.of(file));
    int port = getLocalPort() == 0 ? getOptions().getProxyPort() : getLocalPort();
    socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), getOptions().getMaxBacklog());
    this.localPort = socket.getLocalPort();
  }

  @Override
  protected void doStart() {
    try {
      Thread listenerThread = new Thread("spanner-postgres-adapter-proxy-listener") {
        @Override
        public void run() {
          try {
            runListeningServer();
          } catch (Exception exception) {
            logger.log(
                Level.WARNING,
                exception,
                () ->
                    String.format(
                        "Server on port %s stopped by exception: %s",
                        getLocalPort(), exception));
          }
        }
      };
      listenerThread.start();
      Thread readerThread = new Thread("spanner-postgres-adapter-reader") {
        @Override
        public void run() {
          try {
            runServer();
          } catch (Exception exception) {
            logger.log(
                Level.WARNING,
                exception,
                () ->
                    String.format(
                        "Server on port %s stopped by exception: %s",
                        getLocalPort(), exception));
          }
        }
      };
      readerThread.start();
      notifyStarted();
    } catch (Throwable throwable) {
      notifyFailed(throwable);
    }
  }

  void runListeningServer() throws IOException {
    //noinspection InfiniteLoopStatement
    while (true) {
      if (acceptSelector.selectNow() > 0) {
        Set<SelectionKey> keys = acceptSelector.selectedKeys();
        for (SelectionKey key : keys) {
          if (key.isAcceptable()) {
            handleAccept();
          }
        }
        keys.clear();
      }
    }
  }

  void handleAccept() throws IOException {
    SocketChannel channel = this.serverSocketChannel.accept();
    if (channel != null) {
      channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
      channel.configureBlocking(false);
      NonBlockingConnectionHandler handler = new NonBlockingConnectionHandler(this, channel);
      register(handler);
      Thread thread = threadFactory.newThread(handler);
      handler.setThread(thread);
      handler.start();

      channel.register(selector, SelectionKey.OP_READ, handler);
    }
  }

  void runServer() throws IOException {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
    //noinspection InfiniteLoopStatement
    while (true) {
      if (selector.selectNow() > 0) {
        Set<SelectionKey> keys = selector.selectedKeys();
        for (SelectionKey key : keys) {
          try {
            if (key.isReadable()) {
              handleRead(key);
            }
          } catch (CancelledKeyException ignore) {

          }
        }
        keys.clear();
      }
    }
  }

  void handleRead(SelectionKey key) {
    NonBlockingConnectionHandler handler = (NonBlockingConnectionHandler) key.attachment();
    SocketChannel channel = (SocketChannel) key.channel();
    if (!(channel.isOpen() && channel.isConnected())) {
      return;
    }

    try {
      if (handler.getStatus() == ConnectionStatus.UNAUTHENTICATED) {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        do {
          channel.read(lengthBuffer);
        } while (lengthBuffer.hasRemaining());
        int length = ByteConverter.int4(lengthBuffer.array(), 0);
        ByteBuffer dataBuffer = ByteBuffer.allocate(length);
        dataBuffer.put(lengthBuffer.array());
        do {
          channel.read(dataBuffer);
        } while (dataBuffer.hasRemaining());
        handler.getConnectionInputStreamBuffer().write(dataBuffer.array());
        handler.addBootstrapMessage(BootstrapMessage.create(handler));
      } else {
        // All control messages has a 1-byte type + 4 byte length.
        ByteBuffer headerBuffer = ByteBuffer.allocate(5);
        do {
          channel.read(headerBuffer);
        } while (headerBuffer.hasRemaining());
        int length = ByteConverter.int4(headerBuffer.array(), 1);
        ByteBuffer dataBuffer = ByteBuffer.allocate(length + 1);
        dataBuffer.put(headerBuffer.array());
        do {
          channel.read(dataBuffer);
        } while (dataBuffer.hasRemaining());
        handler.getConnectionInputStreamBuffer().write(dataBuffer.array());
        handler.addControlMessage(ControlMessage.create(handler));
      }
    } catch (ClosedChannelException ignore) {
      // ignore, this happens when the connection is closed.
    } catch (Exception exception) {
      exception.printStackTrace();
    }
  }

}
