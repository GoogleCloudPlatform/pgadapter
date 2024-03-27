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

import com.google.cloud.spanner.connection.SpannerPool;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.metadata.ByteBufferInputStream;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.wireprotocol.BootstrapMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage;
import io.opentelemetry.api.OpenTelemetry;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.newsclub.net.unix.AFUNIXSelectorProvider;
import org.newsclub.net.unix.AFUNIXServerSocketChannel;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.postgresql.util.ByteConverter;

public class NonBlockingProxyServer extends ProxyServer {
  private static final Logger logger = Logger.getLogger(NonBlockingProxyServer.class.getName());

  private final Selector acceptSelector;

  private final Selector udsAcceptSelector;

  private final Selector selector;

  private final Selector udsSelector;

  private final ServerSocketChannel serverSocketChannel;

  private final ServerSocketChannel udsServerSocketChannel;

  private Thread tcpListenerThread;

  private Thread tcpReaderThread;

  private Thread udsListenerThread;

  private Thread udsReaderThread;

  public NonBlockingProxyServer(OptionsMetadata optionsMetadata, OpenTelemetry openTelemetry)
      throws IOException {
    this(optionsMetadata, openTelemetry, new Properties());
  }

  public NonBlockingProxyServer(
      OptionsMetadata optionsMetadata, OpenTelemetry openTelemetry, Properties properties)
      throws IOException {
    super(optionsMetadata, openTelemetry, properties);
    this.acceptSelector = Selector.open();
    this.udsAcceptSelector = AFUNIXSelectorProvider.provider().openSelector();
    this.selector = Selector.open();
    this.udsSelector = AFUNIXSelectorProvider.provider().openSelector();

    this.serverSocketChannel = ServerSocketChannel.open();
    this.serverSocketChannel.configureBlocking(false);
    this.serverSocketChannel.register(
        this.acceptSelector, this.serverSocketChannel.validOps(), null);
    ServerSocket socket = this.serverSocketChannel.socket();

    int port = getLocalPort() == 0 ? getOptions().getProxyPort() : getLocalPort();
    socket.bind(
        new InetSocketAddress(InetAddress.getLoopbackAddress(), port),
        getOptions().getMaxBacklog());
    this.localPort = socket.getLocalPort();

    File tempDir = new File(optionsMetadata.getSocketFile(localPort));
    if (tempDir.getParentFile() != null && !tempDir.getParentFile().exists()) {
      tempDir.mkdirs();
    }
    this.udsServerSocketChannel = AFUNIXServerSocketChannel.open();
    this.udsServerSocketChannel.configureBlocking(false);
    this.udsServerSocketChannel.register(
        this.udsAcceptSelector, this.udsServerSocketChannel.validOps(), null);
    ServerSocket udsSocket = this.udsServerSocketChannel.socket();
    udsSocket.bind(AFUNIXSocketAddress.of(tempDir), getOptions().getMaxBacklog());
  }

  @Override
  protected void doStart() {
    try {
      tcpListenerThread =
          new Thread("spanner-postgres-adapter-proxy-listener") {
            @Override
            public void run() {
              try {
                runListeningServer(
                    NonBlockingProxyServer.this.acceptSelector,
                    NonBlockingProxyServer.this.selector,
                    NonBlockingProxyServer.this.serverSocketChannel);
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
      tcpListenerThread.start();
      tcpReaderThread =
          new Thread("spanner-postgres-adapter-reader") {
            @Override
            public void run() {
              try {
                runServer(NonBlockingProxyServer.this.selector);
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
      tcpReaderThread.start();

      udsListenerThread =
          new Thread("spanner-postgres-adapter-proxy-listener") {
            @Override
            public void run() {
              try {
                runListeningServer(
                    NonBlockingProxyServer.this.udsAcceptSelector,
                    NonBlockingProxyServer.this.udsSelector,
                    NonBlockingProxyServer.this.udsServerSocketChannel);
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
      udsListenerThread.start();
      udsReaderThread =
          new Thread("spanner-postgres-adapter-reader") {
            @Override
            public void run() {
              try {
                runServer(NonBlockingProxyServer.this.udsSelector);
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
      udsReaderThread.start();

      notifyStarted();
    } catch (Throwable throwable) {
      notifyFailed(throwable);
    }
  }

  @Override
  protected void doStop() {
    try {
      serverSocketChannel.close();
      udsServerSocketChannel.close();
      acceptSelector.close();
      udsAcceptSelector.close();
    } catch (IOException ioException) {

    }
    for (ConnectionHandler handler : getConnectionHandlers()) {
      handler.terminate();
    }
    try {
      SpannerPool.closeSpannerPool();
    } catch (Throwable ignore) {
    }
    notifyStopped();
  }

  void runListeningServer(
      Selector acceptSelector, Selector selector, ServerSocketChannel serverSocketChannel)
      throws IOException {
    while (true) {
      try {
        if (acceptSelector.selectNow() > 0) {
          Set<SelectionKey> keys = acceptSelector.selectedKeys();
          for (SelectionKey key : keys) {
            if (key.isAcceptable()) {
              handleAccept(selector, serverSocketChannel);
            }
          }
          keys.clear();
        }
      } catch (ClosedSelectorException ignore) {
        // the server is shutting down.
        break;
      } catch (Throwable unexpectedError) {
        logger.warning(
            "Unexpected error while listening for incoming connections: "
                + unexpectedError.getMessage());
      }
    }
  }

  void handleAccept(Selector selector, ServerSocketChannel serverSocketChannel) throws IOException {
    SocketChannel channel = serverSocketChannel.accept();
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

  void runServer(Selector selector) throws IOException {
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
          } catch (EOFException eofException) {
            key.cancel();
            key.channel().close();
          } catch (CancelledKeyException ignore) {
            ignore.printStackTrace();
          } catch (Throwable shouldNotHappen) {
            shouldNotHappen.printStackTrace();
          }
        }
        keys.clear();
      }
    }
  }

  static void handleRead(SelectionKey key) throws IOException {
    NonBlockingConnectionHandler handler = (NonBlockingConnectionHandler) key.attachment();
    if (handler.getStatus() == ConnectionStatus.TERMINATED) {
      throw new EOFException();
    }
    SocketChannel channel = (SocketChannel) key.channel();
    if (!(channel.isOpen() && channel.isConnected())) {
      return;
    }

    try {
      if (handler.getStatus() == ConnectionStatus.UNAUTHENTICATED) {
        ByteBuffer lengthBuffer = read(4, channel);
        lengthBuffer.rewind();
        int length = ByteConverter.int4(lengthBuffer.array(), 0);
        ByteBuffer dataBuffer = read(length, lengthBuffer, channel);
        dataBuffer.rewind();
        try (ByteBufferInputStream inputStream = new ByteBufferInputStream(dataBuffer)) {
          handler.setRawInputStream(inputStream);
          BootstrapMessage message = BootstrapMessage.create(handler);
          handler.addBootstrapMessage(message);
        }
      } else {
        // All control messages has a 1-byte type + 4 byte length.
        ByteBuffer headerBuffer = read(handler.getHeaderBuffer(), channel);
        byte[] dst = new byte[4];
        headerBuffer.position(1);
        headerBuffer.get(dst);
        int length = ByteConverter.int4(dst, 0);
        headerBuffer.rewind();
        ByteBuffer message = read(handler.getMessageBuffer(length + 1), headerBuffer, channel);
        message.rewind();
        try (ByteBufferInputStream inputStream = new ByteBufferInputStream(message)) {
          handler.setRawInputStream(inputStream);
          handler.addControlMessage(ControlMessage.create(handler));
        }
      }
    } catch (EOFException eofException) {
      throw eofException;
    } catch (ClosedChannelException ignore) {
      // ignore, this happens when the connection is closed.
    } catch (SocketException socketException) {
      throw new EOFException();
    } catch (Throwable throwable) {
      handler.handleError(
          PGException.newBuilder(throwable.getMessage())
              .setSQLState(SQLState.InternalError)
              .setCause(throwable)
              .setSeverity(Severity.FATAL)
              .build());
      if (handler.getStatus() == ConnectionStatus.UNAUTHENTICATED
          || handler.getStatus() == ConnectionStatus.AUTHENTICATING) {
        throw new EOFException();
      }
    }
  }

  private static ByteBuffer read(int length, SocketChannel channel) throws IOException {
    return read(length, null, channel);
  }

  private static ByteBuffer read(ByteBuffer destination, SocketChannel channel) throws IOException {
    return read(destination, null, channel);
  }

  private static ByteBuffer read(int length, ByteBuffer header, SocketChannel channel)
      throws IOException {
    ByteBuffer destination = ByteBuffer.allocate(length);
    return read(destination, header, channel);
  }

  private static ByteBuffer read(ByteBuffer destination, ByteBuffer header, SocketChannel channel)
      throws IOException {
    if (header != null) {
      destination.put(header);
    }
    int read;
    do {
      read = channel.read(destination);
    } while (read > -1 && destination.hasRemaining());
    if (read == -1) {
      throw new EOFException();
    }

    return destination;
  }
}
