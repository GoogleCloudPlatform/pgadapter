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
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import io.opentelemetry.api.OpenTelemetry;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.newsclub.net.unix.AFUNIXSelectorProvider;
import org.newsclub.net.unix.AFUNIXServerSocketChannel;
import org.newsclub.net.unix.AFUNIXSocketAddress;

public class NonBlockingProxyServer extends ProxyServer {
  private static final Logger logger = Logger.getLogger(NonBlockingProxyServer.class.getName());

  private final ImmutableList<NonBlockingServerListener> serverListeners;

  public NonBlockingProxyServer(OptionsMetadata optionsMetadata, OpenTelemetry openTelemetry)
      throws IOException {
    this(optionsMetadata, openTelemetry, new Properties());
  }

  public NonBlockingProxyServer(
      OptionsMetadata optionsMetadata, OpenTelemetry openTelemetry, Properties properties)
      throws IOException {
    super(optionsMetadata, openTelemetry, properties);
    int index = 0;
    ImmutableList.Builder<NonBlockingServerListener> listenersBuilder = ImmutableList.builder();
    NonBlockingServerListener tcpListener =
        createServerListener(
            Selector.open(),
            Selector.open(),
            ServerSocketChannel.open(),
            new InetSocketAddress((InetAddress) null, getLocalPort()),
            ++index);
    if (getLocalPort() == 0) {
      this.localPort = tcpListener.getServerSocketChannel().socket().getLocalPort();
    }
    listenersBuilder.add(tcpListener);

    if (optionsMetadata.isDomainSocketEnabled()) {
      File tempDir = new File(optionsMetadata.getSocketFile(getLocalPort()));
      if (tempDir.getParentFile() != null && !tempDir.getParentFile().exists()) {
        //noinspection ResultOfMethodCallIgnored
        tempDir.mkdirs();
      }
      NonBlockingServerListener listener =
          createServerListener(
              AFUNIXSelectorProvider.provider().openSelector(),
              AFUNIXSelectorProvider.provider().openSelector(),
              AFUNIXServerSocketChannel.open(),
              AFUNIXSocketAddress.of(tempDir),
              ++index);
      listenersBuilder.add(listener);
    }
    this.serverListeners = listenersBuilder.build();
  }

  private NonBlockingServerListener createServerListener(
      Selector acceptSelector,
      Selector readSelector,
      ServerSocketChannel serverSocketChannel,
      InetSocketAddress address,
      int index)
      throws IOException {
    serverSocketChannel.configureBlocking(false);
    serverSocketChannel.register(acceptSelector, serverSocketChannel.validOps(), null);
    ServerSocket socket = serverSocketChannel.socket();
    socket.bind(address, getOptions().getMaxBacklog());
    if (getLocalPort() == 0) {
      this.localPort = socket.getLocalPort();
    }
    return new NonBlockingServerListener(
        this, acceptSelector, readSelector, serverSocketChannel, index);
  }

  @Override
  protected void doStart() {
    try {
      int index = 0;
      for (NonBlockingServerListener listener : serverListeners) {
        Thread listenerThread =
            new Thread(listener, "spanner-postgres-adapter-proxy-listener-" + (++index));
        listenerThread.start();
      }
      notifyStarted();
      logger.log(Level.INFO, "Non-blocking server started");
    } catch (Throwable throwable) {
      logger.log(Level.WARNING, "Non-blocking server failed to start", throwable);
      notifyFailed(throwable);
    }
  }

  @Override
  protected void doStop() {
    for (NonBlockingServerListener listener : serverListeners) {
      try {
        listener.getAcceptSelector().close();
        logger.log(Level.INFO, "Closed listening selector {}", listener.getAcceptSelector());
      } catch (IOException ioException) {
        logger.log(Level.WARNING, "Failed to close selector", ioException);
      }
    }
    for (ConnectionHandler handler : getConnectionHandlers()) {
      handler.terminate();
    }
    logger.log(Level.INFO, "Terminated all active connections");
    try {
      SpannerPool.closeSpannerPool();
    } catch (Throwable ignore) {
    }
    for (NonBlockingServerListener listener : serverListeners) {
      try {
        listener.getServerSocketChannel().close();
        logger.log(Level.INFO, "Closed listening socket {}", listener.getServerSocketChannel());
      } catch (IOException ioException) {
        logger.log(Level.WARNING, "Failed to close socket", ioException);
      }
    }
    notifyStopped();
    logger.log(Level.INFO, "Non-blocking listening server stopped");
  }
}
