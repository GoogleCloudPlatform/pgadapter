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

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

final class NonBlockingServerListener implements Runnable {
  private static final Logger logger = Logger.getLogger(NonBlockingServerListener.class.getName());

  private final NonBlockingProxyServer proxyServer;
  private final Selector acceptSelector;
  private final Selector readSelector;
  private final ServerSocketChannel serverSocketChannel;
  private final int index;

  private NonBlockingSocketReader reader;

  NonBlockingServerListener(
      NonBlockingProxyServer proxyServer,
      Selector acceptSelector,
      Selector readSelector,
      ServerSocketChannel serverSocketChannel,
      int index) {
    this.proxyServer = proxyServer;
    this.acceptSelector = acceptSelector;
    this.readSelector = readSelector;
    this.serverSocketChannel = serverSocketChannel;
    this.index = index;
  }

  public Selector getAcceptSelector() {
    return acceptSelector;
  }

  public ServerSocketChannel getServerSocketChannel() {
    return serverSocketChannel;
  }

  @Override
  public void run() {
    Instant lastConnection = Instant.now();
    while (true) {
      try {
        acceptSelector.select(1000L);
        Set<SelectionKey> keys = acceptSelector.selectedKeys();
        for (SelectionKey key : keys) {
          if (key.isAcceptable()) {
            lastConnection = Instant.now();
            handleAccept(readSelector, serverSocketChannel);
          }
        }
        keys.clear();
        long secondsSinceLastConnection = ChronoUnit.SECONDS.between(Instant.now(), lastConnection);
        if (secondsSinceLastConnection > 0L && secondsSinceLastConnection % 10L == 0L) {
          logger.log(Level.WARNING, "Seconds since last connection: " + secondsSinceLastConnection);
        }
      } catch (ClosedSelectorException ignore) {
        // the server is shutting down.
        logger.log(Level.INFO, "Listener shutting down");
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
      if (this.reader == null) {
        this.reader = new NonBlockingSocketReader(this.proxyServer, this.readSelector);
        Thread readerThread =
            new Thread(this.reader, "spanner-postgres-adapter-proxy-reader-" + this.index);
        readerThread.start();
      }

      channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
      channel.configureBlocking(false);
      NonBlockingConnectionHandler handler = new NonBlockingConnectionHandler(proxyServer, channel);
      proxyServer.register(handler);
      Thread thread = proxyServer.getThreadFactory().newThread(handler);
      handler.setThread(thread);
      handler.start();

      channel.register(selector, SelectionKey.OP_READ, handler);
    }
  }
}
