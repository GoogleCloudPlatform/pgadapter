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

import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.metadata.ByteBufferInputStream;
import com.google.cloud.spanner.pgadapter.wireprotocol.BootstrapMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.util.ByteConverter;

class NonBlockingSocketReader implements Runnable {
  private static final Logger logger = Logger.getLogger(NonBlockingSocketReader.class.getName());

  private final NonBlockingProxyServer proxyServer;

  private final Selector selector;

  private final AtomicBoolean running = new AtomicBoolean(true);

  NonBlockingSocketReader(NonBlockingProxyServer proxyServer, Selector selector) {
    this.proxyServer = proxyServer;
    this.selector = selector;
    proxyServer.addListener(
        new Listener() {
          @Override
          public void failed(State from, Throwable failure) {
            super.failed(from, failure);
            running.set(false);
          }

          @Override
          public void terminated(State from) {
            super.terminated(from);
            running.set(false);
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public void run() {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
    try {
      Instant lastReadTime = Instant.now();
      while (running.get()) {
        if (selector.selectNow() > 0) {
          lastReadTime = Instant.now();
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
              // Ignore and try the next
            } catch (Throwable shouldNotHappen) {
              logger.log(Level.WARNING, "Socket read failed", shouldNotHappen);
            }
          }
          keys.clear();
        } else {
          long secondsSinceLastRead = ChronoUnit.SECONDS.between(Instant.now(), lastReadTime);
          if (secondsSinceLastRead > 0L && secondsSinceLastRead % 10L == 0L) {
            System.out.printf("Seconds since last read: %d\n", secondsSinceLastRead);
          }
        }
      }
    } catch (IOException ioException) {
      logger.log(Level.WARNING, "selectNow for reader failed", ioException);
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
        ByteBuffer lengthBuffer = read(4, channel, true);
        lengthBuffer.rewind();
        int length = ByteConverter.int4(lengthBuffer.array(), 0);
        ByteBuffer dataBuffer = read(length, lengthBuffer, channel, true);
        dataBuffer.rewind();
        try (ByteBufferInputStream inputStream = new ByteBufferInputStream(dataBuffer)) {
          handler.setRawInputStream(inputStream);
          BootstrapMessage message = BootstrapMessage.create(handler);
          handler.addBootstrapMessage(message);
        }
      } else {
        // All control messages has a 1-byte type + 4 byte length.
        ByteBuffer headerBuffer = read(handler.getHeaderBuffer(), channel, false);
        byte[] dst = new byte[4];
        headerBuffer.position(1);
        headerBuffer.get(dst);
        int length = ByteConverter.int4(dst, 0);
        headerBuffer.rewind();
        ByteBuffer message =
            read(handler.getMessageBuffer(length + 1), headerBuffer, channel, false);
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

  private static ByteBuffer read(int length, SocketChannel channel, boolean bootstrap)
      throws IOException {
    return read(length, null, channel, bootstrap);
  }

  private static ByteBuffer read(ByteBuffer destination, SocketChannel channel, boolean bootstrap)
      throws IOException {
    return read(destination, null, channel, bootstrap);
  }

  private static ByteBuffer read(
      int length, ByteBuffer header, SocketChannel channel, boolean bootstrap) throws IOException {
    ByteBuffer destination = ByteBuffer.allocate(length);
    return read(destination, header, channel, bootstrap);
  }

  private static ByteBuffer read(
      ByteBuffer destination, ByteBuffer header, SocketChannel channel, boolean bootstrap)
      throws IOException {
    if (header != null) {
      destination.put(header);
    }
    boolean loggedWarning = false;
    int read, zeroBytesCounter = 0;
    do {
      read = channel.read(destination);
      if (read == 0) {
        zeroBytesCounter++;
        if (zeroBytesCounter % 1000 == 0) {
          System.out.println("Read zero bytes " + zeroBytesCounter + " times");
          System.out.println("Expecting " + destination.capacity() + " bytes");
          System.out.println("Remaining " + destination.remaining() + " bytes");
          loggedWarning = true;
        }
      }
    } while (read > -1 && destination.hasRemaining());
    if (loggedWarning) {
      System.out.println("Finished reading");
    }
    if (read == -1) {
      System.out.println("EOFException after warning");
      throw new EOFException();
    }

    return destination;
  }
}
