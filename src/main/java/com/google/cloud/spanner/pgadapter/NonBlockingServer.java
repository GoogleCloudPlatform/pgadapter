package com.google.cloud.spanner.pgadapter;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.newsclub.net.unix.AFUNIXSelectorProvider;
import org.newsclub.net.unix.AFUNIXServerSocketChannel;
import org.newsclub.net.unix.AFUNIXSocketAddress;

public class NonBlockingServer {

  public static void main(String[] args) throws IOException {
    NonBlockingServer server = new NonBlockingServer();
    Thread acceptThread = new Thread("accept-thread") {
      @Override
      public void run() {
        try {
          server.runListeningServer();
        } catch (IOException ioException) {
          //noinspection CallToPrintStackTrace
          ioException.printStackTrace();
        }
      }
    };
    acceptThread.start();
    server.runServer();
  }

  private final Selector acceptSelector;

  private final Selector selector;

  private final ServerSocketChannel serverSocketChannel;

  private NonBlockingServer() throws IOException {
    // System.setProperty("java.nio.channels.spi.SelectorProvider", "sun.nio.ch.PollSelectorProvider");
    // this.acceptSelector = Selector.open();
    this.acceptSelector = AFUNIXSelectorProvider.provider().openSelector();
    // this.selector = Selector.open();
    this.selector = AFUNIXSelectorProvider.provider().openSelector();
    this.serverSocketChannel = AFUNIXServerSocketChannel.open();
    // this.serverSocketChannel = ServerSocketChannel.open();
    this.serverSocketChannel.configureBlocking(false);
    this.serverSocketChannel.register(this.acceptSelector, this.serverSocketChannel.validOps(), null);
    ServerSocket socket = this.serverSocketChannel.socket();

    File file = new File("/Users/loite/latency_test.tmp");
    socket.bind(AFUNIXSocketAddress.of(file));
    // socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 5433), 1000);
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

  void runServer() throws IOException {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
    //noinspection InfiniteLoopStatement
    while (true) {
      if (selector.selectNow() > 0) {
        Set<SelectionKey> keys = selector.selectedKeys();
        for (SelectionKey key : keys) {
          if (key.isReadable()) {
            handleRead(key);
          } else if (key.isWritable()) {
            handleWrite(key);
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
      channel.register(selector, SelectionKey.OP_READ);
      System.out.println("Connected");
    }
  }

  void handleRead(SelectionKey key) throws IOException {
    long beforeReadTime = System.nanoTime();
    SocketChannel channel = (SocketChannel) key.channel();
    ByteBuffer buffer = ByteBuffer.allocate(128);
    int length = channel.read(buffer);
    byte[] bytes = new byte[length];
    System.arraycopy(buffer.array(), 0, bytes, 0, length);
    String msg = new String(bytes, StandardCharsets.UTF_8);
    if (msg.contains("ping")) {
      System.out.println("Received ping");
    } else {
      long time = Long.parseLong(msg);
      long latency = System.nanoTime() - time;
      long beforeReadLatency = beforeReadTime - time;
      System.out.println(
          "Latency: " + TimeUnit.MICROSECONDS.convert(latency, TimeUnit.NANOSECONDS) + "u");
      System.out.println("Before read latency: " + TimeUnit.MICROSECONDS.convert(beforeReadLatency,
          TimeUnit.NANOSECONDS) + "u");
    }
  }

  void handleWrite(SelectionKey key) {

  }

}
