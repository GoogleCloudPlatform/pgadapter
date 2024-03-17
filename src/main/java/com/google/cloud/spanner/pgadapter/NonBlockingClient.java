package com.google.cloud.spanner.pgadapter;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.newsclub.net.unix.AFUNIXSocketChannel;

public class NonBlockingClient implements AutoCloseable {

  public static void main(String[] args) throws IOException {
    try (NonBlockingClient client = new NonBlockingClient()) {
      client.runClient();
    } catch (InterruptedException ignore) {
      // Just finish
    }
  }

  private final SocketChannel channel;

  private NonBlockingClient() throws IOException {
    // this.channel = SocketChannel.open(new InetSocketAddress(InetAddress.getLoopbackAddress(), 5433));
    this.channel = AFUNIXSocketChannel.open(AFUNIXSocketAddress.of(new File("/Users/loite/latency_test.tmp")));
    this.channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
    this.channel.finishConnect();
  }

  @Override
  public void close() throws IOException {
    if (this.channel != null && this.channel.isOpen()) {
      this.channel.close();
    }
  }

  void runClient() throws IOException, InterruptedException {
    while (true) {
      //noinspection BusyWait
      Thread.sleep(ThreadLocalRandom.current().nextInt(1000));
      String msg = String.valueOf(System.nanoTime());
      this.channel.write(ByteBuffer.wrap(msg.getBytes(StandardCharsets.UTF_8)));
      System.out.println(msg);
    }
  }

}
