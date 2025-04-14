package com.google.cloud.spanner.pgadapter;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SocketChannelTest {

  public static void main(String[] args) throws Exception {
    ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
    //serverSocketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
    InetSocketAddress address = new InetSocketAddress("localhost", 9011);
    serverSocketChannel.bind(address);
    serverSocketChannel.configureBlocking(true);

//    ServerSocket serverSocket = new ServerSocket(0, 1000, InetAddress.getLocalHost());
//    // Optimize for latency (2), then bandwidth (1) and then connection time (0).
//    serverSocket.setPerformancePreferences(0, 2, 1);

    Thread listenerThread = new Thread(() -> {
      int index = 0;
      while (true) {
        try {
          //Socket socket = serverSocket.accept();
          System.out.println("Listening...");
          SocketChannel socket = serverSocketChannel.accept();
          System.out.println("Accepted connection from " + socket.socket().getInetAddress());
          ServerChannel server = new ServerChannel(socket);
          Thread serverThread = new Thread(server, String.format("server-thread-%d", ++index));
          serverThread.start();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, "listener-thread");
    listenerThread.start();

    Thread.sleep(500);
    System.out.println("Connecting...");
    Socket socket = new Socket(InetAddress.getByName("localhost"), 9011);
    Client client = new Client(socket);
    Thread clientThread = new Thread(client, "client-thread");
    clientThread.start();
  }

  static class Server implements Runnable {
    static final int BUFFER_SIZE = 1 << 16;

    final Socket socket;
    final DataInputStream input;
    final DataOutputStream output;

    Server(Socket socket) throws IOException {
      this.socket = socket;
      this.input = new DataInputStream(new BufferedInputStream(socket.getInputStream(), BUFFER_SIZE));
      this.output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), BUFFER_SIZE));
    }

    @Override
    public void run() {
      while (true) {
        try {
          byte identifier = this.input.readByte();
          int length = this.input.readInt();
          String message = readString(input);
          String timestamp = readString(input);
          long nanos = Long.parseLong(timestamp);
          long duration = System.nanoTime() - nanos;
          System.out.println(identifier + " " + length + " " + message + " " + nanos);
          System.out.println("Duration: " + duration);
        } catch (IOException exception) {
          exception.printStackTrace();
        }
      }
    }
  }

  static class ServerChannel implements Runnable {
    static final int BUFFER_SIZE = 1 << 16;

    final SocketChannel channel;
    final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

    ServerChannel(SocketChannel channel) throws IOException {
      this.channel = channel;
    }

    @Override
    public void run() {
      while (true) {
        try {
          while (channel.socket().getInputStream().available() == 0) {
            Thread.yield();
          }
          this.buffer.position(0);
          this.buffer.limit(5);
          this.channel.read(this.buffer);
          this.buffer.position(0);
          byte identifier = this.buffer.get();
          int length = this.buffer.getInt();
          this.buffer.position(0);
          this.buffer.limit(length - 4);
          this.channel.read(this.buffer);
          this.buffer.position(0);

          byte[] array = new byte[this.buffer.limit()];
          this.buffer.get(array);
          this.buffer.position(0);

          ByteBuffer slice1 = this.buffer.slice();
          int startPos = this.buffer.position();
          while (this.buffer.hasRemaining() && this.buffer.get() != 0) {}
          slice1.limit(this.buffer.position() - startPos - 1);
          CharBuffer message = StandardCharsets.UTF_8.decode(slice1);

          ByteBuffer slice2 = this.buffer.slice();
          startPos = this.buffer.position();
          while (this.buffer.hasRemaining() && this.buffer.get() != 0) {}
          slice2.limit(this.buffer.position() - startPos - 1);
          CharBuffer timestamp = StandardCharsets.UTF_8.decode(slice2);

          long nanos = Long.parseLong(timestamp.toString());
          long duration = System.nanoTime() - nanos;
          System.out.println(identifier + " " + length + " " + message + " " + nanos);
          System.out.println("Duration: " + duration);
        } catch (IOException exception) {
          exception.printStackTrace();
        }
      }
    }
  }

  static class Client implements Runnable {
    final Socket socket;
    final DataInputStream input;
    final DataOutputStream output;

    Client(Socket socket) throws IOException {
      this.socket = socket;
      this.input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
      this.output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    @Override
    public void run() {
      try {
        while (true) {
          int sleep = ThreadLocalRandom.current().nextInt(200);
          Thread.sleep(sleep);
          String message = String.valueOf(ThreadLocalRandom.current().nextLong());
          String nanos = String.valueOf(System.nanoTime());
          byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
          byte[] timestampBytes = nanos.getBytes(StandardCharsets.UTF_8);
          int length = 4 + messageBytes.length + 1 + timestampBytes.length + 1;
          this.output.writeByte('Q');
          this.output.writeInt(length);
          this.output.write(messageBytes);
          this.output.writeByte(0);
          this.output.write(timestampBytes);
          this.output.writeByte(0);
          this.output.flush();
        }
      } catch (InterruptedException | IOException exception) {
        exception.printStackTrace();
      }
    }
  }

  private static final int MARK_READ_LIMIT = 100_000_000;

  static String readString(DataInputStream inputStream) throws IOException {
    inputStream.mark(MARK_READ_LIMIT);
    try {
      int index = 0;
      while (index < MARK_READ_LIMIT) {
        byte b = inputStream.readByte();
        if (b == 0) {
          break;
        }
        index++;
        if (index == MARK_READ_LIMIT) {
          throw new IOException("No null terminator found");
        }
      }
      if (index == 0) {
        // Empty string, we don't have to ready anything.
        return "";
      }

      // Reset the stream to the mark and read the string.
      inputStream.reset();
      byte[] result = new byte[index];
      inputStream.readFully(result);
      // Skip the null-terminator.
      //noinspection StatementWithEmptyBody
      while (inputStream.skip(1) < 1) {}
      return new String(result, StandardCharsets.UTF_8);
    } finally {
      // Drop the mark to prevent unnecessary buffering.
      inputStream.mark(0);
    }
  }

}
