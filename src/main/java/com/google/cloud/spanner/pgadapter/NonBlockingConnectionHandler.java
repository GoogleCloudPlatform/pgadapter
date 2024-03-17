package com.google.cloud.spanner.pgadapter;

import com.google.cloud.spanner.pgadapter.metadata.ChannelOutputStream;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.wireprotocol.BootstrapMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class NonBlockingConnectionHandler extends ConnectionHandler {
  private static final Logger logger = Logger.getLogger(NonBlockingConnectionHandler.class.getName());

  private final BlockingQueue<BootstrapMessage> bootstrapMessages = new LinkedBlockingQueue<>();

  private final BlockingQueue<ControlMessage> controlMessages = new LinkedBlockingQueue<>();

  private final PipedInputStream connectionInputStream = new PipedInputStream();

  private final PipedOutputStream connectionInputStreamBuffer = new PipedOutputStream();

  private final SocketChannel channel;

  NonBlockingConnectionHandler(ProxyServer server, SocketChannel channel) throws IOException {
    super(server, channel.socket());
    this.channel = channel;
    this.connectionInputStream.connect(connectionInputStreamBuffer);
    this.connectionMetadata = new ConnectionMetadata(connectionInputStream, new ChannelOutputStream(channel));
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
