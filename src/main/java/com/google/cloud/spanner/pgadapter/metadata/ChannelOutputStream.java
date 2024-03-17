package com.google.cloud.spanner.pgadapter.metadata;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ChannelOutputStream extends OutputStream {
  private byte[] b1;
  private final WritableByteChannel channel;

  public ChannelOutputStream(WritableByteChannel channel) {
    this.channel = channel;
  }

  @Override
  public void write(int b) throws IOException {
    if (b1 == null) {
      b1 = new byte[1];
    }
    b1[0] = (byte) b;
    this.channel.write(ByteBuffer.wrap(b1));
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(off >= 0);
    Preconditions.checkArgument(off + len < b.length);
    this.channel.write(ByteBuffer.wrap(b, off, len));
  }


}
