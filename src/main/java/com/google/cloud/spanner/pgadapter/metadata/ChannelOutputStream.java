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
    write(b1, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(off >= 0);
    Preconditions.checkArgument(off + len <= b.length);
    int remainingLength = len;
    while (remainingLength > 0) {
      int written = this.channel.write(ByteBuffer.wrap(b, off, remainingLength));
      remainingLength -= written;
      off += written;
    }
  }
}
