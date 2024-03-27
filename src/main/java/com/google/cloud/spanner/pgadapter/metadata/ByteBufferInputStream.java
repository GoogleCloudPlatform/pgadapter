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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;

/** Input stream backed by a {@link ByteBuffer}. */
public class ByteBufferInputStream extends InputStream {
  private final ByteBuffer buffer;

  public ByteBufferInputStream(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public int read() throws IOException {
    if (!buffer.hasRemaining()) {
      return -1;
    }
    return Byte.toUnsignedInt(buffer.get());
  }

  @Override
  public int read(@Nonnull byte[] destination, int offset, int len) throws IOException {
    if (!buffer.hasRemaining()) {
      return -1;
    }

    int actualLength = Math.min(len, buffer.remaining());
    buffer.get(destination, offset, actualLength);
    return actualLength;
  }
}
