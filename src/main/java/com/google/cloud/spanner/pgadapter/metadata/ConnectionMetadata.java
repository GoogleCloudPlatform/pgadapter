// Copyright 2020 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

@InternalApi
public class ConnectionMetadata implements AutoCloseable {
  private static final int SOCKET_BUFFER_SIZE = 1 << 16;

  private final DataInputStream inputStream;
  private final DataOutputStream outputStream;
  private boolean markedForRestart;

  /**
   * Creates a {@link DataInputStream} and a {@link DataOutputStream} from the given raw streams and
   * pushes these as the current streams to use for communication for a connection.
   */
  public ConnectionMetadata(InputStream rawInputStream, OutputStream rawOutputStream) {
    this.inputStream =
        new DataInputStream(
            new BufferedInputStream(
                Preconditions.checkNotNull(rawInputStream), SOCKET_BUFFER_SIZE));
    this.outputStream =
        new DataOutputStream(
            new BufferedOutputStream(
                Preconditions.checkNotNull(rawOutputStream), SOCKET_BUFFER_SIZE));
  }

  public void markForRestart() {
    markedForRestart = true;
  }

  @Override
  public void close() throws Exception {
    if (!markedForRestart) {
      this.inputStream.close();
      this.outputStream.close();
    }
  }

  /** Returns the current {@link DataInputStream} for the connection. */
  public DataInputStream getInputStream() {
    return inputStream;
  }

  /** Returns the current {@link DataOutputStream} for the connection. */
  public DataOutputStream getOutputStream() {
    return outputStream;
  }

  /**
   * Returns the next byte in the input stream without removing it. Returns zero if no bytes are
   * available. This method will wait for up to maxWaitMillis milliseconds to allow pending data to
   * become available in the buffer.
   */
  public char peekNextByte(long maxWaitMillis) throws IOException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (inputStream.available() == 0
        && stopwatch.elapsed(TimeUnit.MILLISECONDS) < maxWaitMillis) {
      Thread.yield();
    }
    if (inputStream.available() > 0) {
      inputStream.mark(1);
      char result = (char) inputStream.readUnsignedByte();
      inputStream.reset();

      return result;
    }
    return 0;
  }
}
