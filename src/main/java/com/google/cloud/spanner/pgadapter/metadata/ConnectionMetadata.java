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
import com.google.cloud.Tuple;
import com.google.common.base.Preconditions;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Stack;

@InternalApi
public class ConnectionMetadata implements AutoCloseable {
  private static final int SOCKET_BUFFER_SIZE = 1 << 16;

  private final InputStream rawInputStream;
  private final OutputStream rawOutputStream;
  private final Stack<DataInputStream> inputStream = new Stack<>();
  private final Stack<DataOutputStream> outputStream = new Stack<>();

  /**
   * Creates a {@link DataInputStream} and a {@link DataOutputStream} from the given raw streams and
   * pushes these as the current streams to use for communication for a connection.
   */
  public ConnectionMetadata(InputStream rawInputStream, OutputStream rawOutputStream) {
    this.rawInputStream = Preconditions.checkNotNull(rawInputStream);
    this.rawOutputStream = Preconditions.checkNotNull(rawOutputStream);
    pushNewStreams();
  }

  @Override
  public void close() throws Exception {
    this.rawInputStream.close();
    this.rawOutputStream.close();
  }

  /**
   * Creates a new buffered {@link DataInputStream} and {@link DataOutputStream} tuple to use for
   * the connection. This is used for the COPY sub-protocol to prevent mixing the buffers used for
   * the normal protocol with the COPY protocol, as that would cause multithreaded access to those
   * buffers.
   */
  public Tuple<DataInputStream, DataOutputStream> pushNewStreams() {
    return Tuple.of(
        this.inputStream.push(
            new DataInputStream(new BufferedInputStream(this.rawInputStream, SOCKET_BUFFER_SIZE))),
        this.outputStream.push(
            new DataOutputStream(
                new BufferedOutputStream(this.rawOutputStream, SOCKET_BUFFER_SIZE))));
  }

  /**
   * Pops the current {@link DataInputStream} and {@link DataOutputStream} from the connection. This
   * is done when the COPY sub-protocol has finished.
   */
  public Tuple<DataInputStream, DataOutputStream> popStreams() {
    return Tuple.of(this.inputStream.pop(), this.outputStream.pop());
  }

  /** Returns the current {@link DataInputStream} for the connection. */
  public DataInputStream peekInputStream() {
    return inputStream.peek();
  }

  /** Returns the current {@link DataOutputStream} for the connection. */
  public DataOutputStream peekOutputStream() {
    return outputStream.peek();
  }
  /**
   * Returns the next byte in the input stream without removing it. Returns zero if no bytes are
   * available.
   */
  public char peekNextByte() throws IOException {
    DataInputStream dataInputStream = inputStream.peek();
    if (dataInputStream.available() > 0) {
      dataInputStream.mark(1);
      char result = (char) dataInputStream.readUnsignedByte();
      dataInputStream.reset();

      return result;
    }
    return 0;
  }
}
