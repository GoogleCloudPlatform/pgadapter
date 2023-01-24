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

package com.google.cloud.spanner.myadapter.wireoutput;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.myadapter.wireinput.WireMessage;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Classes inheriting WireOutput concern themselves with sending data back to the client using MySQL
 * wire protocol. These classes function similarly to {@link WireMessage} in that they use the
 * constructor to instantiate items, but require send to be called to actually send data.
 *
 * <p>Note that this class will handle sending header (payload length and packet number) via send,
 * so you just have to implement payload sending. The payload should be provided via one or more
 * calls to writePayload before calling send, and this process can be repeated any number of times.
 */
@InternalApi
public abstract class WireOutput {

  private static final Logger logger = Logger.getLogger(WireOutput.class.getName());

  private int currentSequenceNumber;
  private final DataOutputStream outputStream;
  private final ByteArrayOutputStream bufferOutputStream;
  protected int length;

  public WireOutput(int currentSequenceNumber, ConnectionMetadata connectionMetadata) {
    this.currentSequenceNumber = currentSequenceNumber;
    this.outputStream = connectionMetadata.getOutputStream();
    this.bufferOutputStream = new ByteArrayOutputStream(256);
  }

  protected void writePayload(byte[] data) throws IOException {
    this.bufferOutputStream.write(data);
    // TODO: If buffered data exceeded the max write packet size, send the buffered data in chunks.
  }

  public int send() throws IOException {
    send(false);
    return currentSequenceNumber;
  }

  public int send(boolean flush) throws IOException {
    logger.log(Level.FINE, this::toString);
    sendHeader();
    this.bufferOutputStream.writeTo(outputStream);
    if (flush) {
      outputStream.flush();
    }
    return currentSequenceNumber;
  }

  private void sendHeader() throws IOException {
    int length = this.bufferOutputStream.size();
    for (int i = 0; i < 3; ++i) {
      this.outputStream.write((byte) (length & 255));
      length >>= 8;
    }

    this.outputStream.write((byte) getNextMessageSequenceNumber());
  }

  private int getNextMessageSequenceNumber() {
    return ++currentSequenceNumber;
  }

  /**
   * Used for logging.
   *
   * @return The official name of the wire message.
   */
  protected abstract String getMessageName();

  /**
   * Used for logging.
   *
   * @return Payload metadata.
   */
  protected abstract String getPayloadString();

  @Override
  public String toString() {
    return new MessageFormat("< Sending Message: {0}, Length: {1}, Payload: '{'{3}'}'")
        .format(
            new Object[] {
              this.getMessageName(), this.bufferOutputStream.size(), this.getPayloadString()
            });
  }
}
