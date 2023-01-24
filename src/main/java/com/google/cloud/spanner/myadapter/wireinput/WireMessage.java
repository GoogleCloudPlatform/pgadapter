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

package com.google.cloud.spanner.myadapter.wireinput;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.myadapter.ConnectionHandler;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Generic representation for a wire message, generally executed by calling process. */
@InternalApi
public abstract class WireMessage {

  private static final Logger logger = Logger.getLogger(WireMessage.class.getName());

  protected int length;
  protected ByteArrayInputStream bufferedInputStream;
  protected ConnectionHandler connection;
  private int messageSequenceNumber;

  public WireMessage(HeaderMessage headerMessage) throws IOException {
    this.length = headerMessage.getRemainingPayloadLength();
    this.messageSequenceNumber = headerMessage.getMessageSequenceNumber();
    this.bufferedInputStream = headerMessage.getBufferedInputStream();
  }

  public WireMessage(int messageSequenceNumber) {
    this.length = 0;
    this.messageSequenceNumber = messageSequenceNumber;
    this.bufferedInputStream = new ByteArrayInputStream(new byte[0]);
  }

  /**
   * Process the incoming request. Effectively a template pattern.
   *
   * @throws Exception If the processing fails.
   */
  public void process() throws Exception {
    logger.log(Level.FINE, this::toString);
    processRequest();
  }

  /**
   * Override this method to process the incoming request and send a response. Template method for
   * process the request.
   *
   * @throws Exception If any step in output message fails.
   */
  protected abstract void processRequest() throws Exception;

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

  /**
   * Used for logging.
   *
   * @return Message Identifier (int for Bootstrap, char otherwise).
   */
  protected abstract String getIdentifier();

  @Override
  public String toString() {
    return new MessageFormat("> Received Message: ({0}) {1}, with Payload: '{'{2}'}'")
        .format(
            new Object[] {this.getIdentifier(), this.getMessageName(), this.getPayloadString()});
  }

  protected String readAll() throws IOException {
    byte[] bytes = new byte[this.bufferedInputStream.available()];
    this.bufferedInputStream.read(bytes);
    return new String(bytes);
  }

  protected long readLengthEncodedInt() throws IOException {
    if (this.bufferedInputStream.available() < 1) {
      throw new IOException("Not enough data in the stream to read a length encoded int");
    }
    int value = this.bufferedInputStream.read();
    if (value < 251) {
      return value;
    }
    switch (value) {
      case 0xFC: // 2-byte integer.
        return readFixedLengthInt(2);
      case 0xFD: // 3-byte integer.
        return readFixedLengthInt(3);
      case 0xFE: // 8-byte integer.
        return readFixedLengthInt(8);
      default:
        throw new IOException("Illegal length encoded int");
    }
  }

  protected long readFixedLengthInt(int length) throws IOException {
    if (this.bufferedInputStream.available() < length) {
      throw new IOException("Not enough bytes for length encoded int");
    }

    long value = 0;
    long multiplier = 1;
    for (int i = 0; i < length; ++i) {
      value += (this.bufferedInputStream.read() & 0xff) * multiplier;
      multiplier *= 256;
    }

    return value;
  }

  public int getMessageSequenceNumber() {
    return messageSequenceNumber;
  }
}
