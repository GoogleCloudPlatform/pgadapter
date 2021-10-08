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

package com.google.cloud.spanner.pgadapter.wireoutput;

import java.io.DataOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Classes inheriting WireOutput concern themselves with sending data back to the client using PG
 * wire protocol. These classes function similarly to {@link
 * com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage} in that they use the constructor to
 * instantiate items, but require send to be called to actually send data. Here, you must override
 * postSend with data you wish to send. Note that this subclass will handle sending the identifier
 * and length (provided you initialize them correctly through getIdentifier and he constructor
 * respectively) via send, so you just have to implement payload sending.
 */
public abstract class WireOutput {

  private static final Logger logger = Logger.getLogger(WireOutput.class.getName());

  protected static final Charset UTF8 = StandardCharsets.UTF_8;

  protected DataOutputStream outputStream;
  protected int length;

  public WireOutput(DataOutputStream output, int length) {
    this.outputStream = output;
    this.length = length;
  }

  /**
   * This is the method which must be called to actually send data. Here we log information, send
   * the protocol identifier and length automatically (provided those items were instantiated
   * correctly). For most WireOutput subclasses this should be unchanged; there are however
   * exceptions (such as where length) need not be sent for very specific protocols: for those,
   * override this and you will need to send the identifier and log yourself.
   *
   * @throws Exception
   */
  public void send() throws Exception {
    logger.log(Level.FINE, this.toString());
    this.outputStream.writeByte(this.getIdentifier());
    if (this.isCompoundResponse()) {
      this.outputStream.writeInt(this.length);
    }
    sendPayload();
    this.outputStream.flush();
  }

  /**
   * Override this method to include post-processing and metadata in the sending process. Template
   * method for send.
   *
   * @throws Exception
   */
  protected abstract void sendPayload() throws Exception;

  /**
   * Override this to specify the byte which represents the protocol for the specific message. Used
   * for logging and by send.
   *
   * @return
   */
  public abstract byte getIdentifier();

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
   * Whether this response sends more data than just the identifier (i.e.: length). WireOutput items
   * are convoluted in that some do send a large payload, and other (such as {@link
   * DeclineSSLResponse}) send only one byte back. Override with false if that is the case.
   *
   * @return True if compound, false otherwise.
   */
  protected boolean isCompoundResponse() {
    return true;
  }

  @Override
  public String toString() {
    return new MessageFormat("< Sending Message: ({0}) {1}, with Payload: '{'{2}'}'")
        .format(
            new Object[] {
              (char) this.getIdentifier(), this.getMessageName(), this.getPayloadString()
            });
  }
}
