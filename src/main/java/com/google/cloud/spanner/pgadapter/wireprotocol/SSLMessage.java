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

package com.google.cloud.spanner.pgadapter.wireprotocol;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.wireoutput.DeclineSSLResponse;
import java.io.IOException;
import java.text.MessageFormat;

/**
 * Handles SSL bootstrap message. Since we do not do SSL (connection is expected to be through
 * localhost after all), we decline the first message, and send an error for subsequent ones.
 */
@InternalApi
public class SSLMessage extends BootstrapMessage {

  private static final int MESSAGE_LENGTH = 8;
  public static final int IDENTIFIER = 80877103; // First Hextet: 1234, Second Hextet: 5679

  private ThreadLocal<Boolean> executedOnce = ThreadLocal.withInitial(() -> false);

  public SSLMessage(ConnectionHandler connection) throws Exception {
    super(connection, MESSAGE_LENGTH);
  }

  @Override
  protected void sendPayload() throws Exception {
    if (executedOnce.get()) {
      this.connection.handleTerminate();
      throw new IOException("SSL not supported by server");
    }
    new DeclineSSLResponse(this.outputStream).send();
    executedOnce.set(true);
  }

  /**
   * Here we send another bootstrap factory, since we ignore SSL.
   *
   * @throws Exception
   */
  @Override
  public void nextHandler() throws Exception {
    this.connection.setMessageState(BootstrapMessage.create(this.connection));
  }

  @Override
  protected String getMessageName() {
    return "SSL Setup";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Previously Executed: {1}")
        .format(new Object[] {this.length, this.executedOnce.get()});
  }

  @Override
  protected String getIdentifier() {
    return Integer.toString(IDENTIFIER);
  }
}
