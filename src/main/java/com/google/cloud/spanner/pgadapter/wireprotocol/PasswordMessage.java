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

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse.State;
import com.google.cloud.spanner.pgadapter.wireoutput.TerminateResponse;
import java.text.MessageFormat;

/**
 * A Password Message takes a username and password and input and supposedly handles auth. Here,
 * however, since connections are through localhost, we do not do so.
 */
public class PasswordMessage extends ControlMessage {

  protected static final char IDENTIFIER = 'p';

  private final String username;
  private final String password;

  public PasswordMessage(ConnectionHandler connection, String username)
      throws Exception {
    super(connection);
    this.username = username;
    this.password = this.readAll();
  }

  protected void sendPayload() throws Exception {
    if(useAuthentication() && !checkCredentials(this.username, this.password)) {
      new ErrorResponse(this.outputStream,
          new Exception("Could not Authenticate User"),
          State.InternalError).send();
      new TerminateResponse(this.outputStream).send();
    } else {
      BootstrapMessage.sendStartupMessage(
          this.outputStream,
          this.connection.getConnectionId(),
          this.connection.getSecret()
      );
    }
  }

  private boolean useAuthentication() {
    return false;
  }

  private boolean checkCredentials(String username, String password) {
    // TODO add an actual way to auth
    return true;
  }

  @Override
  protected String getMessageName() {
    return "Password Exchange";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat(
        "Length: {0}, "
            + "Username: {1}, "
            + "Password: {2}")
        .format(new Object[]{
            this.length,
            this.username,
            this.password
        });
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }
}
