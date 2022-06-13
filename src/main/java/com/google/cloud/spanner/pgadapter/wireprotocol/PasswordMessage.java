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

import static com.google.cloud.spanner.pgadapter.wireprotocol.StartupMessage.DATABASE_KEY;
import static com.google.cloud.spanner.pgadapter.wireprotocol.StartupMessage.createConnectionAndSendStartupMessage;

import com.google.api.client.util.PemReader;
import com.google.api.client.util.PemReader.Section;
import com.google.api.client.util.Strings;
import com.google.api.core.InternalApi;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse.State;
import com.google.cloud.spanner.pgadapter.wireoutput.TerminateResponse;
import java.io.IOException;
import java.io.StringReader;
import java.text.MessageFormat;
import java.util.Map;
import org.postgresql.util.ReaderInputStream;

/**
 * A Password Message takes a username and password and input and supposedly handles auth. Here,
 * however, since connections are through localhost, we do not do so.
 */
@InternalApi
public class PasswordMessage extends ControlMessage {

  private static final String USER_KEY = "user";
  protected static final char IDENTIFIER = 'p';

  private final Map<String, String> parameters;
  private final String username;
  private final String password;

  public PasswordMessage(ConnectionHandler connection, Map<String, String> parameters)
      throws Exception {
    super(connection);
    this.parameters = parameters;
    this.username = parameters.get(USER_KEY);
    this.password = this.readAll();
  }

  protected void sendPayload() throws Exception {
    if (!useAuthentication()) {
      new ErrorResponse(
              this.outputStream,
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Received PasswordMessage while authentication is disabled."),
              State.ProtocolViolation)
          .send(false);
      new TerminateResponse(this.outputStream).send();
      return;
    }

    GoogleCredentials credentials = checkCredentials(this.username, this.password);
    if (credentials == null) {
      new ErrorResponse(
              this.outputStream,
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.PERMISSION_DENIED,
                  "Invalid credentials received. "
                      + "PGAdapter expects the password to contain the JSON payload of a credentials file. "
                      + "Alternatively, the password may contain only the private key of a service account. "
                      + "The user name must in that case contain the service account email address."),
              State.InvalidPassword)
          .send(false);
      new TerminateResponse(this.outputStream).send();
    } else {
      createConnectionAndSendStartupMessage(
          this.connection, this.parameters.get(DATABASE_KEY), credentials);
    }
  }

  private boolean useAuthentication() {
    return this.connection.getServer().getOptions().shouldAuthenticate();
  }

  private GoogleCredentials checkCredentials(String username, String password) {
    if (Strings.isNullOrEmpty(password)) {
      return null;
    }

    // Verify that the password is either a JSON credentials file or a private key.
    // A private key is only allowed in combination with a username that is the email address of a
    // service account.
    if (!Strings.isNullOrEmpty(username) && username.indexOf('@') > -1) {
      // The username is potentially an email address. That means that the password could be a
      // private key. Try to parse it as such.
      try {
        Section privateKeySection =
            PemReader.readFirstSectionAndClose(new StringReader(password), "PRIVATE KEY");
        if (privateKeySection != null) {
          // Successfully identified as a private key. Manually create a ServiceAccountCredentials
          // instance and try to use this when connecting to Spanner.
          return ServiceAccountCredentials.fromPkcs8(
              /*clientId=*/ null, username, password, /*privateKeyId=*/ null, /*scopes=*/ null);
        }
      } catch (IOException ioException) {
        // Ignore and try to parse it as a credentials file.
      }
    }

    // Try to parse the password field as a JSON string that contains a credentials object.
    try {
      return GoogleCredentials.fromStream(new ReaderInputStream(new StringReader(password)));
    } catch (IOException ioException) {
      // Ignore and fallthrough..
    }

    return null;
  }

  @Override
  protected String getMessageName() {
    return "Password Exchange";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Username: {1}, " + "Password: {2}")
        .format(new Object[] {this.length, this.username, this.password});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public String getUsername() {
    return this.username;
  }

  public String getPassword() {
    return this.password;
  }
}
