// Copyright 2023 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.common.base.Preconditions;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;

/**
 * Notices can be sent as asynchronous messages and can include warnings, informational messages,
 * debug information, etc.
 */
@InternalApi
public class NoticeResponse extends WireOutput {
  public enum NoticeSeverity {
    WARNING,
    NOTICE,
    DEBUG,
    INFO,
    LOG,
    TIP,
  }

  private static final int HEADER_LENGTH = 4;
  private static final int FIELD_IDENTIFIER_LENGTH = 1;
  private static final int NULL_TERMINATOR_LENGTH = 1;

  private static final byte MESSAGE_FLAG = 'M';
  private static final byte SEVERITY_FLAG = 'S';
  private static final byte HINT_FLAG = 'H';
  private static final byte NULL_TERMINATOR = 0;

  private final NoticeSeverity severity;
  private final String message;
  private final String hint;

  public NoticeResponse(DataOutputStream output, String message) {
    this(output, NoticeSeverity.NOTICE, Preconditions.checkNotNull(message), null);
  }

  public NoticeResponse(
      DataOutputStream output, NoticeSeverity severity, String message, String hint) {
    super(
        output,
        HEADER_LENGTH
            + FIELD_IDENTIFIER_LENGTH
            + Preconditions.checkNotNull(severity).name().getBytes(StandardCharsets.UTF_8).length
            + NULL_TERMINATOR_LENGTH
            + FIELD_IDENTIFIER_LENGTH
            + Preconditions.checkNotNull(message).getBytes(StandardCharsets.UTF_8).length
            + NULL_TERMINATOR_LENGTH
            + (hint == null
                ? 0
                : (FIELD_IDENTIFIER_LENGTH
                    + hint.getBytes(StandardCharsets.UTF_8).length
                    + NULL_TERMINATOR_LENGTH))
            + NULL_TERMINATOR_LENGTH);
    this.severity = severity;
    this.message = message;
    this.hint = hint;
  }

  @Override
  protected void sendPayload() throws Exception {
    this.outputStream.writeByte(SEVERITY_FLAG);
    this.outputStream.write(severity.name().getBytes(StandardCharsets.UTF_8));
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.writeByte(MESSAGE_FLAG);
    this.outputStream.write(message.getBytes(StandardCharsets.UTF_8));
    this.outputStream.writeByte(NULL_TERMINATOR);
    if (this.hint != null) {
      this.outputStream.writeByte(HINT_FLAG);
      this.outputStream.write(hint.getBytes(StandardCharsets.UTF_8));
      this.outputStream.writeByte(NULL_TERMINATOR);
    }
    this.outputStream.writeByte(NULL_TERMINATOR);
  }

  @Override
  public byte getIdentifier() {
    return 'N';
  }

  @Override
  protected String getMessageName() {
    return "Notice";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, Severity: {1}, Notice Message: {2}")
        .format(new Object[] {this.length, this.severity, this.message});
  }
}
