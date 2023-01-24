// Copyright 2022 Google LLC
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

import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.myadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.myadapter.parsers.ParserHelper;
import com.google.cloud.spanner.myadapter.parsers.StringParser;
import java.io.IOException;

public class ErrorResponse extends WireOutput {

  public ErrorResponse(
      int currentSequenceNumber, ConnectionMetadata connectionMetadata, String message, int code)
      throws IOException {
    super(currentSequenceNumber, connectionMetadata);

    byte[] errorIdentifier = new byte[] {(byte) 255};
    writePayload(errorIdentifier);

    byte[] errorCode = ParserHelper.trailing2bytes(code);
    writePayload(errorCode);

    byte[] sqlState = new byte[] {0x23, 0x34, 0x32, 0x30, 0x30, 0x30};
    writePayload(sqlState);

    byte[] errorMessage = new StringParser(message).parse(FormatCode.LENGTH_ENCODED);
    writePayload(errorMessage);
  }

  @Override
  protected String getMessageName() {
    return "ErrorResponse";
  }

  @Override
  protected String getPayloadString() {
    return "";
  }
}
