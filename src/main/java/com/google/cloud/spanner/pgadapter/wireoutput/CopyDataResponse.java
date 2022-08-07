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

package com.google.cloud.spanner.pgadapter.wireoutput;

import static com.google.cloud.spanner.pgadapter.statements.CopyToStatement.COPY_BINARY_HEADER;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;

@InternalApi
public class CopyDataResponse extends WireOutput {
  @InternalApi
  public enum ResponseType {
    HEADER,
    ROW,
    TRAILER,
  }

  private final ResponseType responseType;
  private final DataFormat format;
  private final byte[][] binaryData;
  private final String stringData;
  private final char rowTerminator;

  /** Creates a {@link CopyDataResponse} message containing the fixed binary COPY header. */
  @InternalApi
  public static CopyDataResponse createBinaryHeader(DataOutputStream output) {
    return new CopyDataResponse(output, COPY_BINARY_HEADER.length + 8, ResponseType.HEADER);
  }

  /** Creates a {@link CopyDataResponse} message containing the fixed binary COPY trailer. */
  @InternalApi
  public static CopyDataResponse createBinaryTrailer(DataOutputStream output) {
    return new CopyDataResponse(output, 2, ResponseType.TRAILER);
  }

  private CopyDataResponse(DataOutputStream output, int length, ResponseType responseType) {
    super(output, length + 4);
    this.responseType = responseType;
    this.format = DataFormat.POSTGRESQL_BINARY;
    this.binaryData = null;
    this.stringData = null;
    this.rowTerminator = 0;
  }

  public CopyDataResponse(DataOutputStream output, String data, char rowTerminator) {
    super(output, data.length() + 5);
    this.responseType = ResponseType.ROW;
    this.format = DataFormat.POSTGRESQL_TEXT;
    this.stringData = data;
    this.rowTerminator = rowTerminator;
    this.binaryData = null;
  }

  public CopyDataResponse(DataOutputStream output, int length, byte[][] data) {
    super(output, length + 4);
    this.responseType = ResponseType.ROW;
    this.format = DataFormat.POSTGRESQL_BINARY;
    this.stringData = null;
    this.rowTerminator = 0;
    this.binaryData = data;
  }

  @Override
  protected void sendPayload() throws Exception {
    if (this.format == DataFormat.POSTGRESQL_TEXT) {
      this.outputStream.write(this.stringData.getBytes(StandardCharsets.UTF_8));
      this.outputStream.write(this.rowTerminator);
    } else if (this.format == DataFormat.POSTGRESQL_BINARY) {
      if (this.responseType == ResponseType.HEADER) {
        this.outputStream.write(COPY_BINARY_HEADER);
        this.outputStream.writeInt(0); // flags
        this.outputStream.writeInt(0); // header extension area length
      } else if (this.responseType == ResponseType.TRAILER) {
        this.outputStream.writeShort(-1);
      } else {
        this.outputStream.writeShort(this.binaryData.length);
        for (int col = 0; col < this.binaryData.length; col++) {
          if (this.binaryData[col] == null) {
            this.outputStream.writeInt(-1);
          } else {
            this.outputStream.writeInt(this.binaryData[col].length);
            this.outputStream.write(this.binaryData[col]);
          }
        }
      }
    }
  }

  @Override
  public byte getIdentifier() {
    return 'd';
  }

  @Override
  protected String getMessageName() {
    return "Copy Data Response";
  }

  @Override
  protected String getPayloadString() {
    return "<REDACTED DUE TO LENGTH & PERFORMANCE CONSTRAINTS>";
  }
}
