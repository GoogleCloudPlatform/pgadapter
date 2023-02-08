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
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.utils.Converter;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;

@InternalApi
public class CopyDataResponse extends WireOutput {
  @InternalApi
  public enum ResponseType {
    ROW,
    TRAILER,
    TRAILER_WITH_HEADER,
  }

  private final ResponseType responseType;
  private final DataFormat format;
  private final String stringData;
  private final char rowTerminator;
  private final Converter converter;

  /** Creates a {@link CopyDataResponse} message containing the fixed binary COPY trailer. */
  @InternalApi
  public static CopyDataResponse createBinaryTrailer(
      DataOutputStream output, boolean includeHeader) {
    return new CopyDataResponse(
        output,
        includeHeader ? 21 : 2,
        includeHeader ? ResponseType.TRAILER_WITH_HEADER : ResponseType.TRAILER);
  }

  private CopyDataResponse(DataOutputStream output, int length, ResponseType responseType) {
    super(output, length + 4);
    this.responseType = responseType;
    this.format = DataFormat.POSTGRESQL_BINARY;
    this.stringData = null;
    this.rowTerminator = 0;
    this.converter = null;
  }

  public CopyDataResponse(DataOutputStream output, String data, char rowTerminator) {
    super(output, data.length() + 5);
    this.responseType = ResponseType.ROW;
    this.format = DataFormat.POSTGRESQL_TEXT;
    this.stringData = data;
    this.rowTerminator = rowTerminator;
    this.converter = null;
  }

  public CopyDataResponse(DataOutputStream output, Converter converter) {
    super(output, 0);
    this.responseType = ResponseType.ROW;
    this.format = DataFormat.POSTGRESQL_BINARY;
    this.stringData = null;
    this.rowTerminator = 0;
    this.converter = converter;
  }

  @Override
  public void send(boolean flush) throws Exception {
    if (converter != null) {
      this.length = 4 + converter.convertResultSetRowToDataRowResponse();
    }
    super.send(flush);
  }

  @Override
  protected void sendPayload() throws Exception {
    if (this.format == DataFormat.POSTGRESQL_TEXT) {
      this.outputStream.write(this.stringData.getBytes(StandardCharsets.UTF_8));
      this.outputStream.write(this.rowTerminator);
    } else if (this.format == DataFormat.POSTGRESQL_BINARY) {
      if (this.responseType == ResponseType.TRAILER_WITH_HEADER) {
        this.outputStream.write(COPY_BINARY_HEADER);
        this.outputStream.writeInt(0); // flags
        this.outputStream.writeInt(0); // header extension area length
        this.outputStream.writeShort(-1);
      } else if (this.responseType == ResponseType.TRAILER) {
        this.outputStream.writeShort(-1);
      } else if (this.converter != null) {
        this.converter.writeBuffer(this.outputStream);
      } else {
        // This should not happen.
        throw PGExceptionFactory.newPGException("Invalid CopyDataResponse", SQLState.InternalError);
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
