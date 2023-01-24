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
import com.google.cloud.spanner.myadapter.parsers.IntegerParser;
import com.google.cloud.spanner.myadapter.parsers.StringParser;
import java.io.IOException;

public class ColumnDefinitionResponse extends WireOutput {

  String catalog = "def";
  // TODO : Today the MySQL cli does not accept any other value apart from 12 for
  //  fixedLengthFieldsLength. Assess that a constant value of 12 is good enough.
  int fixedLengthFieldsLength = 12;

  public static class Builder {
    int connectionSequenceNumber;
    ConnectionMetadata connectionMetadata;

    String schema;
    String table;
    String originalTable;
    String column;
    String originalColumn;

    int charset;
    int maxColumnLength;
    int columnType;
    int columnDefinitionFlags;
    int decimals;

    public Builder(int connectionSequenceNumber, ConnectionMetadata connectionMetadata) {
      this.connectionSequenceNumber = connectionSequenceNumber;
      this.connectionMetadata = connectionMetadata;
    }

    public Builder schema(String schema) {
      this.schema = schema;
      return this;
    }

    public Builder table(String table) {
      this.table = table;
      return this;
    }

    public Builder originalTable(String originalTable) {
      this.originalTable = originalTable;
      return this;
    }

    public Builder column(String column) {
      this.column = column;
      return this;
    }

    public Builder originalColumn(String originalColumn) {
      this.originalColumn = originalColumn;
      return this;
    }

    public Builder charset(int charset) {
      this.charset = charset;
      return this;
    }

    public Builder maxColumnLength(int maxColumnLength) {
      this.maxColumnLength = maxColumnLength;
      return this;
    }

    public Builder columnType(int columnType) {
      this.columnType = columnType;
      return this;
    }

    public Builder columnDefinitionFlags(int columnDefinitionFlags) {
      this.columnDefinitionFlags = columnDefinitionFlags;
      return this;
    }

    public Builder decimals(int decimals) {
      this.decimals = decimals;
      return this;
    }

    public ColumnDefinitionResponse build() throws IOException {
      return new ColumnDefinitionResponse(this);
    }
  }

  public ColumnDefinitionResponse(ColumnDefinitionResponse.Builder builder) throws IOException {
    super(builder.connectionSequenceNumber, builder.connectionMetadata);

    writePayload(StringParser.getLengthEncodedBytes(catalog));
    writePayload(StringParser.getLengthEncodedBytes(builder.schema));
    writePayload(StringParser.getLengthEncodedBytes(builder.table));
    writePayload(StringParser.getLengthEncodedBytes(builder.originalTable));
    // String columnName = resultSet.getMetadata().getRowType().getFields(columnIndex).getName();
    writePayload(StringParser.getLengthEncodedBytes(builder.column));
    // TODO: Understand the relationship between columnName and originalColumnName.
    writePayload(StringParser.getLengthEncodedBytes(builder.originalColumn));
    writePayload(new byte[] {(byte) fixedLengthFieldsLength});
    byte[] charSet =
        new byte[] {
          (byte) (builder.charset & 255), (byte) ((builder.charset >> 8) & 255)
        }; // binary charset
    writePayload(charSet);
    writePayload(IntegerParser.binaryParse(builder.maxColumnLength));
    byte[] columnType = new byte[] {(byte) builder.columnType};
    writePayload(columnType);
    byte[] columnDefinitionFlags =
        new byte[] {
          (byte) (builder.columnDefinitionFlags & 255),
          (byte) ((builder.columnDefinitionFlags >> 8) & 255)
        };
    writePayload(columnDefinitionFlags);
    byte[] decimals = new byte[] {(byte) builder.decimals};
    writePayload(decimals);
    byte[] iDontKnowWhatTheseBytesMean = new byte[] {(byte) 0x00, (byte) 0x00};
    writePayload(iDontKnowWhatTheseBytesMean);
  }

  @Override
  protected String getMessageName() {
    return "OkResponse";
  }

  @Override
  protected String getPayloadString() {
    return "";
  }
}
