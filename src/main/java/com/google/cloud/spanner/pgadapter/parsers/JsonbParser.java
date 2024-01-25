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

package com.google.cloud.spanner.pgadapter.parsers;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.annotation.Nonnull;

/** Translate from wire protocol to jsonb. */
@InternalApi
public class JsonbParser extends Parser<String> {
  private static final byte[] BINARY_HEADER = new byte[] {1};

  JsonbParser(ResultSet item, int position) {
    this.item = item.getPgJsonb(position);
  }

  JsonbParser(Object item) {
    this.item = (String) item;
  }

  JsonbParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          this.item = new String(item, UTF8);
          break;
        case BINARY:
          this.item = toString(item);
          break;
        default:
      }
    }
  }

  /** Converts the binary data to an UTF8 string. */
  public static String toString(@Nonnull byte[] data) {
    if (data.length > 0) {
      if (data[0] == 1) {
        return new String(Arrays.copyOfRange(data, 1, data.length), StandardCharsets.UTF_8);
      } else {
        throw PGException.newBuilder("Unknown version in binary jsonb value: " + data[0])
            .setSQLState(SQLState.RaiseException)
            .setSeverity(Severity.ERROR)
            .build();
      }
    } else {
      return "";
    }
  }

  @Override
  public String stringParse() {
    return this.item;
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    return convertToPG(this.item);
  }

  static byte[] convertToPG(String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    byte[] result = new byte[bytes.length + 1];
    // Set version = 1
    result[0] = 1;
    System.arraycopy(bytes, 0, result, 1, bytes.length);

    return result;
  }

  public static byte[] convertToPG(
      DataOutputStream dataOutputStream, ResultSet resultSet, int position, DataFormat format) {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        StringParser.writeToPG(dataOutputStream, resultSet.getPgJsonb(position));
        return null;
      case POSTGRESQL_BINARY:
        StringParser.writeToPG(dataOutputStream, resultSet.getPgJsonb(position), BINARY_HEADER);
        return null;
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(Value.pgJsonb(this.item));
  }
}
