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
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.annotation.Nonnull;

/** Translate from wire protocol to jsonb. */
@InternalApi
public class JsonbParser extends Parser<String> {

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
          this.item = toString(item);
          break;
        case BINARY:
          if (item.length > 0) {
            if (item[0] == 1) {
              this.item = toString(Arrays.copyOfRange(item, 1, item.length));
            } else {
              throw PGException.newBuilder("Unknown version in binary jsonb value: " + item[0])
                  .setSQLState(SQLState.RaiseException)
                  .setSeverity(Severity.ERROR)
                  .build();
            }
          } else {
            this.item = "";
          }
          break;
        default:
      }
    }
  }

  /** Converts the binary data to an UTF8 string. */
  public static String toString(@Nonnull byte[] data) {
    return new String(data, UTF8);
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

  public static byte[] convertToPG(ResultSet resultSet, int position, DataFormat format) {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        return resultSet.getPgJsonb(position).getBytes(StandardCharsets.UTF_8);
      case POSTGRESQL_BINARY:
        return convertToPG(resultSet.getPgJsonb(position));
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
