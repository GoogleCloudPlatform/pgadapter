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
              throw PGException.newBuilder()
                  .setSQLState(SQLState.RaiseException)
                  .setSeverity(Severity.ERROR)
                  .setMessage("Unknown version in binary jsonb value: " + item[0])
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
    byte[] value = this.item.getBytes(StandardCharsets.UTF_8);
    byte[] result = new byte[value.length + 1];
    // Set version = 1
    result[0] = 1;
    System.arraycopy(value, 0, result, 1, value.length);

    return result;
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
