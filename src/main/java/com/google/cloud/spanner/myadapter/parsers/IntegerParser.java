// Copyright 2021 Google LLC
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

package com.google.cloud.spanner.myadapter.parsers;

import com.google.cloud.spanner.ResultSet;
import java.io.IOException;

/** Translate from wire protocol to int and vice versa */
public class IntegerParser extends Parser<Integer> {

  IntegerParser(ResultSet item, int position) {
    super((resultSet, index) -> Math.toIntExact(item.getLong(position)), item, position);
  }

  public static byte[] binaryParse(int value) {
    byte[] result = new byte[4];
    for (int i = 0; i < 4; ++i) {
      result[i] = (byte) (value & 255);
      value >>= 8;
    }
    return result;
  }

  @Override
  public byte[] toLengthEncodedBytes() throws IOException {
    throw new IOException(
        "Cannot convert integer to length encoded bytes. LongParser should be" + " used instead");
  }
}
