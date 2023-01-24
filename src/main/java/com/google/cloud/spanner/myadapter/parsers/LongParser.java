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

package com.google.cloud.spanner.myadapter.parsers;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import java.io.IOException;

/** Translate from wire protocol to long and vice versa. */
@InternalApi
public class LongParser extends Parser<Long> {

  LongParser(ResultSet item, int position) {
    super((resultSet, index) -> item.getLong(position), item, position);
  }

  @Override
  public byte[] toLengthEncodedBytes() throws IOException {
    return StringParser.getLengthEncodedBytes(Long.toString(item));
  }

  public static byte[] getLengthEncodedBytes(long value) {
    byte[] bytes;
    if (value < 251) {
      bytes = new byte[] {(byte) value};
    } else if (value < (1L << 16)) {
      bytes = new byte[3];
      bytes[0] = (byte) 0xFC;
    } else if (value < (1L << 24)) {
      bytes = new byte[4];
      bytes[0] = (byte) 0xFD;
    } else {
      bytes = new byte[9];
      bytes[0] = (byte) 0xFE;
    }

    for (int i = 1; i < bytes.length; ++i) {
      bytes[i] = (byte) (value & 255);
      value = value >> 8;
    }

    return bytes;
  }
}
