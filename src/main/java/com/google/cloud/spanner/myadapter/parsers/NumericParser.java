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

package com.google.cloud.spanner.myadapter.parsers;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * Parse specified type to binary (generally this is the simplest parse class, as items are
 * generally represented in binary for wire format).
 */
@InternalApi
public class NumericParser extends Parser<BigDecimal> {

  NumericParser(ResultSet item, int position) {
    super((resultSet, index) -> item.getBigDecimal(index), item, position);
  }

  @Override
  public byte[] toLengthEncodedBytes() throws IOException {
    return StringParser.getLengthEncodedBytes(String.valueOf(item));
  }
}
