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
public class Float64Parser extends Parser<Double> {

  Float64Parser(ResultSet item, int position) {
    super((resultSet, index) -> item.getDouble(position), item, position);
  }

  @Override
  public byte[] toLengthEncodedBytes() throws IOException {
    return StringParser.getLengthEncodedBytes(item == null ? null : Double.toString(item));
  }
}
