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
import com.google.common.collect.ImmutableSet;
import java.util.Set;

/**
 * Parse specified data to boolean. For most cases it is simply returning the bit representation.
 */
@InternalApi
public class BooleanParser extends Parser<Boolean> {
  private static final byte[] TRUE_VALUE_BYTES = new byte[] {'1'};
  private static final byte[] FALSE_VALUE_BYTES = new byte[] {'0'};
  public static final Set<String> TRUE_VALUES = ImmutableSet.of("true", "1");
  public static final Set<String> FALSE_VALUES = ImmutableSet.of("false", "0");

  BooleanParser(ResultSet item, int position) {
    super((resultSet, index) -> item.getBoolean(position), item, position);
  }

  public byte[] toLengthEncodedBytes() {
    return item ? TRUE_VALUE_BYTES : FALSE_VALUE_BYTES;
  }
}
