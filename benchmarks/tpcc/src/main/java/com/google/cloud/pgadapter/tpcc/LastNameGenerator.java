// Copyright 2024 Google LLC
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
package com.google.cloud.pgadapter.tpcc;

import com.google.common.collect.ImmutableList;
import java.util.Random;

public class LastNameGenerator {
  private static final ImmutableList<String> LAST_NAME_PARTS =
      ImmutableList.of(
          "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING");

  public static String generateLastName(Random random, long rowIndex) {
    int row;
    if (rowIndex < 1000L) {
      row = (int) rowIndex;
    } else {
      row = random.nextInt(1000);
    }
    return LAST_NAME_PARTS.get(row / 100)
        + LAST_NAME_PARTS.get((row / 10) % 10)
        + LAST_NAME_PARTS.get(row % 10);
  }
}
