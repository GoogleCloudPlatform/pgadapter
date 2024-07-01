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
package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;

class ItemRowProducer extends AbstractRowProducer {
  private static final String TABLE = "item";
  private static final String COLUMNS =
      """
    i_id,
    i_im_id,
    i_name,
    i_price,
    i_data
    """;

  ItemRowProducer(DataLoadStatus status, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incItem);
  }

  @Override
  List<ImmutableList> createRowsAsList(long rowIndex) {
    return Arrays.asList(
        ImmutableList.of(
            getId(rowIndex), getRandomImId(), getName(rowIndex), getRandomPrice(), getData()));
  }

  String getRandomImId() {
    return getRandomInt(1, 10000);
  }

  String getRandomPrice() {
    return getRandomDecimal(100, 2);
  }

  String getName(long rowIndex) {
    return quote(String.format("item-%d-%s", rowIndex, getRandomString(10)));
  }

  String getData() {
    return quote(getRandomString(30));
  }
}
