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

class DistrictRowProducer extends AbstractRowProducer {
  private static final String TABLE = "district";
  private static final String COLUMNS =
      """
      d_id,
      w_id,
      d_name,
      d_street_1,
      d_street_2,
      d_city,
      d_state,
      d_zip,
      d_tax,
      d_ytd,
      d_next_o_id""";

  DistrictRowProducer(DataLoadStatus status, long warehouseId, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incDistrict);
    this.warehouseId = warehouseId;
  }

  @Override
  List<ImmutableList> createRowsAsList(long rowIndex) {
    return Arrays.asList(
        ImmutableList.of(
            getId(rowIndex),
            String.valueOf(warehouseId),
            getRandomName(),
            getRandomStreet(1),
            getRandomStreet(2),
            getRandomCity(),
            getRandomState(),
            getRandomZip(),
            getRandomTax(),
            "0.0",
            "3001"));
  }
}
