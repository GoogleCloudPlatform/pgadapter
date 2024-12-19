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

class WarehouseRowProducer extends AbstractRowProducer {
  private static final String TABLE = "warehouse";
  private static final String COLUMNS =
      "w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd";

  WarehouseRowProducer(DataLoadStatus status, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incWarehouse);
  }

  @Override
  List<ImmutableList> createRowsAsList(long rowIndex) {
    return Arrays.asList(
        ImmutableList.of(
            getId(rowIndex),
            getRandomName(),
            getRandomStreet(1),
            getRandomStreet(2),
            getRandomCity(),
            getRandomState(),
            getRandomZip(),
            getRandomTax(),
            "0.0"));
  }
}
