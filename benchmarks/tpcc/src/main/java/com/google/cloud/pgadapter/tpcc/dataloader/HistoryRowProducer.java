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

class HistoryRowProducer extends AbstractRowProducer {
  private static final String TABLE = "history";
  private static final String COLUMNS =
      """
    c_id,
    d_id,
    w_id,
    h_d_id,
    h_w_id,
    h_date,
    h_amount,
    h_data
    """;

  private final long warehouseId;

  private final long districtId;

  HistoryRowProducer(DataLoadStatus status, long warehouseId, long districtId, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incHistory);
    this.warehouseId = warehouseId;
    this.districtId = districtId;
  }

  @Override
  String createRow(long rowIndex) {
    return String.join(
        ",",
        ImmutableList.of(
            getId(rowIndex),
            String.valueOf(districtId),
            String.valueOf(warehouseId),
            String.valueOf(districtId),
            String.valueOf(warehouseId),
            now(),
            getYtdPayment(),
            getData()));
  }

  String getYtdPayment() {
    return "10.0";
  }

  String getData() {
    return getRandomString(random.nextInt(24) + 1);
  }
}
