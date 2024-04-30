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

class StockRowProducer extends AbstractRowProducer {
  private static final String TABLE = "stock";
  private static final String COLUMNS =
      """
    s_i_id,
    w_id,
    s_quantity,
    s_dist_01,
    s_dist_02,
    s_dist_03,
    s_dist_04,
    s_dist_05,
    s_dist_06,
    s_dist_07,
    s_dist_08,
    s_dist_09,
    s_dist_10,
    s_ytd,
    s_order_cnt,
    s_remote_cnt,
    s_data
    """;

  private final long warehouseId;

  StockRowProducer(DataLoadStatus status, long warehouseId, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incStock);
    this.warehouseId = warehouseId;
  }

  @Override
  String createRow(long rowIndex) {
    return String.join(
        ",",
        ImmutableList.of(
            getId(rowIndex),
            String.valueOf(warehouseId),
            getRandomQuantity(),
            getRandomString(24),
            getRandomString(24),
            getRandomString(24),
            getRandomString(24),
            getRandomString(24),
            getRandomString(24),
            getRandomString(24),
            getRandomString(24),
            getRandomString(24),
            getRandomString(24),
            getYtd(),
            getOrderCount(),
            getRemoteCount(),
            getData()));
  }

  String getRandomQuantity() {
    return getRandomInt(10, 100);
  }

  String getYtd() {
    return "0";
  }

  String getOrderCount() {
    return "0";
  }

  String getRemoteCount() {
    return "0";
  }

  String getData() {
    return getRandomString(random.nextInt(25) + 26);
  }
}
