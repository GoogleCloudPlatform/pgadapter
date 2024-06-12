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

import com.google.cloud.pgadapter.tpcc.dataloader.OrderRowProducer.DistrictId;
import com.google.common.collect.ImmutableList;

class NewOrderRowProducer extends AbstractOrderedIdRowProducer {
  private static final String TABLE = "new_orders";
  private static final String COLUMNS =
      """
    o_id,
    c_id,
    d_id,
    w_id
    """;

  private final long warehouseId;

  private final long districtId;

  private final ImmutableList<Long> customerIds;

  NewOrderRowProducer(DataLoadStatus status, long warehouseId, long districtId, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incNewOrder);
    this.warehouseId = warehouseId;
    this.districtId = districtId;
    this.customerIds = OrderRowProducer.CUSTOMER_IDS.get(new DistrictId(warehouseId, districtId));
  }

  @Override
  String createRow(long rowIndex) {
    if (rowIndex % 3L == 0) {
      return String.join(
          ",",
          ImmutableList.of(
              getId(rowIndex),
              getCustomerId(rowIndex),
              String.valueOf(districtId),
              String.valueOf(warehouseId)));
    }
    return null;
  }

  String getCustomerId(long rowIndex) {
    return String.valueOf(Long.reverse(customerIds.get((int) rowIndex)));
  }
}
