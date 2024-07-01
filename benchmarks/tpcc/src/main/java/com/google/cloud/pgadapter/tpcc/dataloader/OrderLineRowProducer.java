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

import com.google.cloud.Timestamp;
import com.google.cloud.pgadapter.tpcc.dataloader.OrderRowProducer.DistrictId;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

class OrderLineRowProducer extends AbstractOrderedIdRowProducer {
  private static final String TABLE = "order_line";
  private static final String COLUMNS =
      """
    o_id,
    c_id,
    d_id,
    w_id,
    ol_number,
    ol_i_id,
    ol_supply_w_id,
    ol_delivery_d,
    ol_quantity,
    ol_amount,
    ol_dist_info
    """;

  private final int itemCount;

  private final ImmutableList<Long> customerIds;

  OrderLineRowProducer(
      DataLoadStatus status, long warehouseId, long districtId, int itemCount, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incOrderLine);
    this.warehouseId = warehouseId;
    this.districtId = districtId;
    this.itemCount = itemCount;
    this.customerIds = OrderRowProducer.CUSTOMER_IDS.get(new DistrictId(warehouseId, districtId));
  }

  @Override
  String createRow(long rowIndex) {
    StringBuilder builder = new StringBuilder();
    for (int line = 0; line < getOrderLineCount(rowIndex); line++) {
      if (line > 0) {
        builder.append("\n");
      }
      builder.append(
          String.join(
              ",",
              ImmutableList.of(
                  getId(rowIndex),
                  getCustomerId(rowIndex),
                  String.valueOf(districtId),
                  String.valueOf(warehouseId),
                  String.valueOf(line),
                  getRandomItem(),
                  String.valueOf(warehouseId),
                  getDeliveryDateAsString(rowIndex),
                  getQuantity(),
                  getPrice(rowIndex),
                  getData())));
    }
    return builder.toString();
  }

  @Override
  List<ImmutableList> createRowsAsList(long rowIndex) {
    ImmutableList[] array = new ImmutableList[getOrderLineCount(rowIndex)];
    for (int line = 0; line < getOrderLineCount(rowIndex); line++) {
      array[line] =
          ImmutableList.of(
              getId(rowIndex),
              getCustomerId(rowIndex),
              String.valueOf(districtId),
              String.valueOf(warehouseId),
              String.valueOf(line),
              getRandomItem(),
              String.valueOf(warehouseId),
              getDeliveryDate(rowIndex),
              getQuantity(),
              getPrice(rowIndex),
              getData());
    }
    return Arrays.asList(array);
  }

  String getCustomerId(long rowIndex) {
    return String.valueOf(Long.reverse(customerIds.get((int) rowIndex)));
  }

  int getOrderLineCount(long rowIndex) {
    return Math.abs(Objects.hash(warehouseId, districtId, rowIndex) % 11) + 5;
  }

  String getRandomItem() {
    return String.valueOf(Long.reverse(random.nextInt(itemCount)));
  }

  Timestamp getDeliveryDate(long rowIndex) {
    // Use smallest timestamp to represent NULL because ImmutableList does not
    // permit NULLs.
    return rowIndex % 3L == 0 ? Timestamp.MIN_VALUE : Timestamp.now();
  }

  String getDeliveryDateAsString(long rowIndex) {
    return rowIndex % 3L == 0 ? "" : nowAsString();
  }

  String getQuantity() {
    return "5";
  }

  String getPrice(long rowIndex) {
    return rowIndex % 3L == 0 ? "0.0" : getRandomDecimal(10000, 2);
  }

  String getData() {
    return quote(getRandomString(24));
  }
}
