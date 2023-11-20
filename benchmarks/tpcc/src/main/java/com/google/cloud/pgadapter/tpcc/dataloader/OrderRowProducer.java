package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.common.collect.ImmutableList;
import java.util.Objects;

class OrderRowProducer extends AbstractRowProducer {
  private static final String TABLE = "orders";
  private static final String COLUMNS =
      """
    o_id,
    o_d_id,
    o_w_id,
    o_c_id,
    o_entry_d,
    o_carrier_id,
    o_ol_cnt,
    o_all_local
    """;

  private final long warehouseId;

  private final long districtId;

  private final int customerCount;

  OrderRowProducer(long warehouseId, long districtId, int customerCount, long rowCount) {
    super(TABLE, COLUMNS, rowCount);
    this.warehouseId = warehouseId;
    this.districtId = districtId;
    this.customerCount = customerCount;
  }

  @Override
  String createRow(long rowIndex) {
    return String.join(
        ",",
        ImmutableList.of(
            getId(rowIndex),
            String.valueOf(districtId),
            String.valueOf(warehouseId),
            getRandomCustomerId(),
            now(),
            getRandomCarrierId(rowIndex),
            getOrderLineCount(rowIndex),
            getAllLocal()));
  }

  String getRandomCustomerId() {
    return String.valueOf(Long.reverse(random.nextInt(customerCount)));
  }

  String getRandomCarrierId(long rowIndex) {
    // NOTE: Empty string == NULL
    return rowIndex % 3L == 0 ? "" : String.valueOf(random.nextInt(10));
  }

  String getOrderLineCount(long rowIndex) {
    return String.valueOf(Math.abs(Objects.hash(warehouseId, districtId, rowIndex) % 11) + 5);
  }

  String getAllLocal() {
    return "1";
  }
}
