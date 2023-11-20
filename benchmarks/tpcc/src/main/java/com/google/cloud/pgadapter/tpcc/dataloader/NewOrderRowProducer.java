package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.common.collect.ImmutableList;

class NewOrderRowProducer extends AbstractRowProducer {
  private static final String TABLE = "new_orders";
  private static final String COLUMNS = """
    no_o_id,
    no_d_id,
    no_w_id
    """;

  private final long warehouseId;

  private final long districtId;

  NewOrderRowProducer(long warehouseId, long districtId, long rowCount) {
    super(TABLE, COLUMNS, rowCount);
    this.warehouseId = warehouseId;
    this.districtId = districtId;
  }

  @Override
  String createRow(long rowIndex) {
    if (rowIndex % 3L == 0) {
      return String.join(
          ",",
          ImmutableList.of(
              getId(rowIndex), String.valueOf(districtId), String.valueOf(warehouseId)));
    }
    return null;
  }
}
