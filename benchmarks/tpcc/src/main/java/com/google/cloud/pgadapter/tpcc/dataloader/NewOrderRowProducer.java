package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.cloud.pgadapter.tpcc.dataloader.OrderRowProducer.DistrictId;
import com.google.common.collect.ImmutableList;

class NewOrderRowProducer extends AbstractOrderedIdRowProducer {
  private static final String TABLE = "new_orders";
  private static final String COLUMNS = """
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
