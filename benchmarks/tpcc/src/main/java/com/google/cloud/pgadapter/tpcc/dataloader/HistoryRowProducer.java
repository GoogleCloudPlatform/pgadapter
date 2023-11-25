package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.common.collect.ImmutableList;

class HistoryRowProducer extends AbstractRowProducer {
  private static final String TABLE = "history";
  private static final String COLUMNS =
      """
    h_c_id,
    h_c_d_id,
    h_c_w_id,
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
