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
