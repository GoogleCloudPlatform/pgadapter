package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.common.collect.ImmutableList;

class DistrictRowProducer extends AbstractRowProducer {
  private static final String TABLE = "district";
  private static final String COLUMNS =
      """
      d_id,
      w_id,
      d_name,
      d_street_1,
      d_street_2,
      d_city,
      d_state,
      d_zip,
      d_tax,
      d_ytd,
      d_next_o_id""";

  private final long warehouseId;

  DistrictRowProducer(DataLoadStatus status, long warehouseId, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incDistrict);
    this.warehouseId = warehouseId;
  }

  @Override
  String createRow(long rowIndex) {
    return String.join(
        ",",
        ImmutableList.of(
            getId(rowIndex),
            String.valueOf(warehouseId),
            getRandomName(),
            getRandomStreet(1),
            getRandomStreet(2),
            getRandomCity(),
            getRandomState(),
            getRandomZip(),
            getRandomTax(),
            "0.0",
            "3001"));
  }
}
