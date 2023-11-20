package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.common.collect.ImmutableList;

class WarehouseRowProducer extends AbstractRowProducer {
  private static final String TABLE = "warehouse";
  private static final String COLUMNS =
      "w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd";

  WarehouseRowProducer(long rowCount) {
    super(TABLE, COLUMNS, rowCount);
  }

  @Override
  String createRow(long rowIndex) {
    return String.join(
        ",",
        ImmutableList.of(
            getId(rowIndex),
            getRandomName(),
            getRandomStreet(1),
            getRandomStreet(2),
            getRandomCity(),
            getRandomState(),
            getRandomZip(),
            getRandomTax(),
            "0.0"));
  }
}
