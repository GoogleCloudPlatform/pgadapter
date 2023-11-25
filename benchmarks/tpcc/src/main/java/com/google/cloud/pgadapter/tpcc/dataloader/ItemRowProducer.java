package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.common.collect.ImmutableList;

class ItemRowProducer extends AbstractRowProducer {
  private static final String TABLE = "item";
  private static final String COLUMNS =
      """
    i_id,
    i_im_id,
    i_name,
    i_price,
    i_data
    """;

  ItemRowProducer(DataLoadStatus status, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incItem);
  }

  @Override
  String createRow(long rowIndex) {
    return String.join(
        ",",
        ImmutableList.of(
            getId(rowIndex), getRandomImId(), getName(rowIndex), getRandomPrice(), getData()));
  }

  String getRandomImId() {
    return getRandomInt(1, 10000);
  }

  String getRandomPrice() {
    return getRandomDecimal(100, 2);
  }

  String getName(long rowIndex) {
    return quote(String.format("item-%d-%s", rowIndex, getRandomString(10)));
  }

  String getData() {
    return quote(getRandomString(30));
  }
}
