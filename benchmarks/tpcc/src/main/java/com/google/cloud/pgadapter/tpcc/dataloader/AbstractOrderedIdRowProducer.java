package com.google.cloud.pgadapter.tpcc.dataloader;

abstract class AbstractOrderedIdRowProducer extends AbstractRowProducer {

  AbstractOrderedIdRowProducer(String table, String columns, long rowCount) {
    super(table, columns, rowCount);
  }

  @Override
  String getId(long rowIndex) {
    return String.valueOf(rowIndex);
  }
}
