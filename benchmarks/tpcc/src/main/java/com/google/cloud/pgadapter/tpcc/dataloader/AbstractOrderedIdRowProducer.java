package com.google.cloud.pgadapter.tpcc.dataloader;

abstract class AbstractOrderedIdRowProducer extends AbstractRowProducer {

  AbstractOrderedIdRowProducer(
      String table, String columns, long rowCount, Runnable rowCounterIncrementer) {
    super(table, columns, rowCount, rowCounterIncrementer);
  }

  @Override
  String getId(long rowIndex) {
    return String.valueOf(rowIndex);
  }
}
