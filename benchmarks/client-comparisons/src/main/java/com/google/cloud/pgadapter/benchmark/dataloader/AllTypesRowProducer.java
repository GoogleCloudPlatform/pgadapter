package com.google.cloud.pgadapter.benchmark.dataloader;

import com.google.common.collect.ImmutableList;

class AllTypesRowProducer extends AbstractRowProducer {
  private static final String TABLE = "benchmark_all_types";
  private static final String COLUMNS =
      """
      col_bigint,
      col_bool,
      col_bytea,
      col_float8,
      col_numeric,
      col_timestamptz,
      col_date,
      col_varchar,
      col_jsonb,
      col_array_bigint,
      col_array_bool,
      col_array_bytea,
      col_array_float8,
      col_array_numeric,
      col_array_timestamptz,
      col_array_date,
      col_array_varchar,
      col_array_jsonb
    """;

  AllTypesRowProducer(DataLoadStatus status, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incAllTypes);
  }

  @Override
  String createRow(long rowIndex) {
    return String.join(
        ",",
        ImmutableList.of(
            getRandomLong(),
            getRandomBoolean(),
            getRandomBytes(10, 1000),
            getRandomDouble(),
            getRandomDecimal(2),
            now(),
            getRandomDate(),
            quote(getRandomString(getRandomInt(20, 50))),
            getRandomJsonb(),
            getRandomLongArray(),
            getRandomBooleanArray(),
            getRandomBytesArray(),
            getRandomDoubleArray(),
            getRandomDecimalArray(),
            getRandomTimestampArray(),
            getRandomDateArray(),
            getRandomStringArray(),
            "null"));
  }
}
