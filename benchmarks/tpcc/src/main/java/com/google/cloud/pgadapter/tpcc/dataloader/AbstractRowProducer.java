// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.common.collect.ImmutableList;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

abstract class AbstractRowProducer {

  private final String table;
  private final String columns;
  private final List<String> columnList;
  private final long rowCount;
  private final Runnable rowCounterIncrementer;
  protected Long warehouseId = null;
  protected Long districtId = null;
  final Random random = new Random();

  AbstractRowProducer(String table, String columns, long rowCount, Runnable rowCounterIncrementer) {
    this.table = table;
    this.columns = columns;
    String[] parts = columns.split(",");
    for (int i = 0; i < parts.length; i++) {
      parts[i] = parts[i].trim().replace("\n", "");
    }
    this.columnList = Arrays.asList(parts);
    this.rowCount = rowCount;
    this.rowCounterIncrementer = rowCounterIncrementer;
  }

  void incRowCounterIncrementer(long count) {
    for (int i = 0; i < count; i++) {
      rowCounterIncrementer.run();
    }
  }

  Long getWarehouseId() {
    return warehouseId;
  }

  Long getDistrictId() {
    return districtId;
  }

  String getTable() {
    return table;
  }

  String getColumns() {
    return columns;
  }

  List<String> getColumnsAsList() {
    return columnList;
  }

  long getRowCount() {
    return rowCount;
  }

  Future<Long> asyncWriteRows(ExecutorService executor, Writer writer) {
    return executor.submit(
        () -> {
          try (writer) {
            for (long rowIndex = 0L; rowIndex < rowCount; rowIndex++) {
              String row = createRow(rowIndex);
              if (row != null) {
                writer.write(row + "\n");
                rowCounterIncrementer.run();
              }
            }
            return rowCount;
          }
        });
  }

  String createRow(long rowIndex) {
    List<ImmutableList> list = createRowsAsList(rowIndex);
    if (list == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < list.size(); i++) {
      if (i > 0) {
        builder.append("\n");
      }
      builder.append(String.join(",", list.get(i)));
    }
    return builder.toString();
  }

  abstract List<ImmutableList> createRowsAsList(long rowIndex);

  String quote(String input) {
    return "'" + input + "'";
  }

  String getId(long rowIndex) {
    return String.valueOf(Long.reverse(rowIndex));
  }

  String getRandomName() {
    return quote("name-" + getRandomString(5));
  }

  String getRandomStreet(int index) {
    return quote("street" + index + "-" + getRandomString(5));
  }

  String getRandomCity() {
    return quote("city-" + getRandomString(8));
  }

  String getRandomState() {
    return quote(getRandomString(2));
  }

  String getRandomZip() {
    return quote(getRandomInt(1000, 9999) + getRandomString(2));
  }

  String getRandomTax() {
    return getRandomDecimal(2).toPlainString();
  }

  String getRandomString(int length) {
    int a = 97, z = 122;
    return random
        .ints(a, z + 1)
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  String getRandomInt(int min, int max) {
    return String.valueOf(random.nextInt(max - min + 1) + min);
  }

  BigDecimal getRandomDecimal(int precision) {
    return getRandomDecimal(1, precision);
  }

  BigDecimal getRandomDecimal(int factor, int precision) {
    return BigDecimal.valueOf(random.nextDouble() * factor)
        .round(new MathContext(precision, RoundingMode.HALF_UP));
  }

  String getRandomPhone() {
    return getRandomInt(100_000_000, 999_999_999);
  }

  String nowAsString() {
    return DateTimeFormatter.ISO_OFFSET_DATE_TIME
        .format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()))
        .replace('T', ' ');
  }
}
