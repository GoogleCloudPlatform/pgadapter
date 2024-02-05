package com.google.cloud.pgadapter.benchmark.dataloader;

import java.io.Writer;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.codec.binary.Hex;

abstract class AbstractRowProducer {

  private final String table;
  private final String columns;
  private final long rowCount;
  private final Runnable rowCounterIncrementer;
  final Random random = new Random();

  AbstractRowProducer(String table, String columns, long rowCount, Runnable rowCounterIncrementer) {
    this.table = table;
    this.columns = columns;
    this.rowCount = rowCount;
    this.rowCounterIncrementer = rowCounterIncrementer;
  }

  String getTable() {
    return table;
  }

  String getColumns() {
    return columns;
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

  abstract String createRow(long rowIndex);

  String quote(String input) {
    return "'" + input + "'";
  }

  String getRandomString(int length) {
    int a = 97, z = 122;
    return random
        .ints(a, z + 1)
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  String getRandomLong() {
    return String.valueOf(random.nextLong());
  }

  String getRandomBoolean() {
    return String.valueOf(random.nextBoolean());
  }

  String getRandomDouble() {
    return String.valueOf(random.nextDouble());
  }

  int getRandomInt(int min, int max) {
    return random.nextInt(max - min + 1) + min;
  }

  String getRandomBytes(int min, int max) {
    return quote(getUnquotedRandomBytes(min, max));
  }

  private String getUnquotedRandomBytes(int min, int max) {
    byte[] result = new byte[getRandomInt(min, max)];
    random.nextBytes(result);
    return "\\x" + new String(Hex.encodeHex(result));
  }

  String getRandomDecimal(int precision) {
    return getRandomDecimal(1, precision);
  }

  String getRandomDecimal(int factor, int precision) {
    return BigDecimal.valueOf(random.nextDouble() * factor)
        .round(new MathContext(precision, RoundingMode.HALF_UP))
        .toPlainString();
  }

  String now() {
    return DateTimeFormatter.ISO_OFFSET_DATE_TIME
        .format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()))
        .replace('T', ' ');
  }

  String getRandomTimestamp() {
    return DateTimeFormatter.ISO_OFFSET_DATE_TIME
        .format(
            ZonedDateTime.ofInstant(
                Instant.ofEpochSecond(random.nextLong(0L, System.currentTimeMillis() / 1000L)),
                ZoneId.systemDefault()))
        .replace('T', ' ');
  }

  String getRandomDate() {
    return LocalDate.of(getRandomInt(1900, 2024), getRandomInt(1, 12), getRandomInt(1, 28))
        .toString();
  }

  String getRandomJsonb() {
    return quote(getUnquotedRandomJsonb());
  }

  private String getUnquotedRandomJsonb() {
    return String.format(
        "{\"key1\": \"%s\", \"key2\": \"%s\"}",
        getRandomString(getRandomInt(10, 30)), getRandomString(getRandomInt(10, 30)));
  }

  String getRandomLongArray() {
    return random
        .longs()
        .limit(getRandomInt(2, 10))
        .mapToObj(String::valueOf)
        .collect(Collectors.joining(",", "'{", "}'"));
  }

  String getRandomBooleanArray() {
    return random
        .ints()
        .limit(getRandomInt(2, 10))
        .mapToObj(i -> i % 2 == 0 ? "t" : "f")
        .collect(Collectors.joining(",", "'{", "}'"));
  }

  String getRandomBytesArray() {
    return IntStream.range(0, getRandomInt(2, 10))
        .mapToObj(i -> getUnquotedRandomBytes(10, 20))
        .collect(Collectors.joining(",", "'{", "}'"));
  }

  String getRandomDoubleArray() {
    return IntStream.range(0, getRandomInt(2, 10))
        .mapToObj(i -> getRandomDouble())
        .collect(Collectors.joining(",", "'{", "}'"));
  }

  String getRandomDecimalArray() {
    return IntStream.range(0, getRandomInt(2, 10))
        .mapToObj(i -> getRandomDecimal(1))
        .collect(Collectors.joining(",", "'{", "}'"));
  }

  String getRandomTimestampArray() {
    return IntStream.range(0, getRandomInt(2, 10))
        .mapToObj(i -> getRandomTimestamp())
        .collect(Collectors.joining(",", "'{", "}'"));
  }

  String getRandomDateArray() {
    return IntStream.range(0, getRandomInt(2, 10))
        .mapToObj(i -> getRandomDate())
        .collect(Collectors.joining(",", "'{", "}'"));
  }

  String getRandomStringArray() {
    return IntStream.range(0, getRandomInt(2, 10))
        .mapToObj(i -> getRandomString(getRandomInt(5, 15)))
        .collect(Collectors.joining(",", "'{", "}'"));
  }

  String getRandomJsonbArray() {
    return IntStream.range(0, getRandomInt(2, 10))
        .mapToObj(i -> "''" + getUnquotedRandomJsonb() + "''")
        .collect(Collectors.joining(",", "'{", "}'"));
  }
}
