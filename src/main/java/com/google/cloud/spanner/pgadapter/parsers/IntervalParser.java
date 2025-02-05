// Copyright 2025 Google LLC
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

package com.google.cloud.spanner.pgadapter.parsers;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Interval;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Locale;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;
import org.postgresql.util.PGInterval;

/** Translate from wire protocol to interval. */
@InternalApi
public class IntervalParser extends Parser<Interval> {
  private static final BigDecimal NANOS_PER_SECOND = new BigDecimal(Interval.NANOS_PER_SECOND);

  IntervalParser(ResultSet item, int position) {
    this.item = item.getInterval(position);
  }

  IntervalParser(Object item) {
    this.item = (Interval) item;
  }

  IntervalParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          this.item = toInterval(new String(item, StandardCharsets.UTF_8));
          break;
        case BINARY:
          this.item = toInterval(item);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  /** Converts the binary data to an {@link Interval}. */
  public static Interval toInterval(@Nonnull byte[] data) {
    if (data.length < 16) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid length for interval: " + data.length);
    }

    long pgMicros = ByteConverter.int8(data, 0);
    BigInteger nanos = new BigInteger(pgMicros + "000");
    int pgDays = ByteConverter.int4(data, 8);
    int pgMonths = ByteConverter.int4(data, 12);
    return Interval.fromMonthsDaysNanos(pgMonths, pgDays, nanos);
  }

  /** Converts the given string value to a {@link Interval}. */
  public static Interval toInterval(@Nonnull String value) {
    PGInterval pgInterval = new PGInterval();
    try {
      pgInterval.setValue(value);
    } catch (SQLException exception) {
      throw PGExceptionFactory.newPGException(
          "Invalid interval value: " + value, SQLState.SyntaxError);
    }
    String isoValue =
        String.format(
            "P%dY%dM%dDT%dH%dM%d.%dS",
            pgInterval.getYears(),
            pgInterval.getMonths(),
            pgInterval.getDays(),
            pgInterval.getHours(),
            pgInterval.getMinutes(),
            pgInterval.getWholeSeconds(),
            Interval.NANOS_PER_MICRO * pgInterval.getMicroSeconds());
    return Interval.parseFromString(isoValue);
  }

  @Override
  public String stringParse() {
    return this.item == null ? null : toPGString(this.item);
  }

  @Override
  protected String spannerParse() {
    return this.item == null ? null : item.toString();
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    return convertToPGBinary(this.item);
  }

  static byte[] convertToPGBinary(Interval value) {
    long microseconds = value.getNanoseconds().divide(Interval.NANOS_PER_MICROSECOND).longValue();
    int days = value.getDays();
    int months = value.getMonths();
    byte[] result = new byte[16];
    ByteConverter.int8(result, 0, microseconds);
    ByteConverter.int4(result, 8, days);
    ByteConverter.int4(result, 12, months);
    return result;
  }

  public static byte[] convertToPG(ResultSet resultSet, int position, DataFormat format) {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        return toPGString(resultSet.getInterval(position)).getBytes(StandardCharsets.UTF_8);
      case POSTGRESQL_BINARY:
        return convertToPGBinary(resultSet.getInterval(position));
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  private static String toPGString(Interval value) {
    BigInteger nanos = value.getNanoseconds();
    BigInteger[] hours = nanos.divideAndRemainder(Interval.NANOS_PER_HOUR);
    nanos = hours[1];
    BigInteger[] minutes = nanos.divideAndRemainder(Interval.NANOS_PER_MINUTE);
    nanos = minutes[1];
    BigInteger[] seconds = nanos.divideAndRemainder(Interval.NANOS_PER_SECOND);
    nanos = seconds[1];
    long micros = Math.abs(nanos.longValueExact() / Interval.NANOS_PER_MICRO);
    return String.format(
        Locale.ROOT,
        "%d mons %d days %02d:%02d:%s.%06d",
        value.getMonths(),
        value.getDays(),
        hours[0],
        minutes[0],
        seconds[0],
        micros);
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
