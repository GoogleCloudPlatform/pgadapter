// Copyright 2020 Google LLC
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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.regex.Pattern;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to timestamp. */
public class TimestampParser extends Parser<Timestamp> {

  private static final int MICROSECONDS_IN_SECOND = 1000000;
  private static final long NANOSECONDS_IN_MICROSECONDS = 1000L;
  private static final char TIMESTAMP_SEPARATOR = 'T';
  private static final char EMPTY_SPACE = ' ';
  private static final String ZERO_TIMEZONE = "Z";
  private static final String PG_ZERO_TIMEZONE = "+00";

  /** Regular expression for parsing timestamps. */
  private static final String TIMESTAMP_REGEX =
      // yyyy-MM-dd
      "(\\d{4})-(\\d{2})-(\\d{2})"
          // ' 'HH:mm:ss.milliseconds
          + "( (\\d{2}):(\\d{2}):(\\d{2})(\\.\\d{1,9})?)"
          // 'Z' or time zone shift HH:mm following '+' or '-'
          + "([Zz]|([+-])(\\d{2})(:(\\d{2}))?)?";

  private static final Pattern TIMESTAMP_PATTERN = Pattern.compile(TIMESTAMP_REGEX);

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseLenient()
          .parseCaseInsensitive()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 9, true)
          .appendOffset("+HH:mm", "Z")
          .toFormatter();

  TimestampParser(ResultSet item, int position) {
    this.item = item.getTimestamp(position);
  }

  TimestampParser(Object item) {
    this.item = (Timestamp) item;
  }

  TimestampParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          String stringValue = toPGString(new String(item, StandardCharsets.UTF_8));
          TemporalAccessor temporalAccessor = TIMESTAMP_FORMATTER.parse(stringValue);
          this.item =
              Timestamp.ofTimeSecondsAndNanos(
                  temporalAccessor.getLong(ChronoField.INSTANT_SECONDS),
                  temporalAccessor.get(ChronoField.NANO_OF_SECOND));
          break;
        case BINARY:
          this.item = toTimestamp(item);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  /** Converts the binary data to a {@link Timestamp}. */
  public static Timestamp toTimestamp(byte[] data) {
    if (data.length < 8) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid length for timestamptz: " + data.length);
    }
    long pgMicros = ByteConverter.int8(data, 0);
    com.google.cloud.Timestamp ts = com.google.cloud.Timestamp.ofTimeMicroseconds(pgMicros);
    long javaSeconds = ts.getSeconds() + PG_EPOCH_SECONDS;
    int javaNanos = ts.getNanos();
    return Timestamp.ofTimeSecondsAndNanos(javaSeconds, javaNanos);
  }

  /**
   * Checks whether the given text contains a timestamp that can be parsed by PostgreSQL.
   *
   * @param value The value to check. May not be <code>null</code>.
   * @return <code>true</code> if the text contains a valid timestamp.
   */
  static boolean isTimestamp(String value) {
    Preconditions.checkNotNull(value);
    return TIMESTAMP_PATTERN.matcher(toPGString(value)).matches();
  }

  @Override
  public String stringParse() {
    return this.item == null ? null : toPGString(this.item.toString());
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
    long microseconds =
        ((this.item.getSeconds() - PG_EPOCH_SECONDS) * MICROSECONDS_IN_SECOND)
            + (this.item.getNanos() / NANOSECONDS_IN_MICROSECONDS);
    byte[] result = new byte[8];
    ByteConverter.int8(result, 0, microseconds);
    return result;
  }

  /**
   * Converts the given {@link Timestamp} to a text value that is understood by PostgreSQL. That
   * means a space delimiter between date and time values, and no trailing 'Z'.
   *
   * @return a {@link String} that can be interpreted by PostgreSQL.
   */
  private static String toPGString(String value) {
    return value.replace(TIMESTAMP_SEPARATOR, EMPTY_SPACE).replace(ZERO_TIMEZONE, PG_ZERO_TIMEZONE);
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
