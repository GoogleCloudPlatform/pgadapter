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

import com.google.api.core.InternalApi;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to timestamp. */
@InternalApi
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

  private static final DateTimeFormatter TIMESTAMP_OUTPUT_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseLenient()
          .parseCaseInsensitive()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          // We should never return more than microsecond precision, as some PG clients, such as
          // SQLAlchemy will fail if they receive more.
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
          // Java 8 does not support seconds in timezone offset.
          .appendOffset(OptionsMetadata.isJava8() ? "+HH:mm" : "+HH:mm:ss", "+00")
          .toFormatter();

  private static final DateTimeFormatter TIMESTAMPTZ_INPUT_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseLenient()
          .parseCaseInsensitive()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          // Java 8 does not support seconds in timezone offset.
          .appendOffset(OptionsMetadata.isJava8() ? "+HH:mm" : "+HH:mm:ss", "+00:00:00")
          .toFormatter();
  private static final DateTimeFormatter TIMESTAMP_INPUT_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseLenient()
          .parseCaseInsensitive()
          .appendPattern("yyyy-MM-dd[[ ]['T']HH:mm[:ss][XXX]]")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter();

  private final SessionState sessionState;

  TimestampParser(ResultSet item, int position, SessionState sessionState) {
    this.item = item.getTimestamp(position);
    this.sessionState = sessionState;
  }

  TimestampParser(Object item, SessionState sessionState) {
    this.item = (Timestamp) item;
    this.sessionState = sessionState;
  }

  TimestampParser(byte[] item, FormatCode formatCode, SessionState sessionState) {
    this.sessionState = sessionState;
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          this.item = toTimestamp(new String(item, StandardCharsets.UTF_8), sessionState);
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
  public static Timestamp toTimestamp(@Nonnull byte[] data) {
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

  /** Converts the given string value to a {@link Timestamp}. */
  public static Timestamp toTimestamp(String value, SessionState sessionState) {
    try {
      String stringValue = toPGString(value);
      TemporalAccessor temporalAccessor = TIMESTAMPTZ_INPUT_FORMATTER.parse(stringValue);
      return Timestamp.ofTimeSecondsAndNanos(
          temporalAccessor.getLong(ChronoField.INSTANT_SECONDS),
          temporalAccessor.get(ChronoField.NANO_OF_SECOND));
    } catch (Exception ignore) {
      try {
        TemporalAccessor temporalAccessor =
            TIMESTAMP_INPUT_FORMATTER.parseBest(
                value, ZonedDateTime::from, LocalDateTime::from, LocalDate::from);
        ZonedDateTime zonedDateTime = null;
        if (temporalAccessor instanceof ZonedDateTime) {
          zonedDateTime = (ZonedDateTime) temporalAccessor;
        } else if (temporalAccessor instanceof LocalDateTime) {
          LocalDateTime localDateTime = (LocalDateTime) temporalAccessor;
          zonedDateTime = localDateTime.atZone(sessionState.getTimezone());
        } else if (temporalAccessor instanceof LocalDate) {
          LocalDate localDate = (LocalDate) temporalAccessor;
          zonedDateTime = localDate.atStartOfDay().atZone(sessionState.getTimezone());
        }
        if (zonedDateTime != null) {
          return Timestamp.ofTimeSecondsAndNanos(
              zonedDateTime.getLong(ChronoField.INSTANT_SECONDS),
              zonedDateTime.get(ChronoField.NANO_OF_SECOND));
        }
        throw PGExceptionFactory.newPGException("Invalid timestamp value: " + value);
      } catch (Exception exception) {
        throw PGExceptionFactory.newPGException("Invalid timestamp value: " + value);
      }
    }
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
    return this.item == null ? null : toPGString(this.item, sessionState.getTimezone());
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
    return convertToPG(this.item);
  }

  static byte[] convertToPG(Timestamp value) {
    long microseconds =
        ((value.getSeconds() - PG_EPOCH_SECONDS) * MICROSECONDS_IN_SECOND)
            + (value.getNanos() / NANOSECONDS_IN_MICROSECONDS);
    byte[] result = new byte[8];
    ByteConverter.int8(result, 0, microseconds);
    return result;
  }

  public static byte[] convertToPG(
      ResultSet resultSet, int position, DataFormat format, ZoneId zoneId) {
    switch (format) {
      case SPANNER:
        return resultSet.getTimestamp(position).toString().getBytes(StandardCharsets.UTF_8);
      case POSTGRESQL_TEXT:
        return toPGString(resultSet.getTimestamp(position), zoneId)
            .getBytes(StandardCharsets.UTF_8);
      case POSTGRESQL_BINARY:
        return convertToPG(resultSet.getTimestamp(position));
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
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

  private static String toPGString(Timestamp value, ZoneId zoneId) {
    OffsetDateTime offsetDateTime =
        OffsetDateTime.ofInstant(
            Instant.ofEpochSecond(value.getSeconds(), value.getNanos()), zoneId);
    return TIMESTAMP_OUTPUT_FORMATTER.format(offsetDateTime);
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
