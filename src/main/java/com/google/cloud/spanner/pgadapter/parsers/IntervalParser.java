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
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.collect.ImmutableMap;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to interval. */
@InternalApi
public class IntervalParser extends Parser<Interval> {
  private static final long MONTHS_PER_YEAR = 12;
  private static final long MINUTES_PER_HOUR = 60;
  private static final long SECONDS_PER_MINUTE = 60;
  private static final long SECONDS_PER_HOUR = MINUTES_PER_HOUR * SECONDS_PER_MINUTE;
  private static final long MILLIS_PER_SECOND = 1000;
  private static final long MICROS_PER_MILLI = 1000;
  private static final long NANOS_PER_MICRO = 1000;
  private static final BigInteger NANOS_PER_MICRO_BIG_INTEGER = BigInteger.valueOf(NANOS_PER_MICRO);
  private static final long MICROS_PER_SECOND = MICROS_PER_MILLI * MILLIS_PER_SECOND;
  private static final long MICROS_PER_MINUTE = SECONDS_PER_MINUTE * MICROS_PER_SECOND;
  private static final long MICROS_PER_HOUR = SECONDS_PER_HOUR * MICROS_PER_SECOND;
  private static final BigInteger NANOS_PER_MILLI =
      BigInteger.valueOf(MICROS_PER_MILLI * NANOS_PER_MICRO);
  private static final BigInteger NANOS_PER_SECOND =
      BigInteger.valueOf(MICROS_PER_SECOND * NANOS_PER_MICRO);
  private static final BigInteger NANOS_PER_MINUTE =
      BigInteger.valueOf(MICROS_PER_MINUTE * NANOS_PER_MICRO);
  private static final BigInteger NANOS_PER_HOUR =
      BigInteger.valueOf(MICROS_PER_HOUR * NANOS_PER_MICRO);
  private static final BigInteger NANOS_PER_DAY = BigInteger.valueOf(24).multiply(NANOS_PER_HOUR);
  private static final BigDecimal DECIMAL_NANOS_PER_HOUR = new BigDecimal(NANOS_PER_HOUR);
  private static final BigDecimal DECIMAL_NANOS_PER_MINUTE = new BigDecimal(NANOS_PER_MINUTE);
  private static final BigDecimal DECIMAL_NANOS_PER_SECOND = new BigDecimal(NANOS_PER_SECOND);
  private static final BigDecimal DECIMAL_NANOS_PER_MILLI = new BigDecimal(NANOS_PER_MILLI);
  private static final BigDecimal DECIMAL_NANOS_PER_MICRO =
      new BigDecimal(NANOS_PER_MICRO_BIG_INTEGER);
  private static final BigDecimal DECIMAL_NANOS_PER_DAY = new BigDecimal(NANOS_PER_DAY);

  private static final Pattern TOKEN_PATTERN =
      Pattern.compile(
          "([+-]?\\d+:\\d+(?::\\d+(?:\\.\\d+)?)?)" // Group 1: TIME
              + "|([+-]?\\d+-\\d+(?:-\\d+)+)" // Group 2: INVALID_DATE (e.g. 1-2-3)
              + "|([+-]?\\d+-\\d+)" // Group 3: YEAR_MONTH (e.g. 1-2)
              + "|([+-]?\\d+(?:\\.\\d+)?|\\.\\d+)" // Group 4: NUMBER
              + "|([+-])" // Group 5: SIGN
              + "|([a-zA-Z]+)" // Group 6: WORD
              + "|(\\S+)" // Group 7: UNKNOWN
          );

  private static final Pattern ISO8601_PATTERN =
      Pattern.compile(
          "^([+-])?P(?:(-?\\d+(?:[.,]\\d+)?)Y)?(?:(-?\\d+(?:[.,]\\d+)?)M)?(?:(-?\\d+(?:[.,]\\d+)?)W)?(?:(-?\\d+(?:[.,]\\d+)?)D)?(T(?:(-?\\d+(?:[.,]\\d+)?)H)?(?:(-?\\d+(?:[.,]\\d+)?)M)?(?:(-?\\d+(?:[.,]\\d+)?)S)?)?$",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern ISO8601_ALTERNATIVE_PATTERN =
      Pattern.compile(
          "^([+-])?P(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2}):(\\d{2}(?:[.,]\\d+)?)$",
          Pattern.CASE_INSENSITIVE);

  private enum UnitType {
    MILLENNIUM,
    CENTURY,
    DECADE,
    YEAR,
    MONTH,
    WEEK,
    DAY,
    HOUR,
    MINUTE,
    SECOND,
    MILLISECOND,
    MICROSECOND
  }

  IntervalParser(ResultSet item, int position) {
    this.item = item.getInterval(position);
  }

  IntervalParser(Object item) {
    this.item = (Interval) item;
  }

  IntervalParser(byte[] item, FormatCode formatCode) {
    this.item = toInterval(item, formatCode);
  }

  /** Converts the given data to an {@link Interval} based on the format code. */
  public static Interval toInterval(byte[] item, FormatCode formatCode) {
    if (item == null) {
      return null;
    }
    switch (formatCode) {
      case TEXT:
        return toInterval(new String(item, StandardCharsets.UTF_8));
      case BINARY:
        return toInterval(item);
      default:
        throw new IllegalArgumentException("Unsupported format: " + formatCode);
    }
  }

  /** Converts the binary data to an {@link Interval}. */
  public static Interval toInterval(@Nonnull byte[] data) {
    if (data.length < 16) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid length for interval: " + data.length);
    }

    long pgMicros = ByteConverter.int8(data, 0);
    BigInteger nanos = BigInteger.valueOf(pgMicros).multiply(NANOS_PER_MICRO_BIG_INTEGER);
    int pgDays = ByteConverter.int4(data, 8);
    int pgMonths = ByteConverter.int4(data, 12);
    return Interval.fromMonthsDaysNanos(pgMonths, pgDays, nanos);
  }

  /** Converts the given string value to a {@link Interval}. */
  public static Interval toInterval(String value) {
    if (value == null) {
      throw PGExceptionFactory.newPGException("Invalid interval value: null", SQLState.SyntaxError);
    }
    try {
      return parseInterval(value);
    } catch (IllegalArgumentException | ArithmeticException | IndexOutOfBoundsException exception) {
      throw PGExceptionFactory.newPGException(
          "Invalid interval value: " + value, SQLState.SyntaxError);
    }
  }

  private static Interval parseInterval(String value) {
    String intervalString = value.trim();
    if (intervalString.isEmpty()) {
      throw new IllegalArgumentException("Empty interval");
    }

    Matcher isoMatcher = ISO8601_PATTERN.matcher(intervalString);
    if (isoMatcher.matches()) {
      return parseIso8601(isoMatcher);
    }
    Matcher isoAlternativeMatcher = ISO8601_ALTERNATIVE_PATTERN.matcher(intervalString);
    if (isoAlternativeMatcher.matches()) {
      return parseIso8601Alternative(isoAlternativeMatcher);
    }

    return parsePostgresFormat(intervalString);
  }

  private static Interval parsePostgresFormat(String intervalString) {
    if (intervalString.startsWith("@")) {
      intervalString = intervalString.substring(1).trim();
      if (intervalString.isEmpty()) {
        throw new IllegalArgumentException("Empty interval after '@'");
      }
    }

    boolean isAgo = false;
    if (intervalString.length() >= 3
        && intervalString.regionMatches(true, intervalString.length() - 3, "ago", 0, 3)) {
      if (intervalString.length() == 3) {
        throw new IllegalArgumentException("Interval cannot be just 'ago'");
      }
      char beforeAgo = intervalString.charAt(intervalString.length() - 4);
      if (Character.isWhitespace(beforeAgo)) {
        isAgo = true;
        intervalString = intervalString.substring(0, intervalString.length() - 4).trim();
      }
    }

    if (intervalString.indexOf(',') >= 0) {
      intervalString = intervalString.replace(',', ' ');
    }
    Matcher matcher = TOKEN_PATTERN.matcher(intervalString);
    IntervalAccumulator accumulator = new IntervalAccumulator(isAgo);

    boolean hasTokens = false;
    while (matcher.find()) {
      hasTokens = true;
      accumulator.processToken(matcher);
    }
    if (!hasTokens) {
      throw new IllegalArgumentException("Invalid interval input: " + intervalString);
    }

    return accumulator.build();
  }

  private static final class IntervalAccumulator {
    private final boolean isAgo;
    private long totalMonths;
    private long totalDays;
    private BigInteger totalNanos = BigInteger.ZERO;
    private final EnumSet<UnitType> seenUnits = EnumSet.noneOf(UnitType.class);
    private int pendingSign;
    private BigDecimal pendingNumber;

    IntervalAccumulator(boolean isAgo) {
      this.isAgo = isAgo;
    }

    void processToken(Matcher matcher) {
      String timeToken = matcher.group(1);
      String invalidDate = matcher.group(2);
      String yearMonthToken = matcher.group(3);
      String numberToken = matcher.group(4);
      String signToken = matcher.group(5);
      String wordToken = matcher.group(6);
      String unknown = matcher.group(7);

      if (invalidDate != null || unknown != null) {
        throw new IllegalArgumentException(
            "Invalid token: " + (invalidDate != null ? invalidDate : unknown));
      }

      if (signToken != null) {
        processSign(signToken);
      } else if (timeToken != null) {
        processTime(timeToken);
      } else if (yearMonthToken != null) {
        processYearMonth(yearMonthToken);
      } else if (numberToken != null) {
        processNumber(numberToken);
      } else {
        processWord(wordToken);
      }
    }

    private void processSign(String signToken) {
      if (pendingSign != 0 || pendingNumber != null) {
        throw new IllegalArgumentException("Invalid sign placement: " + signToken);
      }
      pendingSign = signToken.equals("-") ? -1 : 1;
    }

    private void processTime(String timeToken) {
      if (pendingSign != 0 && (timeToken.startsWith("-") || timeToken.startsWith("+"))) {
        throw new IllegalArgumentException("Consecutive signs are not allowed: " + timeToken);
      }
      if (pendingNumber != null) {
        if (seenUnits.contains(UnitType.DAY)) {
          throw new IllegalArgumentException("Duplicate unit: DAY");
        }
        seenUnits.add(UnitType.DAY);
        addDays(pendingNumber);
        pendingNumber = null;
      }

      if (seenUnits.contains(UnitType.HOUR)
          || seenUnits.contains(UnitType.MINUTE)
          || seenUnits.contains(UnitType.SECOND)
          || seenUnits.contains(UnitType.MILLISECOND)
          || seenUnits.contains(UnitType.MICROSECOND)) {
        throw new IllegalArgumentException("Conflicting time string: " + timeToken);
      }
      seenUnits.add(UnitType.HOUR);
      seenUnits.add(UnitType.MINUTE);
      seenUnits.add(UnitType.SECOND);
      seenUnits.add(UnitType.MILLISECOND);
      seenUnits.add(UnitType.MICROSECOND);

      int sign = 1;
      String timeString = timeToken;
      if (timeString.startsWith("-")) {
        sign = -1;
        timeString = timeString.substring(1);
      } else if (timeString.startsWith("+")) {
        timeString = timeString.substring(1);
      }
      if (pendingSign != 0) {
        sign *= pendingSign;
        pendingSign = 0;
      }

      String[] parts = timeString.split(":");
      long hours = Long.parseLong(parts[0]);
      long minutes = Long.parseLong(parts[1]);
      long seconds = 0;
      long fractionNanos = 0;
      if (parts.length > 2) {
        String secondPart = parts[2];
        int dotIndex = secondPart.indexOf('.');
        if (dotIndex >= 0) {
          seconds = Long.parseLong(secondPart.substring(0, dotIndex));
          String fraction = secondPart.substring(dotIndex + 1);
          if (fraction.length() > 9) {
            fraction = fraction.substring(0, 9);
          }
          long fractionValue = Long.parseLong(fraction);
          for (int i = fraction.length(); i < 9; i++) {
            fractionValue *= 10;
          }
          fractionNanos = fractionValue;
        } else {
          seconds = Long.parseLong(secondPart);
        }
      }
      BigInteger timeNanos =
          BigInteger.valueOf(hours)
              .multiply(NANOS_PER_HOUR)
              .add(BigInteger.valueOf(minutes).multiply(NANOS_PER_MINUTE))
              .add(BigInteger.valueOf(seconds).multiply(NANOS_PER_SECOND))
              .add(BigInteger.valueOf(fractionNanos));
      if (sign < 0) {
        timeNanos = timeNanos.negate();
      }
      totalNanos = totalNanos.add(timeNanos);
    }

    private void processYearMonth(String yearMonthToken) {
      if (pendingSign != 0 && (yearMonthToken.startsWith("-") || yearMonthToken.startsWith("+"))) {
        throw new IllegalArgumentException("Consecutive signs are not allowed: " + yearMonthToken);
      }
      if (pendingNumber != null) {
        throw new IllegalArgumentException("Unexpected number before year-month: " + pendingNumber);
      }
      if (seenUnits.contains(UnitType.MONTH)) {
        throw new IllegalArgumentException(
            "Conflicting or duplicate month in year-month: " + yearMonthToken);
      }
      seenUnits.add(UnitType.MONTH);

      int sign = 1;
      String yearMonthString = yearMonthToken;
      if (yearMonthString.startsWith("-")) {
        sign = -1;
        yearMonthString = yearMonthString.substring(1);
      } else if (yearMonthString.startsWith("+")) {
        yearMonthString = yearMonthString.substring(1);
      }
      if (pendingSign != 0) {
        sign *= pendingSign;
        pendingSign = 0;
      }

      String[] parts = yearMonthString.split("-");
      long years = Long.parseLong(parts[0]);
      long months = Long.parseLong(parts[1]);
      long deltaMonths =
          Math.multiplyExact(
              (long) sign, Math.addExact(Math.multiplyExact(years, MONTHS_PER_YEAR), months));
      totalMonths = Math.addExact(totalMonths, deltaMonths);
    }

    private void processNumber(String numberToken) {
      if (pendingNumber != null) {
        throw new IllegalArgumentException(
            "Two consecutive numbers without unit: " + pendingNumber + ", " + numberToken);
      }
      if (pendingSign != 0
          && (numberToken.startsWith("-")
              || numberToken.startsWith("+")
              || numberToken.startsWith("."))) {
        throw new IllegalArgumentException(
            "Consecutive signs or signed leading dot are not allowed: " + numberToken);
      }
      BigDecimal number = new BigDecimal(numberToken);
      if (pendingSign != 0) {
        number = number.multiply(BigDecimal.valueOf(pendingSign));
        pendingSign = 0;
      }
      pendingNumber = number;
    }

    private void processWord(String wordToken) {
      if (pendingNumber == null) {
        throw new IllegalArgumentException("Unit without number: " + wordToken);
      }
      UnitType unit = parseUnit(wordToken);
      if (seenUnits.contains(unit)) {
        throw new IllegalArgumentException("Duplicate unit: " + wordToken);
      }
      seenUnits.add(unit);

      BigDecimal valueAmount = pendingNumber;
      pendingNumber = null;
      applyUnit(unit, valueAmount);
    }

    void applyUnit(UnitType unit, BigDecimal valueAmount) {
      switch (unit) {
        case MILLENNIUM:
          totalMonths =
              Math.addExact(
                  totalMonths,
                  valueAmount.multiply(BigDecimal.valueOf(12_000)).toBigInteger().longValueExact());
          break;
        case CENTURY:
          totalMonths =
              Math.addExact(
                  totalMonths,
                  valueAmount.multiply(BigDecimal.valueOf(1_200)).toBigInteger().longValueExact());
          break;
        case DECADE:
          totalMonths =
              Math.addExact(
                  totalMonths,
                  valueAmount.multiply(BigDecimal.valueOf(120)).toBigInteger().longValueExact());
          break;
        case YEAR:
          addYears(valueAmount);
          break;
        case MONTH:
          addMonths(valueAmount);
          break;
        case WEEK:
          addWeeks(valueAmount);
          break;
        case DAY:
          addDays(valueAmount);
          break;
        case HOUR:
          totalNanos = totalNanos.add(valueAmount.multiply(DECIMAL_NANOS_PER_HOUR).toBigInteger());
          break;
        case MINUTE:
          totalNanos =
              totalNanos.add(valueAmount.multiply(DECIMAL_NANOS_PER_MINUTE).toBigInteger());
          break;
        case SECOND:
          if (valueAmount.remainder(BigDecimal.ONE).signum() != 0) {
            if (seenUnits.contains(UnitType.MILLISECOND)
                || seenUnits.contains(UnitType.MICROSECOND)) {
              throw new IllegalArgumentException(
                  "Conflicting fractional seconds with sub-second units: " + valueAmount);
            }
            seenUnits.add(UnitType.MILLISECOND);
            seenUnits.add(UnitType.MICROSECOND);
          }
          totalNanos =
              totalNanos.add(valueAmount.multiply(DECIMAL_NANOS_PER_SECOND).toBigInteger());
          break;
        case MILLISECOND:
          totalNanos = totalNanos.add(valueAmount.multiply(DECIMAL_NANOS_PER_MILLI).toBigInteger());
          break;
        case MICROSECOND:
          totalNanos = totalNanos.add(valueAmount.multiply(DECIMAL_NANOS_PER_MICRO).toBigInteger());
          break;
      }
    }

    void addYears(BigDecimal valueAmount) {
      BigDecimal totalMonthsDecimal = valueAmount.multiply(BigDecimal.valueOf(MONTHS_PER_YEAR));
      totalMonths = Math.addExact(totalMonths, totalMonthsDecimal.toBigInteger().longValueExact());
      BigDecimal fractionalMonthsFromYear = totalMonthsDecimal.remainder(BigDecimal.ONE);
      if (fractionalMonthsFromYear.signum() != 0) {
        addMonths(fractionalMonthsFromYear);
      }
    }

    void addMonths(BigDecimal valueAmount) {
      totalMonths = Math.addExact(totalMonths, valueAmount.toBigInteger().longValueExact());
      BigDecimal fractionalMonths = valueAmount.remainder(BigDecimal.ONE);
      if (fractionalMonths.signum() != 0) {
        BigDecimal daysFromFraction = fractionalMonths.multiply(BigDecimal.valueOf(30));
        addDays(daysFromFraction);
      }
    }

    void addWeeks(BigDecimal valueAmount) {
      BigDecimal daysFromWeeks = valueAmount.multiply(BigDecimal.valueOf(7));
      addDays(daysFromWeeks);
    }

    void addDays(BigDecimal valueAmount) {
      totalDays = Math.addExact(totalDays, valueAmount.toBigInteger().longValueExact());
      BigDecimal fractionalDays = valueAmount.remainder(BigDecimal.ONE);
      if (fractionalDays.signum() != 0) {
        totalNanos = totalNanos.add(fractionalDays.multiply(DECIMAL_NANOS_PER_DAY).toBigInteger());
      }
    }

    Interval build() {
      if (pendingSign != 0) {
        throw new IllegalArgumentException("Dangling sign at end of interval");
      }
      if (pendingNumber != null) {
        if (seenUnits.contains(UnitType.SECOND)
            || seenUnits.contains(UnitType.MILLISECOND)
            || seenUnits.contains(UnitType.MICROSECOND)) {
          throw new IllegalArgumentException(
              "Conflicting trailing number with seconds: " + pendingNumber);
        }
        totalNanos =
            totalNanos.add(pendingNumber.multiply(DECIMAL_NANOS_PER_SECOND).toBigInteger());
      }

      if (isAgo) {
        totalMonths = Math.negateExact(totalMonths);
        totalDays = Math.negateExact(totalDays);
        totalNanos = totalNanos.negate();
      }

      return Interval.fromMonthsDaysNanos(
          Math.toIntExact(totalMonths), Math.toIntExact(totalDays), totalNanos);
    }
  }

  private static UnitType parseUnit(String unitString) {
    String unit = unitString.toLowerCase(Locale.ROOT);
    switch (unit) {
      case "millennium":
      case "millennia":
      case "millenniums":
      case "mil":
      case "mils":
        return UnitType.MILLENNIUM;
      case "century":
      case "centuries":
      case "cent":
      case "c":
        return UnitType.CENTURY;
      case "decade":
      case "decades":
      case "dec":
      case "decs":
        return UnitType.DECADE;
      case "year":
      case "years":
      case "yr":
      case "yrs":
      case "y":
        return UnitType.YEAR;
      case "month":
      case "months":
      case "mon":
      case "mons":
        return UnitType.MONTH;
      case "week":
      case "weeks":
      case "w":
        return UnitType.WEEK;
      case "day":
      case "days":
      case "d":
        return UnitType.DAY;
      case "hour":
      case "hours":
      case "hr":
      case "hrs":
      case "h":
        return UnitType.HOUR;
      case "minute":
      case "minutes":
      case "min":
      case "mins":
      case "m":
        return UnitType.MINUTE;
      case "second":
      case "seconds":
      case "sec":
      case "secs":
      case "s":
        return UnitType.SECOND;
      case "millisecond":
      case "milliseconds":
      case "msec":
      case "msecs":
      case "ms":
        return UnitType.MILLISECOND;
      case "microsecond":
      case "microseconds":
      case "usec":
      case "usecs":
      case "us":
        return UnitType.MICROSECOND;
      default:
        throw new IllegalArgumentException("Unknown interval unit: " + unitString);
    }
  }

  private static Interval parseIso8601(Matcher matcher) {
    boolean allComponentsNull =
        matcher.group(2) == null
            && matcher.group(3) == null
            && matcher.group(4) == null
            && matcher.group(5) == null
            && matcher.group(7) == null
            && matcher.group(8) == null
            && matcher.group(9) == null;
    if (allComponentsNull) {
      if (matcher.group(1) == null && matcher.group(6) != null) {
        return Interval.fromMonthsDaysNanos(0, 0, BigInteger.ZERO);
      }
      throw new IllegalArgumentException("Invalid ISO 8601 interval");
    }

    int sign = "-".equals(matcher.group(1)) ? -1 : 1;
    IntervalAccumulator accumulator = new IntervalAccumulator(false);

    if (matcher.group(2) != null) {
      accumulator.addYears(
          new BigDecimal(matcher.group(2).replace(',', '.')).multiply(BigDecimal.valueOf(sign)));
    }
    if (matcher.group(3) != null) {
      accumulator.addMonths(
          new BigDecimal(matcher.group(3).replace(',', '.')).multiply(BigDecimal.valueOf(sign)));
    }
    if (matcher.group(4) != null) {
      accumulator.addWeeks(
          new BigDecimal(matcher.group(4).replace(',', '.')).multiply(BigDecimal.valueOf(sign)));
    }
    if (matcher.group(5) != null) {
      accumulator.addDays(
          new BigDecimal(matcher.group(5).replace(',', '.')).multiply(BigDecimal.valueOf(sign)));
    }
    if (matcher.group(7) != null) {
      accumulator.applyUnit(
          UnitType.HOUR,
          new BigDecimal(matcher.group(7).replace(',', '.')).multiply(BigDecimal.valueOf(sign)));
    }
    if (matcher.group(8) != null) {
      accumulator.applyUnit(
          UnitType.MINUTE,
          new BigDecimal(matcher.group(8).replace(',', '.')).multiply(BigDecimal.valueOf(sign)));
    }
    if (matcher.group(9) != null) {
      accumulator.applyUnit(
          UnitType.SECOND,
          new BigDecimal(matcher.group(9).replace(',', '.')).multiply(BigDecimal.valueOf(sign)));
    }

    return accumulator.build();
  }

  private static Interval parseIso8601Alternative(Matcher matcher) {
    int sign = "-".equals(matcher.group(1)) ? -1 : 1;
    IntervalAccumulator accumulator = new IntervalAccumulator(false);
    accumulator.addYears(new BigDecimal(matcher.group(2)).multiply(BigDecimal.valueOf(sign)));
    accumulator.addMonths(new BigDecimal(matcher.group(3)).multiply(BigDecimal.valueOf(sign)));
    accumulator.addDays(new BigDecimal(matcher.group(4)).multiply(BigDecimal.valueOf(sign)));
    accumulator.applyUnit(
        UnitType.HOUR, new BigDecimal(matcher.group(5)).multiply(BigDecimal.valueOf(sign)));
    accumulator.applyUnit(
        UnitType.MINUTE, new BigDecimal(matcher.group(6)).multiply(BigDecimal.valueOf(sign)));
    accumulator.applyUnit(
        UnitType.SECOND,
        new BigDecimal(matcher.group(7).replace(',', '.')).multiply(BigDecimal.valueOf(sign)));
    return accumulator.build();
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
    long microseconds = value.getNanos().divide(NANOS_PER_MICRO_BIG_INTEGER).longValueExact();
    int days = value.getDays();
    int months = value.getMonths();
    byte[] result = new byte[16];
    ByteConverter.int8(result, 0, microseconds);
    ByteConverter.int4(result, 8, days);
    ByteConverter.int4(result, 12, months);
    return result;
  }

  public static byte[] convertToPG(
      @Nonnull SessionState sessionState,
      DataOutputStream outputStream,
      ResultSet resultSet,
      int position,
      DataFormat format)
      throws IOException {
    writeToPG(sessionState, outputStream, resultSet, position, format);
    return null;
  }

  static void writeToPG(
      @Nonnull SessionState sessionState,
      DataOutputStream outputStream,
      ResultSet resultSet,
      int position,
      DataFormat format)
      throws IOException {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        StringParser.writeToPG(
            sessionState, outputStream, toPGString(resultSet.getInterval(position)));
        break;
      case POSTGRESQL_BINARY:
        Interval value = resultSet.getInterval(position);
        long microseconds = value.getNanos().divide(NANOS_PER_MICRO_BIG_INTEGER).longValueExact();
        int days = value.getDays();
        int months = value.getMonths();
        outputStream.writeInt(16);
        outputStream.writeLong(microseconds);
        outputStream.writeInt(days);
        outputStream.writeInt(months);
        break;
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
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
    BigInteger nanos = value.getNanos();
    nanos = nanos.abs();
    BigInteger[] hours = nanos.divideAndRemainder(NANOS_PER_HOUR);
    nanos = hours[1];
    BigInteger[] minutes = nanos.divideAndRemainder(NANOS_PER_MINUTE);
    nanos = minutes[1];
    BigInteger[] seconds = nanos.divideAndRemainder(NANOS_PER_SECOND);
    nanos = seconds[1];
    long micros = nanos.longValueExact() / NANOS_PER_MICRO;
    boolean isNegative =
        value.getNanos().signum() < 0
            && (hours[0].signum() != 0
                || minutes[0].signum() != 0
                || seconds[0].signum() != 0
                || micros != 0);
    return String.format(
        Locale.ROOT,
        "%d mons %d days %s%02d:%02d:%s.%06d",
        value.getMonths(),
        value.getDays(),
        isNegative ? "-" : "",
        hours[0],
        minutes[0],
        seconds[0],
        micros);
  }

  public static void bind(
      ImmutableMap.Builder<String, Value> parametersBuilder,
      String name,
      byte[] item,
      FormatCode formatCode) {
    parametersBuilder.put(name, Value.interval(toInterval(item, formatCode)));
  }

  @Override
  public void bind(ImmutableMap.Builder<String, Value> parametersBuilder, String name) {
    parametersBuilder.put(name, Value.interval(this.item));
  }
}
