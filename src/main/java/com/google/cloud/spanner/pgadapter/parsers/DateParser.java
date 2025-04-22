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
import com.google.cloud.Date;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ProtobufResultSet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.collect.ImmutableMap;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;

/** Translate wire protocol dates to desired formats. */
@InternalApi
public class DateParser extends Parser<Date> {

  DateParser(ResultSet item, int position) {
    this.item = item.getDate(position);
  }

  DateParser(Object item) {
    this.item = (Date) item;
  }

  DateParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          String stringValue = new String(item, UTF8);
          // Use the first 10 characters of the date string, as the string might contain a timezone
          // identifier, which is not supported by parseDate(String).
          if (stringValue.length() >= 10) {
            this.item = Date.parseDate(stringValue.substring(0, 10));
          } else {
            throw PGExceptionFactory.newPGException("Invalid date value: " + stringValue);
          }
          break;
        case BINARY:
          this.item = toDate(item);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  /** Converts the binary data to a {@link Date}. */
  public static Date toDate(@Nonnull byte[] data) {
    if (data.length < 4) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid length for date: " + data.length);
    }
    long days = ByteConverter.int4(data, 0) + PG_EPOCH_DAYS;
    LocalDate localDate = LocalDate.ofEpochDay(validateRange(days));
    return Date.fromYearMonthDay(
        localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
  }

  @Override
  public String stringParse() {
    return this.item == null ? null : toString(this.item);
  }

  static String toString(Date date) {
    int yearValue = date.getYear();
    int monthValue = date.getMonth();
    int dayValue = date.getDayOfMonth();
    StringBuilder buf = new StringBuilder(10);
    if (yearValue < 1000) {
      // Note that Date cannot contain negative year numbers, so this value is always guaranteed to
      // be between 1 and 999 (inclusive).
      buf.append(yearValue + 10000).deleteCharAt(0);
    } else {
      // Year values beyond 9999 are not supported by Cloud Spanner, but it is allowed by the Date
      // class. The ISO specification requires year numbers that have more than 4 digits to have a
      // plus or minus sign.
      if (yearValue > 9999) {
        buf.append('+');
      }
      buf.append(yearValue);
    }
    return buf.append(monthValue < 10 ? "-0" : "-")
        .append(monthValue)
        .append(dayValue < 10 ? "-0" : "-")
        .append(dayValue)
        .toString();
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    return convertToPG(this.item);
  }

  static byte[] convertToPG(Date value) {
    LocalDate localDate = LocalDate.of(value.getYear(), value.getMonth(), value.getDayOfMonth());
    long days = localDate.toEpochDay() - PG_EPOCH_DAYS;
    int daysAsInt = validateRange(days);
    return IntegerParser.binaryParse(daysAsInt);
  }

  public static byte[] convertToPG(
      SessionState sessionState,
      DataOutputStream outputStream,
      ResultSet resultSet,
      int position,
      DataFormat format)
      throws IOException {
    writeToPG(sessionState, outputStream, resultSet, position, format);
    return null;
  }

  static void writeToPG(
      SessionState sessionState,
      DataOutputStream outputStream,
      ResultSet resultSet,
      int position,
      DataFormat format)
      throws IOException {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        StringParser.writeToPG(sessionState, outputStream, getDateAsString(resultSet, position));
        break;
      case POSTGRESQL_BINARY:
        byte[] value = convertToPG(resultSet.getDate(position));
        outputStream.writeInt(value.length);
        outputStream.write(value);
        break;
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  static String getDateAsString(ResultSet resultSet, int column) {
    if (resultSet instanceof ProtobufResultSet
        && ((ProtobufResultSet) resultSet).canGetProtobufValue(column)) {
      return ((ProtobufResultSet) resultSet).getProtobufValue(column).getStringValue();
    }
    return toString(resultSet.getDate(column));
  }

  /**
   * Dates are stored as long, but technically cannot be longer than int. Here we ensure that is the
   * case.
   *
   * @param days Number of days to validate.
   */
  static int validateRange(long days) {
    if (days > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Date is out of range, epoch day=" + days);
    }
    return (int) days;
  }

  @Override
  public void bind(ImmutableMap.Builder<String, Value> parametersBuilder, String name) {
    parametersBuilder.put(name, Value.date(this.item));
  }
}
