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
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;

/** Translate wire protocol dates to desired formats. */
@InternalApi
public class DateParser extends Parser<Date> {
  // Valid format for date: 'yyyy-MM-dd [+-]HH[:mi]'.
  // Timezone information is optional. Timezone may also be specified using only hour value.
  // NOTE: Following algorithm might perform slowly due to exception handling; sadly, this seems
  //       to be the accepted default method for date validation.
  private static final SimpleDateFormat[] VALID_DATE_FORMATS = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm"),
    new SimpleDateFormat("yyyy-MM-dd +HH:mm"),
    new SimpleDateFormat("yyyy-MM-dd -HH:mm"),
    new SimpleDateFormat("yyyy-MM-dd +HH"),
    new SimpleDateFormat("yyyy-MM-dd -HH")
  };

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

  /**
   * Checks whether the given text contains a date that can be parsed by PostgreSQL.
   *
   * @param value The value to check. May not be <code>null</code>.
   * @return <code>true</code> if the text contains a valid date.
   */
  static boolean isDate(String value) {
    Preconditions.checkNotNull(value);
    for (SimpleDateFormat dateFormat : VALID_DATE_FORMATS) {
      try {
        dateFormat.parse(value);
        return true;
      } catch (ParseException e) {
        // ignore and try the next
      }
    }
    return false;
  }

  @Override
  public String stringParse() {
    return this.item == null ? null : item.toString();
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

  public static byte[] convertToPG(ResultSet resultSet, int position, DataFormat format) {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        return resultSet.getDate(position).toString().getBytes(StandardCharsets.UTF_8);
      case POSTGRESQL_BINARY:
        return convertToPG(resultSet.getDate(position));
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
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
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
