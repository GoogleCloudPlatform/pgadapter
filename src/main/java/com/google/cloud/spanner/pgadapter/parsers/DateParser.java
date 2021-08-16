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

import com.google.common.base.Preconditions;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import org.postgresql.util.ByteConverter;

/**
 * Translate wire protocol dates to desired formats.
 */
public class DateParser extends Parser<java.sql.Date> {

  public DateParser(ResultSet item, int position) throws SQLException {
    this.item = item.getDate(position);
  }

  public DateParser(Object item) {
    this.item = (java.sql.Date) item;
  }

  public DateParser(byte[] item) {
    long days = ByteConverter.int4(item, 0) + PG_EPOCH_DAYS;
    this.validateRange(days);
    this.item = java.sql.Date.valueOf(LocalDate.ofEpochDay(days));
  }

  /**
   * Checks whether the given text contains a date that can be parsed by PostgreSQL.
   *
   * @param value The value to check. May not be <code>null</code>.
   * @return <code>true</code> if the text contains a valid date.
   */
  public static boolean isDate(String value) {
    Preconditions.checkNotNull(value);
    // Valid format for date: 'yyyy-mm-dd [+-]HH:mi'.
    // Timezone information is optional. Timezone may also be specified using only hour value.
    // NOTE: Following algorithm might perform slowly due to exception handling; sadly, this seems
    //       to be the accepted default method for date validation.
    SimpleDateFormat[] dateFormats = {
        new SimpleDateFormat("yyyy-mm-dd HH:mm"),
        new SimpleDateFormat("yyyy-mm-dd +HH:mm"),
        new SimpleDateFormat("yyyy-mm-dd -HH:mm")
    };
    for (SimpleDateFormat dateFormat : dateFormats) {
      try {
        dateFormat.parse(value);
        return true;
      } catch (ParseException e) {
      }
    }
    return false;
  }

  @Override
  protected String stringParse() {
    return item.toString();
  }

  @Override
  protected byte[] binaryParse() {
    Long days = this.item.toLocalDate().toEpochDay() - PG_EPOCH_DAYS;
    this.validateRange(days);
    return toBinary(days.intValue(), Types.INTEGER);
  }

  /**
   * Dates are stored as long, but technically cannot be longer than int. Here we ensure that is the
   * case.
   *
   * @param days Number of days to validate.
   */
  private void validateRange(long days) {
    if (days > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Date is out of range, epoch day=" + days);
    }
  }
}
