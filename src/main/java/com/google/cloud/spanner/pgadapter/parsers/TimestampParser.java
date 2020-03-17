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
import java.sql.Timestamp;
import java.sql.Types;
import java.util.regex.Pattern;
import org.postgresql.util.ByteConverter;

/**
 * Translate from wire protocol to timestamp.
 */
public class TimestampParser extends Parser<Timestamp> {

  public static final int MILLISECONDS_IN_SECOND = 1000;
  public static final int MICROSECONDS_IN_SECOND = 1000000;
  public static final long NANOSECONDS_IN_MICROSECONDS = 1000L;
  private static final char TIMESTAMP_SEPARATOR = 'T';
  private static final char ZERO_TIMEZONE = 'Z';
  private static final char EMPTY_SPACE = ' ';

  /**
   * Regular expression for parsing timestamps.
   */
  private static final String TIMESTAMP_REGEX = "(\\d{4})-(\\d{2})-(\\d{2})" // yyyy-MM-dd
      + "( (\\d{2}):(\\d{2}):(\\d{2})(\\.\\d{1,9})?)" // ' 'HH:mm:ss.milliseconds
      + "([Zz]|([+-])(\\d{2})(:(\\d{2}))?)?"; // 'Z' or time zone shift HH:mm following '+' or '-'
  private static final Pattern TIMESTAMP_PATTERN = Pattern.compile(TIMESTAMP_REGEX);

  public TimestampParser(ResultSet item, int position) throws SQLException {
    this.item = item.getTimestamp(position);
  }

  public TimestampParser(Object item) {
    this.item = (Timestamp) item;
  }

  public TimestampParser(byte[] item) {
    long micros = ByteConverter.int8(item, 0);
    this.item = new Timestamp(micros);
  }

  /**
   * Checks whether the given text contains a timestamp that can be parsed by PostgreSQL.
   *
   * @param value The value to check. May not be <code>null</code>.
   * @return <code>true</code> if the text contains a valid timestamp.
   */
  public static boolean isTimestamp(String value) {
    Preconditions.checkNotNull(value);
    return TIMESTAMP_PATTERN.matcher(value).matches();
  }

  @Override
  public Timestamp getItem() {
    return item;
  }

  @Override
  protected String stringParse() {
    return toPGString();
  }

  @Override
  protected String spannerParse() {
    return item.toString();
  }

  @Override
  protected byte[] binaryParse() {
    long microseconds = (
        (this.item.getTime() / MILLISECONDS_IN_SECOND - PG_EPOCH_SECONDS) * MICROSECONDS_IN_SECOND)
        + (this.item.getNanos() / NANOSECONDS_IN_MICROSECONDS);
    return toBinary(microseconds, Types.BIGINT);
  }

  /**
   * Converts the given {@link Timestamp} to a text value that is understood by PostgreSQL. That
   * means a space delimiter between date and time values, and no trailing 'Z'.
   *
   * @return a {@link String} that can be interpreted by PostgreSQL.
   */
  private String toPGString() {
    return this.item.toString()
        .replace(TIMESTAMP_SEPARATOR, EMPTY_SPACE)
        .replace(ZERO_TIMEZONE, EMPTY_SPACE);
  }
}
