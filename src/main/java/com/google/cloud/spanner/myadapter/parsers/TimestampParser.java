// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.myadapter.parsers;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class TimestampParser extends Parser<Timestamp> {
  private static final DateTimeFormatter TIMESTAMP_OUTPUT_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseLenient()
          .parseCaseInsensitive()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
          .toFormatter();

  TimestampParser(ResultSet item, int position) {
    super((resultSet, index) -> item.getTimestamp(position), item, position);
  }

  @Override
  public byte[] toLengthEncodedBytes() throws IOException {
    OffsetDateTime offsetDateTime =
        OffsetDateTime.ofInstant(
            Instant.ofEpochSecond(item.getSeconds(), item.getNanos()), ZoneId.from(ZoneOffset.UTC));
    String dateTime = TIMESTAMP_OUTPUT_FORMATTER.format(offsetDateTime);
    return StringParser.getLengthEncodedBytes(dateTime);
  }
}
