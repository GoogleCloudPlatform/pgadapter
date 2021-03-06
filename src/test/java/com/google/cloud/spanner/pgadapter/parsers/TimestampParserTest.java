// Copyright 2022 Google LLC
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

import static com.google.cloud.spanner.pgadapter.parsers.Parser.PG_EPOCH_SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.util.ByteConverter;

@RunWith(JUnit4.class)
public class TimestampParserTest {

  @Test
  public void testToTimestamp() {
    long micros = new Random().nextLong();
    if (micros < -62135596800000L) {
      micros = -62135596800000L;
    } else if (micros > 253402300799000L) {
      micros = 253402300799000L;
    }
    byte[] data = new byte[8];
    ByteConverter.int8(data, 0, micros - PG_EPOCH_SECONDS * 1000_000L);
    assertEquals(Timestamp.ofTimeMicroseconds(micros), TimestampParser.toTimestamp(data));

    SpannerException spannerException =
        assertThrows(SpannerException.class, () -> TimestampParser.toTimestamp(new byte[4]));
    assertEquals(ErrorCode.INVALID_ARGUMENT, spannerException.getErrorCode());
  }

  @Test
  public void testStringParse() {
    assertEquals(
        "2022-07-08 07:22:59.123456789+00",
        new TimestampParser(Timestamp.parseTimestamp("2022-07-08T07:22:59.123456789Z"))
            .stringParse());
    assertNull(new TimestampParser(null).stringParse());
  }
}
