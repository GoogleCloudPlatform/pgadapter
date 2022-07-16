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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import java.math.BigDecimal;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.util.ByteConverter;

@RunWith(JUnit4.class)
public class NumericParserTest {

  @Test
  public void testToNumeric() {
    BigDecimal bd = BigDecimal.valueOf(new Random().nextDouble());
    byte[] data = ByteConverter.numeric(bd);
    assertEquals(bd.toString(), NumericParser.toNumericString(data));

    SpannerException spannerException =
        assertThrows(SpannerException.class, () -> NumericParser.toNumericString(new byte[4]));
    assertEquals(ErrorCode.INVALID_ARGUMENT, spannerException.getErrorCode());
  }

  @Test
  public void testStringParse() {
    assertEquals("3.14", new NumericParser("3.14").stringParse());
    assertEquals("NaN", new NumericParser("NaN").stringParse());
    assertNull(new NumericParser(null).stringParse());
  }
}
