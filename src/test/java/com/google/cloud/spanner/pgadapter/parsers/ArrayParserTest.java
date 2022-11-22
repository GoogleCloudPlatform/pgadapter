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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.collect.ImmutableList;
import java.time.ZoneId;
import java.util.Arrays;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ArrayParserTest {

  @Test
  public void testInt64StringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(Type.int64(), Value.int64Array(Arrays.asList(1L, null, 2L))),
            0,
            mock(SessionState.class));

    assertEquals("{1,NULL,2}", parser.stringParse());
  }

  @Test
  public void testBoolStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(Type.bool(), Value.boolArray(Arrays.asList(true, null, false))),
            0,
            mock(SessionState.class));

    assertEquals("{t,NULL,f}", parser.stringParse());
  }

  @Test
  public void testBytesStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.bytes(),
                Value.bytesArray(
                    Arrays.asList(ByteArray.copyFrom("test1"), null, ByteArray.copyFrom("test2")))),
            0,
            mock(SessionState.class));

    assertEquals("{\"\\\\x7465737431\",NULL,\"\\\\x7465737432\"}", parser.stringParse());
  }

  @Test
  public void testFloat64StringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.float64(), Value.float64Array(Arrays.asList(3.14, null, 6.626))),
            0,
            mock(SessionState.class));

    assertEquals("{3.14,NULL,6.626}", parser.stringParse());
  }

  @Test
  public void testNumericStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.pgNumeric(), Value.pgNumericArray(Arrays.asList("3.14", null, "6.626"))),
            0,
            mock(SessionState.class));

    assertEquals("{3.14,NULL,6.626}", parser.stringParse());
  }

  @Test
  public void testDateStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.date(),
                Value.dateArray(
                    Arrays.asList(
                        Date.parseDate("2022-07-08"), null, Date.parseDate("2000-01-01")))),
            0,
            mock(SessionState.class));

    assertEquals("{\"2022-07-08\",NULL,\"2000-01-01\"}", parser.stringParse());
  }

  @Test
  public void testTimestampStringParse() {
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.getTimezone()).thenReturn(ZoneId.of("UTC"));

    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.timestamp(),
                Value.timestampArray(
                    Arrays.asList(
                        Timestamp.parseTimestamp("2022-07-08T07:00:02.123456789Z"),
                        null,
                        Timestamp.parseTimestamp("2000-01-01T00:00:00Z")))),
            0,
            sessionState);

    assertEquals(
        "{\"2022-07-08 07:00:02.123456789+00\",NULL,\"2000-01-01 00:00:00+00\"}",
        parser.stringParse());
  }

  @Test
  public void testStringStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.string(), Value.stringArray(Arrays.asList("test1", null, "test2"))),
            0,
            mock(SessionState.class));

    assertEquals("{\"test1\",NULL,\"test2\"}", parser.stringParse());
  }

  @Ignore("json not yet supported")
  @Test
  public void testJsonStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.json(),
                Value.jsonArray(
                    Arrays.asList("{\"key\": \"value1\"}}", null, "{\"key\": \"value2\"}"))),
            0,
            mock(SessionState.class));

    assertEquals("{\"test1\",NULL,\"test2\"}", parser.stringParse());
  }

  static ResultSet createArrayResultSet(Type arrayElementType, Value value) {
    ResultSet resultSet =
        ResultSets.forRows(
            Type.struct(StructField.of("col_array", Type.array(arrayElementType))),
            ImmutableList.of(Struct.newBuilder().set("col_array").to(value).build()));
    resultSet.next();
    return resultSet;
  }
}
