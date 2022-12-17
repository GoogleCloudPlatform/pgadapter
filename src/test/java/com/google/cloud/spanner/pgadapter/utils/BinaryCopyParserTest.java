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

package com.google.cloud.spanner.pgadapter.utils;

import static com.google.cloud.spanner.pgadapter.statements.CopyToStatement.COPY_BINARY_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.utils.BinaryCopyParser.BinaryField;
import com.google.cloud.spanner.pgadapter.utils.BinaryCopyParser.BinaryRecord;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BinaryCopyParserTest {

  @Test
  public void testValidHeader() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);

    parser.verifyBinaryHeader();
  }

  @Test
  public void testInvalidHeader() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(new byte[11]);

    IOException ioException = assertThrows(IOException.class, parser::verifyBinaryHeader);
    assertTrue(ioException.getMessage().contains("Invalid COPY header encountered."));
  }

  @Test
  public void testGetIterator() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(0); // flags
    data.writeInt(0); // header extensions length

    assertNotNull(parser.iterator());
  }

  @Test
  public void testGetIterator_MissingData() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    pipedOutputStream.close();

    assertThrows(SpannerException.class, parser::iterator);
  }

  @Test
  public void testIteratorHasNext_Trailer() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(0);
    data.writeInt(0);

    // Write trailer.
    data.writeShort(-1);

    Iterator<CopyRecord> iterator = parser.iterator();
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorHasNext() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(0);
    data.writeInt(0);

    // Write a tuple.
    data.writeShort(2);
    data.writeInt(10);
    data.write(new byte[10]);
    data.writeInt(4);
    data.writeInt(100);
    // Then write the trailer.
    data.writeShort(-1);

    Iterator<CopyRecord> iterator = parser.iterator();
    assertTrue(iterator.hasNext());
    iterator.next();
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorHasNext_InvalidFieldCount() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(0);
    data.writeInt(0);

    // Write an invalid field count.
    data.writeShort(-2);

    Iterator<CopyRecord> iterator = parser.iterator();
    SpannerException spannerException = assertThrows(SpannerException.class, iterator::hasNext);
    assertEquals(ErrorCode.FAILED_PRECONDITION, spannerException.getErrorCode());
    assertEquals(
        "FAILED_PRECONDITION: Invalid field count encountered: -2", spannerException.getMessage());
  }

  @Test
  public void testIteratorHasNext_DifferentFieldCounts() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(0);
    data.writeInt(0);

    // Write two tuples with two different field counts.
    data.writeShort(1);
    data.writeInt(5);
    data.write(new byte[5]);
    data.writeShort(2);

    Iterator<CopyRecord> iterator = parser.iterator();
    assertTrue(iterator.hasNext());
    assertNotNull(iterator.next());
    SpannerException spannerException = assertThrows(SpannerException.class, iterator::hasNext);
    assertEquals(ErrorCode.FAILED_PRECONDITION, spannerException.getErrorCode());
    assertEquals(
        "FAILED_PRECONDITION: Invalid field count encountered: 2, expected 1",
        spannerException.getMessage());
  }

  @Test
  public void testIteratorHasNext_EndOfFile() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(0);
    data.writeInt(0);

    // Closing the underlying output stream causes an EOFException. This is handled internally and
    // considered an indication that there are no more rows.
    pipedOutputStream.close();

    Iterator<CopyRecord> iterator = parser.iterator();
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorNext_WithNoMoreElements() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(0);
    data.writeInt(0);

    // Write the trailer indicator to indicate that there are no more rows.
    data.writeShort(-1);

    Iterator<CopyRecord> iterator = parser.iterator();
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testIteratorNext_NullField() throws IOException {
    SessionState sessionState = mock(SessionState.class);
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(0);
    data.writeInt(0);

    // Write a tuple with a single null field.
    data.writeShort(1);
    data.writeInt(-1);

    // Trailer.
    data.writeShort(-1);

    Iterator<CopyRecord> iterator = parser.iterator();
    assertTrue(iterator.hasNext());
    CopyRecord record = iterator.next();
    assertTrue(record.getValue(sessionState, Type.int64(), 0).isNull());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorNext_GetValue() throws IOException {
    SessionState sessionState = mock(SessionState.class);
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(0);
    data.writeInt(0);

    // Write a tuple with a single field.
    data.writeShort(1);
    data.writeInt(8);
    data.writeLong(100L);

    // Trailer.
    data.writeShort(-1);

    Iterator<CopyRecord> iterator = parser.iterator();
    assertTrue(iterator.hasNext());
    CopyRecord record = iterator.next();
    assertEquals(Value.int64(100L), record.getValue(sessionState, Type.int64(), 0));
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorNext_NegativeFieldLength() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(0);
    data.writeInt(0);

    // Write a tuple with a single field with an invalid length.
    data.writeShort(1);
    data.writeInt(-2);

    Iterator<CopyRecord> iterator = parser.iterator();
    assertTrue(iterator.hasNext());
    assertThrows(SpannerException.class, iterator::next);
  }

  @Test
  public void testIteratorNext_EndOfFile() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(0);
    data.writeInt(0);

    // Write a tuple with a single field with an invalid length.
    data.writeShort(1);
    data.writeInt(10);
    pipedOutputStream.close();

    Iterator<CopyRecord> iterator = parser.iterator();
    assertTrue(iterator.hasNext());
    assertThrows(SpannerException.class, iterator::next);
  }

  @Test
  public void testIteratorNext_WithOid() throws IOException {
    SessionState sessionState = mock(SessionState.class);
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(1 << 16);
    data.writeInt(0);

    // Write a tuple with two normal fields and an oid field.
    data.writeShort(2);
    data.writeInt(4);
    data.writeInt(100); // oid
    data.writeInt(8);
    data.writeLong(100L);
    data.writeInt(4);
    data.write("test".getBytes(StandardCharsets.UTF_8));

    // Trailer.
    data.writeShort(-1);

    Iterator<CopyRecord> iterator = parser.iterator();
    assertTrue(iterator.hasNext());
    CopyRecord record = iterator.next();
    assertEquals(Value.int64(100L), record.getValue(sessionState, Type.int64(), 0));
    assertEquals(Value.string("test"), record.getValue(sessionState, Type.string(), 1));
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorNext_InvalidOidLength() throws IOException {
    PipedOutputStream pipedOutputStream = new PipedOutputStream();
    BinaryCopyParser parser = new BinaryCopyParser(new PipedInputStream(pipedOutputStream, 256));

    DataOutputStream data = new DataOutputStream(pipedOutputStream);
    data.write(COPY_BINARY_HEADER);
    data.writeInt(1 << 16);
    data.writeInt(0);

    // Write a tuple with two normal fields and an oid field.
    data.writeShort(2);
    data.writeInt(8); // Invalid length for an oid

    Iterator<CopyRecord> iterator = parser.iterator();
    assertTrue(iterator.hasNext());
    SpannerException spannerException = assertThrows(SpannerException.class, iterator::next);
    assertEquals(ErrorCode.FAILED_PRECONDITION, spannerException.getErrorCode());
    assertEquals("FAILED_PRECONDITION: Invalid length for OID: 8", spannerException.getMessage());
  }

  @Test
  public void testBinaryRecord() {
    SessionState sessionState = mock(SessionState.class);
    BinaryRecord record = new BinaryRecord(new BinaryField[1]);
    assertFalse(record.hasColumnNames());
    assertThrows(
        UnsupportedOperationException.class,
        () -> record.getValue(sessionState, Type.string(), "column_name"));
    assertThrows(
        IllegalArgumentException.class, () -> record.getValue(sessionState, Type.string(), 10));
    assertThrows(SpannerException.class, () -> record.getValue(sessionState, Type.numeric(), 0));
  }
}
