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

import static com.google.cloud.spanner.pgadapter.utils.MutationWriter.calculateSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter.CopyTransactionMode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MutationWriterTest {

  @Test
  public void testParsePartialPayload() throws IOException {
    CSVFormat format = CSVFormat.POSTGRESQL_TEXT;

    PipedOutputStream payload = new PipedOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(payload, StandardCharsets.UTF_8);

    PipedInputStream inputStream = new PipedInputStream(payload);
    Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);

    CSVParser parser = CSVParser.parse(reader, format);
    // Pass in 2 complete and one incomplete record. It should be possible to parse the two first
    // records without problems.
    String records = "1\t\"One\"\n2\t\"Two\"\n3\t";
    writer.write(records);
    writer.flush();

    // Get an iterator for the parser and get the first two records.
    Iterator<CSVRecord> iterator = parser.iterator();
    assertTrue(iterator.hasNext());
    CSVRecord record = iterator.next();
    assertEquals("1", record.get(0));
    assertEquals("One", record.get(1));

    assertTrue(iterator.hasNext());
    record = iterator.next();
    assertEquals("2", record.get(0));
    assertEquals("Two", record.get(1));

    // Calling iterator.hasNext() or iterator.next() would now block, as there is not enough data
    // to build another record.
    // Add the missing pieces for the last record and parse that as well.
    writer.write("\"Three\"\n");
    writer.close();

    assertTrue(iterator.hasNext());
    record = iterator.next();
    assertEquals("3", record.get(0));
    assertEquals("Three", record.get(1));

    // There are no more records as the writer has been closed.
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testWriteMutations() throws Exception {
    Map<String, TypeCode> tableColumns =
        ImmutableMap.of("number", TypeCode.INT64, "name", TypeCode.STRING);
    CSVFormat format =
        CSVFormat.POSTGRESQL_TEXT
            .builder()
            .setHeader(tableColumns.keySet().toArray(new String[0]))
            .build();
    Connection connection = mock(Connection.class);
    DatabaseClient databaseClient = mock(DatabaseClient.class);
    when(connection.getDatabaseClient()).thenReturn(databaseClient);
    MutationWriter mutationWriter =
        new MutationWriter(
            CopyTransactionMode.ImplicitAtomic,
            connection,
            "numbers",
            tableColumns,
            /* indexedColumnsCount = */ 1,
            format,
            false);

    mutationWriter.addCopyData("1\t\"One\"\n2\t\"Two\"\n".getBytes(StandardCharsets.UTF_8));
    mutationWriter.commit();
    mutationWriter.close();

    StatementResult updateCount = mutationWriter.call();

    assertEquals(2L, updateCount.getUpdateCount().longValue());
    List<Mutation> expectedMutations =
        ImmutableList.of(
            Mutation.newInsertBuilder("numbers").set("number").to(1L).set("name").to("One").build(),
            Mutation.newInsertBuilder("numbers")
                .set("number")
                .to(2L)
                .set("name")
                .to("Two")
                .build());
    verify(databaseClient).write(expectedMutations);
  }

  @Test
  public void testWriteMutations_FailsForLargeBatch() throws Exception {
    System.setProperty("copy_in_mutation_limit", "1");
    try {
      Map<String, TypeCode> tableColumns =
          ImmutableMap.of("number", TypeCode.INT64, "name", TypeCode.STRING);
      CSVFormat format =
          CSVFormat.POSTGRESQL_TEXT
              .builder()
              .setHeader(tableColumns.keySet().toArray(new String[0]))
              .build();
      Connection connection = mock(Connection.class);
      MutationWriter mutationWriter =
          new MutationWriter(
              CopyTransactionMode.ImplicitAtomic,
              connection,
              "numbers",
              tableColumns,
              /* indexedColumnsCount = */ 1,
              format,
              false);

      mutationWriter.addCopyData("1\t\"One\"\n2\t\"Two\"\n".getBytes(StandardCharsets.UTF_8));
      mutationWriter.close();

      SpannerException exception = assertThrows(SpannerException.class, mutationWriter::call);
      assertEquals(
          "FAILED_PRECONDITION: Record count: 2 has exceeded the limit: 1.\n"
              + "\n"
              + "The number of mutations per record is equal to the number of columns in the record plus the number of indexed columns in the record. The maximum number of mutations in one transaction is 20000.\n"
              + "\n"
              + "Execute `SET AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'` before executing a large COPY operation to instruct PGAdapter to automatically break large transactions into multiple smaller. This will make the COPY operation non-atomic.\n\n",
          exception.getMessage());

      verify(connection, never()).write(anyIterable());
    } finally {
      System.getProperties().remove("copy_in_mutation_limit");
    }
  }

  @Test
  public void testWriteMutations_NonAtomic_SucceedsForLargeBatch() throws Exception {
    // 6 == 2 mutations per batch, as we have 2 columns + 1 indexed column.
    System.setProperty("copy_in_mutation_limit", "6");
    try {
      Map<String, TypeCode> tableColumns =
          ImmutableMap.of("number", TypeCode.INT64, "name", TypeCode.STRING);
      CSVFormat format =
          CSVFormat.POSTGRESQL_TEXT
              .builder()
              .setHeader(tableColumns.keySet().toArray(new String[0]))
              .build();
      Connection connection = mock(Connection.class);
      DatabaseClient databaseClient = mock(DatabaseClient.class);
      when(connection.getDatabaseClient()).thenReturn(databaseClient);

      MutationWriter mutationWriter =
          new MutationWriter(
              CopyTransactionMode.ImplicitNonAtomic,
              connection,
              "numbers",
              tableColumns,
              /* indexedColumnsCount = */ 1,
              format,
              false);

      mutationWriter.addCopyData(
          "1\t\"One\"\n2\t\"Two\"\n3\t\"Three\"\n4\t\"Four\"\n5\t\"Five\"\n"
              .getBytes(StandardCharsets.UTF_8));
      mutationWriter.commit();
      mutationWriter.close();

      StatementResult updateCount = mutationWriter.call();

      assertEquals(5L, updateCount.getUpdateCount().longValue());
      verify(databaseClient, times(3)).write(anyIterable());
    } finally {
      System.getProperties().remove("copy_in_mutation_limit");
    }
  }

  @Test
  public void testWriteMutations_FailsForLargeCommit() throws Exception {
    System.setProperty("copy_in_commit_limit", "30");
    try {
      Map<String, TypeCode> tableColumns =
          ImmutableMap.of("number", TypeCode.INT64, "name", TypeCode.STRING);
      CSVFormat format =
          CSVFormat.POSTGRESQL_TEXT
              .builder()
              .setHeader(tableColumns.keySet().toArray(new String[0]))
              .build();
      Connection connection = mock(Connection.class);
      MutationWriter mutationWriter =
          new MutationWriter(
              CopyTransactionMode.ImplicitAtomic,
              connection,
              "numbers",
              tableColumns,
              /* indexedColumnsCount = */ 0,
              format,
              false);

      // The calculated size of these mutations are:
      // 1. 8 bytes for the INT64
      // 2. 3 characters * 4 bytes per character = 12 bytes for STRING
      // 3. Total: 20 bytes per record, 40 bytes for the entire batch.
      mutationWriter.addCopyData("1\t\"One\"\n2\t\"Two\"\n".getBytes(StandardCharsets.UTF_8));
      mutationWriter.close();

      SpannerException exception = assertThrows(SpannerException.class, mutationWriter::call);
      assertTrue(exception.getMessage().contains("Commit size: 40 has exceeded the limit: 30"));

      verify(connection, never()).write(anyIterable());
    } finally {
      System.getProperties().remove("copy_in_commit_limit");
    }
  }

  @Test
  public void testWriteMutations_NonAtomic_SucceedsForLargeCommit() throws Exception {
    System.setProperty("copy_in_commit_limit", "80");
    try {
      Map<String, TypeCode> tableColumns =
          ImmutableMap.of("number", TypeCode.INT64, "name", TypeCode.STRING);
      CSVFormat format =
          CSVFormat.POSTGRESQL_TEXT
              .builder()
              .setHeader(tableColumns.keySet().toArray(new String[0]))
              .build();
      Connection connection = mock(Connection.class);
      DatabaseClient databaseClient = mock(DatabaseClient.class);
      when(connection.getDatabaseClient()).thenReturn(databaseClient);

      MutationWriter mutationWriter =
          new MutationWriter(
              CopyTransactionMode.ImplicitNonAtomic,
              connection,
              "numbers",
              tableColumns,
              /* indexedColumnsCount = */ 1,
              format,
              false);

      mutationWriter.addCopyData(
          "1\t\"One\"\n2\t\"Two\"\n3\t\"Three\"\n4\t\"Four\"\n5\t\"Five\"\n"
              .getBytes(StandardCharsets.UTF_8));
      mutationWriter.commit();
      mutationWriter.close();

      StatementResult updateCount = mutationWriter.call();

      assertEquals(5L, updateCount.getUpdateCount().longValue());
      // We expect two batches, because:
      // 1. The commit limit is 80 bytes. That is for safety halved down to 40 bytes.
      // 2. Each record is 20 bytes.
      // 3. The first batch contains 20 + 20 bytes.
      // 4. The second batch contains 28 bytes. (3 - 'Three')
      // 5. The third batch contains 24 bytes. (4 - 'Four')
      // 6. the fourth batch contains 24 bytes. (5 - 'Five')
      verify(databaseClient, times(4)).write(anyIterable());
    } finally {
      System.getProperties().remove("copy_in_commit_limit");
    }
  }

  @Test
  public void testWritePartials() throws Exception {
    Map<String, TypeCode> tableColumns =
        ImmutableMap.of("number", TypeCode.INT64, "name", TypeCode.STRING);
    CSVFormat format =
        CSVFormat.POSTGRESQL_TEXT
            .builder()
            .setHeader(tableColumns.keySet().toArray(new String[0]))
            .build();
    Connection connection = mock(Connection.class);
    DatabaseClient databaseClient = mock(DatabaseClient.class);
    when(connection.getDatabaseClient()).thenReturn(databaseClient);

    MutationWriter mutationWriter =
        new MutationWriter(
            CopyTransactionMode.ImplicitNonAtomic,
            connection,
            "numbers",
            tableColumns,
            /* indexedColumnsCount = */ 1,
            format,
            false);

    mutationWriter.addCopyData(
        "1\t\"One\"\n2\t\"Two\"\n3\t\"Three\"\n4\t\"Four\"\n5\t\"Five\"\n"
            .getBytes(StandardCharsets.UTF_8));
    mutationWriter.commit();
    mutationWriter.close();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.submit(
        () -> {
          mutationWriter.addCopyData("1\t\"One\"\n".getBytes(StandardCharsets.UTF_8));
          mutationWriter.addCopyData("2\t\"Two".getBytes(StandardCharsets.UTF_8));
          mutationWriter.addCopyData("\"".getBytes(StandardCharsets.UTF_8));
          mutationWriter.addCopyData("\n3\t".getBytes(StandardCharsets.UTF_8));
          mutationWriter.addCopyData(
              "\"Three\"\n4\t\"Four\"\n5\t".getBytes(StandardCharsets.UTF_8));
          mutationWriter.addCopyData("\"Five\"\n".getBytes(StandardCharsets.UTF_8));
          mutationWriter.close();
          return null;
        });
    Future<StatementResult> updateCount = executor.submit(mutationWriter);

    assertEquals(5L, updateCount.get().getUpdateCount().longValue());
    List<Mutation> expectedMutations =
        ImmutableList.of(
            Mutation.newInsertBuilder("numbers").set("number").to(1L).set("name").to("One").build(),
            Mutation.newInsertBuilder("numbers").set("number").to(2L).set("name").to("Two").build(),
            Mutation.newInsertBuilder("numbers")
                .set("number")
                .to(3L)
                .set("name")
                .to("Three")
                .build(),
            Mutation.newInsertBuilder("numbers")
                .set("number")
                .to(4L)
                .set("name")
                .to("Four")
                .build(),
            Mutation.newInsertBuilder("numbers")
                .set("number")
                .to(5L)
                .set("name")
                .to("Five")
                .build());
    verify(databaseClient).write(expectedMutations);

    executor.shutdown();
  }

  @Test
  public void testCalculateSize() {
    assertEquals(
        1,
        calculateSize(
            Mutation.newInsertBuilder("my_table").set("my_col").to((String) null).build()));
    assertEquals(
        8, calculateSize(Mutation.newInsertBuilder("my_table").set("my_col").to(1L).build()));
    assertEquals(
        1, calculateSize(Mutation.newInsertBuilder("my_table").set("my_col").to(true).build()));
    assertEquals(
        3,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .to(ByteArray.copyFrom(new byte[] {1, 2, 3}))
                .build()));
    assertEquals(
        8, calculateSize(Mutation.newInsertBuilder("my_table").set("my_col").to(3.14d).build()));
    assertEquals(
        5,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .to(Value.pgNumeric("6.626"))
                .build()));
    assertEquals(
        30,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .to(Timestamp.parseTimestamp("2022-06-16T18:15:30.123456789Z"))
                .build()));
    assertEquals(
        10,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .to(Date.parseDate("2022-06-16"))
                .build()));
    assertEquals(
        12, calculateSize(Mutation.newInsertBuilder("my_table").set("my_col").to("foo").build()));
    assertEquals(
        12,
        calculateSize(
            Mutation.newInsertBuilder("my_table").set("my_col").to(Value.json("foo")).build()));

    assertEquals(
        16,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .toInt64Array(ImmutableList.of(1L, 2L))
                .build()));
    assertEquals(
        2,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .toBoolArray(ImmutableList.of(true, false))
                .build()));
    assertEquals(
        5,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .toBytesArray(
                    ImmutableList.of(
                        ByteArray.copyFrom(new byte[] {1, 2}),
                        ByteArray.copyFrom(new byte[] {3, 4, 5})))
                .build()));
    assertEquals(
        16,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .toFloat64Array(ImmutableList.of(3.14d, 6.626d))
                .build()));
    assertEquals(
        9,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .toPgNumericArray(ImmutableList.of("6.626", "3.14"))
                .build()));
    assertEquals(
        60,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .toTimestampArray(
                    ImmutableList.of(
                        Timestamp.parseTimestamp("2022-06-16T18:15:30.123456789Z"),
                        Timestamp.parseTimestamp("2000-06-16T18:15:30.123456789Z")))
                .build()));
    assertEquals(
        20,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .toDateArray(
                    ImmutableList.of(Date.parseDate("2000-01-01"), Date.parseDate("2022-06-16")))
                .build()));
    assertEquals(
        16,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .toStringArray(ImmutableList.of("foo", "a"))
                .build()));
    assertEquals(
        16,
        calculateSize(
            Mutation.newInsertBuilder("my_table")
                .set("my_col")
                .toJsonArray(ImmutableList.of("foo", "a"))
                .build()));
  }

  @Test
  public void testBuildMutationNulls() throws IOException {
    // TODO(b/237831798): Add support for ARRAY in COPY operations.
    ImmutableSet<TypeCode> unsupportedTypeCodes =
        ImmutableSet.of(
            TypeCode.TYPE_CODE_UNSPECIFIED, TypeCode.ARRAY, TypeCode.STRUCT, TypeCode.UNRECOGNIZED);

    Connection connection = mock(Connection.class);
    for (TypeCode typeCode : TypeCode.values()) {
      if (unsupportedTypeCodes.contains(typeCode)) {
        continue;
      }

      MutationWriter mutationWriter =
          new MutationWriter(
              CopyTransactionMode.ImplicitAtomic,
              connection,
              "my_table",
              ImmutableMap.of("col", typeCode),
              0,
              CSVFormat.POSTGRESQL_TEXT,
              false);
      CSVParser parser =
          CSVParser.parse(
              new StringReader("col\n\\N\n"), CSVFormat.POSTGRESQL_TEXT.withFirstRecordAsHeader());
      CSVRecord record = parser.getRecords().get(0);

      Mutation mutation = mutationWriter.buildMutation(record);

      assertEquals(String.format("Type code: %s", typeCode), 1, mutation.asMap().size());
    }
  }
}
