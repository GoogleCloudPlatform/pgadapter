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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter.CopyTransactionMode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
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
    MutationWriter mutationWriter =
        new MutationWriter(
            CopyTransactionMode.ImplicitAtomic,
            connection,
            databaseClient,
            "numbers",
            tableColumns,
            /* indexedColumnsCount = */ 1,
            format,
            false);

    mutationWriter.addCopyData("1\t\"One\"\n2\t\"Two\"\n".getBytes(StandardCharsets.UTF_8));
    mutationWriter.close();

    long updateCount = mutationWriter.call();

    assertEquals(2L, updateCount);
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
      assertTrue(exception.getMessage().contains("Record count: 2 has exceeded the limit: 1"));

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

      MutationWriter mutationWriter =
          new MutationWriter(
              CopyTransactionMode.ImplicitNonAtomic,
              connection,
              databaseClient,
              "numbers",
              tableColumns,
              /* indexedColumnsCount = */ 1,
              format,
              false);

      mutationWriter.addCopyData(
          "1\t\"One\"\n2\t\"Two\"\n3\t\"Three\"\n4\t\"Four\"\n5\t\"Five\"\n"
              .getBytes(StandardCharsets.UTF_8));
      mutationWriter.close();

      long updateCount = mutationWriter.call();

      assertEquals(5L, updateCount);
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

      MutationWriter mutationWriter =
          new MutationWriter(
              CopyTransactionMode.ImplicitNonAtomic,
              connection,
              databaseClient,
              "numbers",
              tableColumns,
              /* indexedColumnsCount = */ 1,
              format,
              false);

      mutationWriter.addCopyData(
          "1\t\"One\"\n2\t\"Two\"\n3\t\"Three\"\n4\t\"Four\"\n5\t\"Five\"\n"
              .getBytes(StandardCharsets.UTF_8));
      mutationWriter.close();

      long updateCount = mutationWriter.call();

      assertEquals(5L, updateCount);
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

    MutationWriter mutationWriter =
        new MutationWriter(
            CopyTransactionMode.ImplicitNonAtomic,
            connection,
            databaseClient,
            "numbers",
            tableColumns,
            /* indexedColumnsCount = */ 1,
            format,
            false);

    mutationWriter.addCopyData(
        "1\t\"One\"\n2\t\"Two\"\n3\t\"Three\"\n4\t\"Four\"\n5\t\"Five\"\n"
            .getBytes(StandardCharsets.UTF_8));
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
    Future<Long> updateCount = executor.submit(mutationWriter);

    assertEquals(5L, updateCount.get().longValue());
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
}
