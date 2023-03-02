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

package com.google.cloud.spanner.pgadapter.statements;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement.ParsedCopyStatement;
import org.apache.commons.csv.QuoteMode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class CopyToStatementTest {
  @Rule public MockitoRule rule = MockitoJUnit.rule();
  @Mock private ConnectionHandler connectionHandler;
  @Mock private OptionsMetadata options;
  @Mock private ConnectionMetadata connectionMetadata;

  @Test
  public void testDelimiter() {
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ParsedCopyStatement parsedCopyStatement =
        CopyStatement.parse("copy t to stdout (delimiter '~')");

    CopyToStatement statement =
        new CopyToStatement(connectionHandler, options, "", parsedCopyStatement);
    assertEquals('~', statement.getCsvFormat().getDelimiterString().charAt(0));
  }

  @Test
  public void testNullString() {
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ParsedCopyStatement parsedCopyStatement =
        CopyStatement.parse("copy t to stdout null 'this-is-null'");

    CopyToStatement statement =
        new CopyToStatement(connectionHandler, options, "", parsedCopyStatement);
    assertEquals("this-is-null", statement.getCsvFormat().getNullString());
  }

  @Test
  public void testEscape() {
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ParsedCopyStatement parsedCopyStatement =
        CopyStatement.parse("copy t to stdout (format csv, escape '&')");

    CopyToStatement statement =
        new CopyToStatement(connectionHandler, options, "", parsedCopyStatement);
    assertEquals('&', statement.getCsvFormat().getEscapeCharacter().charValue());
  }

  @Test
  public void testQuote() {
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ParsedCopyStatement parsedCopyStatement =
        CopyStatement.parse("copy t to stdout (format csv, quote '^')");

    CopyToStatement statement =
        new CopyToStatement(connectionHandler, options, "", parsedCopyStatement);
    assertEquals('^', statement.getCsvFormat().getQuoteCharacter().charValue());
    assertEquals(QuoteMode.MINIMAL, statement.getCsvFormat().getQuoteMode());
  }

  @Test
  public void testRecordSeparator() {
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ParsedCopyStatement parsedCopyStatement = CopyStatement.parse("copy t to stdout");

    CopyToStatement statement =
        new CopyToStatement(connectionHandler, options, "", parsedCopyStatement);
    assertEquals("\n", statement.getCsvFormat().getRecordSeparator());
  }

  @Test
  public void testCopyQuery() {
    String query =
        "select executed_at, workload, threads, batch_size, operation_count, round(read_avg/1000) as read_avg, round(read_p95/1000) as read_p95 from run where true and workload='a' and threads=20 and deployment='java_uds' order by executed_at desc limit 100";
    ParsedCopyStatement parsedCopyStatement =
        CopyStatement.parse(String.format("copy (%s) to stdout\n", query));
    assertNotNull(parsedCopyStatement);
    assertEquals(parsedCopyStatement.query, query);
  }
}
