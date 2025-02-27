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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import org.junit.Test;

public class SimpleQueryStatementTest {

  @Test
  public void testReplaceKnownUnsupportedQueriesWithLikeEscape() {
    ParsedStatement parsedStatement = mock(ParsedStatement.class);
    when(parsedStatement.getSqlWithoutComments())
        .thenReturn("SELECT * FROM users WHERE name LIKE 'test%' ESCAPE '\\'");

    OptionsMetadata options = mock(OptionsMetadata.class);
    WellKnownClient client = WellKnownClient.JDBC;

    ParsedStatement result =
        SimpleQueryStatement.replaceKnownUnsupportedQueries(client, options, parsedStatement);

    assertNotNull(result);
    assertEquals(
        "Should transform LIKE ESCAPE query",
        "SELECT * FROM users WHERE name LIKE 'test%'",
        result.getSqlWithoutComments());
  }

  @Test
  public void testReplaceKnownUnsupportedQueriesWithoutModification() {
    ParsedStatement parsedStatement = mock(ParsedStatement.class);
    when(parsedStatement.getSqlWithoutComments()).thenReturn("SELECT * FROM users WHERE id = 1");

    OptionsMetadata options = mock(OptionsMetadata.class);
    WellKnownClient client = WellKnownClient.JDBC;

    ParsedStatement result =
        SimpleQueryStatement.replaceKnownUnsupportedQueries(client, options, parsedStatement);

    assertNotNull(result);
    assertEquals(
        "Should return the same statement if no changes are needed", parsedStatement, result);
  }

  @Test
  public void testReplaceKnownUnsupportedQueriesWithMultipleTransformations() {
    ParsedStatement parsedStatement = mock(ParsedStatement.class);
    when(parsedStatement.getSqlWithoutComments())
        .thenReturn(
            "SELECT * FROM users WHERE name LIKE 'test%' ESCAPE '\\' AND city LIKE 'NY%' ESCAPE '*'");

    OptionsMetadata options = mock(OptionsMetadata.class);
    WellKnownClient client = WellKnownClient.JDBC;

    ParsedStatement result =
        SimpleQueryStatement.replaceKnownUnsupportedQueries(client, options, parsedStatement);

    assertNotNull(result);
    assertEquals(
        "Should remove ESCAPE clauses from multiple LIKE statements",
        "SELECT * FROM users WHERE name LIKE 'test%' AND city LIKE 'NY%'",
        result.getSqlWithoutComments());
  }
}
