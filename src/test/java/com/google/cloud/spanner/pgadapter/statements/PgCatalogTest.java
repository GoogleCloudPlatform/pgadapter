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
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.Tuple;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.cloud.spanner.pgadapter.utils.QueryPartReplacer.ReplacementStatus;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PgCatalogTest {
  @Test
  public void testReplaceCatalogTables() {}

  @Test
  public void testAddCommonTableExpressions() {
    Statement statement = Statement.of("select * from my_table");
    PgCatalog catalog = new PgCatalog(mock(SessionState.class), WellKnownClient.UNSPECIFIED);
    assertSame(statement, catalog.addCommonTableExpressions(statement, ImmutableList.of()));
  }

  @Test
  public void testSelectVersion() {
    Statement statement = Statement.of("select version(), pg_type.* from pg_type");
    SessionState sessionState = mock(SessionState.class);
    PgCatalog catalog = new PgCatalog(sessionState, WellKnownClient.UNSPECIFIED);

    when(sessionState.getServerVersion()).thenReturn("14.1");
    assertEquals(
        Statement.of("with pg_type\n" + "select '14.1', pg_type.* from pg_type"),
        catalog.addCommonTableExpressions(statement, ImmutableList.of("pg_type")));

    when(sessionState.getServerVersion()).thenReturn("9.4");
    assertEquals(
        Statement.of("with pg_type\n" + "select '9.4', pg_type.* from pg_type"),
        catalog.addCommonTableExpressions(statement, ImmutableList.of("pg_type")));
  }

  @Test
  public void testReplaceKnownUnsupportedFunctions() {
    SessionState sessionState = mock(SessionState.class);
    PgCatalog catalog = new PgCatalog(sessionState, WellKnownClient.UNSPECIFIED);

    Tuple<String, ReplacementStatus> result;
    result = catalog.replaceKnownUnsupportedFunctions(Statement.of("select * from foo"));
    assertEquals("select * from foo", result.x());
    assertEquals(ReplacementStatus.CONTINUE, result.y());

    result =
        catalog.replaceKnownUnsupportedFunctions(Statement.of("select pg_table_is_visible('foo')"));
    assertEquals("select true", result.x());
    assertEquals(ReplacementStatus.CONTINUE, result.y());

    result =
        catalog.replaceKnownUnsupportedFunctions(Statement.of("select pg_table_is_visible('foo')"));
    assertEquals("select true", result.x());
    assertEquals(ReplacementStatus.CONTINUE, result.y());
  }
}
