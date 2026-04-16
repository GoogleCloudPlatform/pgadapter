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

import static com.google.cloud.spanner.pgadapter.statements.DeallocateStatement.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DeallocateStatementTest {

  @Test
  public void testParse() {
    assertEquals("foo", parse("deallocate foo").name);
    assertEquals("foo", parse("deallocate prepare foo").name);
    assertEquals("foo", parse("deallocate FOO").name);
    assertEquals("foo", parse("deallocate\tfoo").name);
    assertEquals("foo", parse("deallocate\nfoo").name);
    assertEquals("foo", parse("deallocate/*comment*/foo").name);
    assertEquals("foo", parse("deallocate \"foo\"").name);
    assertEquals("Foo", parse("deallocate \"Foo\"").name);
    assertEquals("prepare", parse("deallocate \"prepare\"").name);
    assertNull(parse("deallocate all").name);
    assertEquals("all", parse("deallocate \"all\"").name);

    assertThrows(PGException.class, () -> parse("prepare foo"));
    assertThrows(PGException.class, () -> parse("deallocate"));
    assertThrows(PGException.class, () -> parse("deallocate prepare"));
    assertThrows(PGException.class, () -> parse("deallocate foo bar"));
    assertThrows(PGException.class, () -> parse("deallocate foo.bar"));
  }

  @Test
  public void testGetCommandTag() {
    assertEquals("DEALLOCATE", createStatement("deallocate foo").getCommandTag());
    assertEquals("DEALLOCATE", createStatement("deallocate prepare foo").getCommandTag());
    assertEquals("DEALLOCATE ALL", createStatement("deallocate all").getCommandTag());
  }

  private static DeallocateStatement createStatement(String sql) {
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    when(connectionHandler.getConnectionMetadata()).thenReturn(mock(ConnectionMetadata.class));
    return new DeallocateStatement(
        connectionHandler,
        mock(OptionsMetadata.class),
        "",
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(Statement.of(sql)),
        Statement.of(sql));
  }
}
