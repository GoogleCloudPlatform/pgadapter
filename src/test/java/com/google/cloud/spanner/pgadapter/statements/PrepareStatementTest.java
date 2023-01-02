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

import static com.google.cloud.spanner.pgadapter.statements.PrepareStatement.parse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.statements.PrepareStatement.ParsedPreparedStatement;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;

@RunWith(JUnit4.class)
public class PrepareStatementTest {

  @Test
  public void testParse() {
    ParsedPreparedStatement statement = parse("prepare foo as select 1");
    assertEquals("foo", statement.name);
    assertArrayEquals(new int[] {}, statement.dataTypes);
    assertEquals("select 1", statement.originalPreparedStatement.getSql());

    statement = parse("prepare foo (boolean) as select * from bar where active=$1");
    assertEquals("foo", statement.name);
    assertArrayEquals(new int[] {Oid.BOOL}, statement.dataTypes);
    assertEquals("select * from bar where active=$1", statement.originalPreparedStatement.getSql());

    statement =
        parse("prepare foo (boolean, bigint) as select * from bar where active=$1 and age=$2");
    assertEquals("foo", statement.name);
    assertArrayEquals(new int[] {Oid.BOOL, Oid.INT8}, statement.dataTypes);
    assertEquals(
        "select * from bar where active=$1 and age=$2",
        statement.originalPreparedStatement.getSql());

    statement =
        parse(
            "prepare foo ("
                + "\tboolean,\n"
                + "\tbigint,\n"
                + "\tcharacter varying(100)\n"
                + ") as select * from bar where active=$1 and age=$2");
    assertEquals("foo", statement.name);
    assertArrayEquals(new int[] {Oid.BOOL, Oid.INT8, Oid.VARCHAR}, statement.dataTypes);
    assertEquals(
        "select * from bar where active=$1 and age=$2",
        statement.originalPreparedStatement.getSql());

    assertThrows(PGException.class, () -> parse("prpra foo ( as select 1"));
    assertThrows(PGException.class, () -> parse("prepare foo ( as select 1"));
    assertThrows(PGException.class, () -> parse("prepare foo () as select 1"));
    assertThrows(PGException.class, () -> parse("prepare (bigint) as select 1"));
    assertThrows(PGException.class, () -> parse("prepare as select 1"));
  }
}
