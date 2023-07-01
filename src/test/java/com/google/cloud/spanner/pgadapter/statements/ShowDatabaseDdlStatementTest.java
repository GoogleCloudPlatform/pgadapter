// Copyright 2023 Google LLC
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

import static com.google.cloud.spanner.pgadapter.statements.ShowDatabaseDdlStatement.parse;
import static org.junit.Assert.*;

import com.google.cloud.spanner.pgadapter.error.PGException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ShowDatabaseDdlStatementTest {

  @Test
  public void testParse() {
    assertFalse(parse("show database ddl").forPostgreSQL);
    assertTrue(parse("show database ddl for postgresql").forPostgreSQL);

    assertThrows(PGException.class, () -> parse("show"));
    assertThrows(PGException.class, () -> parse("show database"));
    assertThrows(PGException.class, () -> parse("show ddl"));
    assertThrows(PGException.class, () -> parse("show database ddl for"));
    assertThrows(PGException.class, () -> parse("show database ddl for postgres"));
    assertThrows(PGException.class, () -> parse("show database ddl for spanner"));
  }
}
