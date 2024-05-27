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

import static com.google.cloud.spanner.pgadapter.statements.DiscardStatement.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.statements.DiscardStatement.ParsedDiscardStatement.DiscardType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DiscardStatementTest {

  @Test
  public void testParse() {
    assertEquals(DiscardType.ALL, parse("discard all").type);
    assertEquals(DiscardType.PLANS, parse("discard plans").type);
    assertEquals(DiscardType.SEQUENCES, parse("discard sequences").type);
    assertEquals(DiscardType.TEMPORARY, parse("discard temp").type);
    assertEquals(DiscardType.TEMPORARY, parse("discard temporary").type);
    assertEquals(DiscardType.TEMPORARY, parse("discard/*comment*/temporary").type);

    assertThrows(PGException.class, () -> parse("discard foo"));
    assertThrows(PGException.class, () -> parse("discard"));
    assertThrows(PGException.class, () -> parse("deallocate all foo"));
    assertThrows(PGException.class, () -> parse("discard temp all"));
  }
}
