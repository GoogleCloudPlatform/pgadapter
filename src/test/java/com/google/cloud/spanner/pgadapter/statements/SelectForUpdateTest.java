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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SelectForUpdateTest {

  @Test
  public void testDetectSelectForUpdate() {
    assertEquals(" for update", parseForClause("select * from foo for update"));
    assertEquals(" for update", parseForClause("select bar, baz, 1 from foo for update"));
    assertEquals(" for update", parseForClause("select * from (select 1 from test) t for update"));
    assertEquals(
        " for update", parseForClause("select * from (select 1 from test for share) t for update"));
  }

  private String parseForClause(String sql) {
    SimpleParser parser = new SimpleParser(sql);
    parser.parseExpressionUntilKeyword(ImmutableList.of("for"), true, false, false);
    return parser.getSql().substring(parser.getPos());
  }
}
