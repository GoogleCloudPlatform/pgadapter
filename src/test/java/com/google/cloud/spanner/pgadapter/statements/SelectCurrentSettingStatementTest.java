// Copyright 2024 Google LLC
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

import static com.google.cloud.spanner.pgadapter.statements.SelectCurrentSettingStatement.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.error.PGException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SelectCurrentSettingStatementTest {

  @Test
  public void testParse() {
    assertEquals("foo", parse("select current_setting('foo')").name);
    assertFalse(parse("select current_setting('foo')").missingOk);
    assertFalse(parse("select current_setting('foo', off)").missingOk);
    assertTrue(parse("select current_setting('foo', t)").missingOk);

    assertEquals(
        "spanner", parse("select current_setting('spanner.autocommit_dml_mode')").extension);
    assertEquals(
        "autocommit_dml_mode", parse("select current_setting('spanner.autocommit_dml_mode')").name);

    assertThrows(PGException.class, () -> parse("select"));
    assertThrows(PGException.class, () -> parse("select foo"));
    assertThrows(PGException.class, () -> parse("select current_setting"));
    assertThrows(PGException.class, () -> parse("select current_setting ("));
    assertThrows(PGException.class, () -> parse("select current_setting ('foo', 'bar')"));
    assertThrows(PGException.class, () -> parse("select current_setting ('foo', true, true)"));
    assertThrows(PGException.class, () -> parse("select current_setting ('foo', true"));
    assertThrows(PGException.class, () -> parse("select current_setting ('foo', true, 1)"));
    assertThrows(PGException.class, () -> parse("select current_setting ('foo', true), 1"));
    assertThrows(PGException.class, () -> parse("select current_setting ('foo.bar.baz', true)"));
    assertThrows(PGException.class, () -> parse("select current_setting('foo-bar')"));
  }
}
