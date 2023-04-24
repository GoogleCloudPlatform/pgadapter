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

package com.google.cloud.spanner.pgadapter.utils;

import static org.junit.Assert.assertEquals;

import com.google.cloud.Tuple;
import com.google.cloud.spanner.pgadapter.utils.QueryPartReplacer.ReplacementStatus;
import com.google.common.base.Suppliers;
import java.util.regex.Pattern;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RegexQueryPartReplacerTest {

  @Test
  public void testReplaceString() {
    QueryPartReplacer replacer =
        RegexQueryPartReplacer.replace(Pattern.compile("foo\\(\\)"), "bar");
    assertEquals("select bar", replacer.replace("select foo()").x());
  }

  @Test
  public void testReplaceSupplier() {
    for (String replacement : new String[] {"bar1", "bar2"}) {
      QueryPartReplacer replacer =
          RegexQueryPartReplacer.replace(
              Pattern.compile("foo\\(\\)"), Suppliers.ofInstance(replacement));
      assertEquals("select " + replacement, replacer.replace("select foo()").x());
    }
  }

  @Test
  public void testReplaceAndStopString() {
    QueryPartReplacer replacer =
        RegexQueryPartReplacer.replaceAndStop(Pattern.compile("foo\\(\\)"), "bar");
    Tuple<String, ReplacementStatus> result = replacer.replace("select foo()");
    assertEquals("select bar", result.x());
    assertEquals(ReplacementStatus.STOP, result.y());
  }

  @Test
  public void testReplaceAndStopSupplier() {
    for (String replacement : new String[] {"bar1", "bar2"}) {
      QueryPartReplacer replacer =
          RegexQueryPartReplacer.replaceAndStop(
              Pattern.compile("foo\\(\\)"), Suppliers.ofInstance(replacement));
      Tuple<String, ReplacementStatus> result = replacer.replace("select foo()");
      assertEquals("select " + replacement, result.x());
      assertEquals(ReplacementStatus.STOP, result.y());
    }
  }

  @Test
  public void testReplaceAndStopNotMatchedString() {
    QueryPartReplacer replacer =
        RegexQueryPartReplacer.replaceAndStop(Pattern.compile("foo\\(\\)"), "bar");
    Tuple<String, ReplacementStatus> result = replacer.replace("select baz()");
    assertEquals("select baz()", result.x());
    assertEquals(ReplacementStatus.CONTINUE, result.y());
  }

  @Test
  public void testReplaceAndStopNotMatchedSupplier() {
    for (String replacement : new String[] {"bar1", "bar2"}) {
      QueryPartReplacer replacer =
          RegexQueryPartReplacer.replaceAndStop(
              Pattern.compile("foo\\(\\)"), Suppliers.ofInstance(replacement));
      Tuple<String, ReplacementStatus> result = replacer.replace("select baz()");
      assertEquals("select baz()", result.x());
      assertEquals(ReplacementStatus.CONTINUE, result.y());
    }
  }
}
