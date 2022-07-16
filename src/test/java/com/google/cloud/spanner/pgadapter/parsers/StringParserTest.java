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

package com.google.cloud.spanner.pgadapter.parsers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StringParserTest {

  @Test
  public void testToString() {
    assertEquals("", StringParser.toString(new byte[] {}));
    assertEquals("test", StringParser.toString("test".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testStringParse() {
    assertEquals("test", new StringParser("test").stringParse());
    assertNull(new StringParser(null).stringParse());
  }
}
