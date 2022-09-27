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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JsonbParserTest {

  @Test
  public void testStringParse() {
    assertEquals(
        "{\"key\": \"value\"}",
        new JsonbParser("{\"key\": \"value\"}".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .stringParse());
    assertEquals(
        "{\"key\": \"value\"}",
        new JsonbParser(
                "\1{\"key\": \"value\"}".getBytes(StandardCharsets.UTF_8), FormatCode.BINARY)
            .stringParse());
    assertEquals(
        "", new JsonbParser("".getBytes(StandardCharsets.UTF_8), FormatCode.BINARY).stringParse());
    assertEquals(
        "",
        new JsonbParser("\1".getBytes(StandardCharsets.UTF_8), FormatCode.BINARY).stringParse());
    assertNull(new JsonbParser(null).stringParse());
    assertNull(new JsonbParser(null, FormatCode.TEXT).stringParse());
    assertNull(new JsonbParser(null, FormatCode.BINARY).stringParse());

    assertThrows(
        PGException.class,
        () ->
            new JsonbParser(
                    "{\"key\": \"value\"}".getBytes(StandardCharsets.UTF_8), FormatCode.BINARY)
                .stringParse());
  }

  @Test
  public void testBinaryParse() {
    assertArrayEquals(
        "\1{\"key\": \"value\"}".getBytes(StandardCharsets.UTF_8),
        new JsonbParser("{\"key\": \"value\"}".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .binaryParse());
    assertArrayEquals(
        "\1{\"key\": \"value\"}".getBytes(StandardCharsets.UTF_8),
        new JsonbParser(
                "\1{\"key\": \"value\"}".getBytes(StandardCharsets.UTF_8), FormatCode.BINARY)
            .binaryParse());
    assertArrayEquals(
        "\1".getBytes(StandardCharsets.UTF_8),
        new JsonbParser("".getBytes(StandardCharsets.UTF_8), FormatCode.BINARY).binaryParse());
    assertArrayEquals(
        "\1".getBytes(StandardCharsets.UTF_8),
        new JsonbParser("\1".getBytes(StandardCharsets.UTF_8), FormatCode.BINARY).binaryParse());
    assertNull(new JsonbParser(null).binaryParse());
    assertNull(new JsonbParser(null, FormatCode.TEXT).binaryParse());
    assertNull(new JsonbParser(null, FormatCode.BINARY).binaryParse());
  }

  @Test
  public void testBind() {
    Statement.Builder builder = Statement.newBuilder("select $1");
    new JsonbParser("{\"key\": \"value\"}".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
        .bind(builder, "p1");
    assertEquals("{\"key\": \"value\"}", builder.build().getParameters().get("p1").getString());
  }
}
