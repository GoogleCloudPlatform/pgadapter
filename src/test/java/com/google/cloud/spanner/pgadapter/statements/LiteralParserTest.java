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

import static com.google.cloud.spanner.pgadapter.statements.LiteralParser.dataTypeNameToOid;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.statements.LiteralParser.Literal;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;

@RunWith(JUnit4.class)
public class LiteralParserTest {

  @Test
  public void testDataTypeNameToOid() {
    assertEquals(Oid.UNSPECIFIED, dataTypeNameToOid("unknown"));
    assertEquals(Oid.INT8, dataTypeNameToOid("bigint"));
    assertEquals(Oid.INT8_ARRAY, dataTypeNameToOid("bigint[]"));
    assertEquals(Oid.INT8, dataTypeNameToOid("int8"));
    assertEquals(Oid.INT8_ARRAY, dataTypeNameToOid("int8[]"));
    assertEquals(Oid.INT4, dataTypeNameToOid("int"));
    assertEquals(Oid.INT4_ARRAY, dataTypeNameToOid("int[]"));
    assertEquals(Oid.INT4, dataTypeNameToOid("int4"));
    assertEquals(Oid.INT4_ARRAY, dataTypeNameToOid("int4[]"));
    assertEquals(Oid.INT4, dataTypeNameToOid("integer"));
    assertEquals(Oid.INT4_ARRAY, dataTypeNameToOid("integer[]"));
    assertEquals(Oid.BOOL, dataTypeNameToOid("boolean"));
    assertEquals(Oid.BOOL_ARRAY, dataTypeNameToOid("boolean[]"));
    assertEquals(Oid.BOOL, dataTypeNameToOid("bool"));
    assertEquals(Oid.BOOL_ARRAY, dataTypeNameToOid("bool[]"));
    assertEquals(Oid.DATE, dataTypeNameToOid("date"));
    assertEquals(Oid.DATE_ARRAY, dataTypeNameToOid("date[]"));
    assertEquals(Oid.FLOAT8, dataTypeNameToOid("double precision"));
    assertEquals(Oid.FLOAT8_ARRAY, dataTypeNameToOid("double precision[]"));
    assertEquals(Oid.FLOAT8, dataTypeNameToOid("float8"));
    assertEquals(Oid.FLOAT8_ARRAY, dataTypeNameToOid("float8[]"));
    assertEquals(Oid.JSONB, dataTypeNameToOid("jsonb"));
    assertEquals(Oid.JSONB_ARRAY, dataTypeNameToOid("jsonb[]"));
    assertEquals(Oid.NUMERIC, dataTypeNameToOid("numeric"));
    assertEquals(Oid.NUMERIC_ARRAY, dataTypeNameToOid("numeric[]"));
    assertEquals(Oid.NUMERIC, dataTypeNameToOid("decimal"));
    assertEquals(Oid.NUMERIC_ARRAY, dataTypeNameToOid("decimal[]"));
    assertEquals(Oid.NUMERIC, dataTypeNameToOid("numeric(1,1)"));
    assertEquals(Oid.NUMERIC_ARRAY, dataTypeNameToOid("numeric(2, 1)[]"));

    assertEquals(Oid.VARCHAR, dataTypeNameToOid("character varying"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("varchar"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("character varying(100)"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("varchar(100)"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("character varying (100)"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("varchar (100)"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("character     varying   ( 100 )  \t"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("varchar\t(100)  \n"));

    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("character varying[]"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("varchar[]"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("character varying(100)[]"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("varchar(100)[]"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("character varying (100) []"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("varchar (100) []"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("character     varying   ( 100 )  \t[\n]"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("varchar\t(100)  \n[  ]"));

    assertThrows(PGException.class, () -> dataTypeNameToOid("invalid_type"));
    assertThrows(PGException.class, () -> dataTypeNameToOid("varchar(100"));
    assertThrows(PGException.class, () -> dataTypeNameToOid("bigint["));
    assertThrows(PGException.class, () -> dataTypeNameToOid("varchar(bar)"));
    assertThrows(PGException.class, () -> dataTypeNameToOid("numeric(10, bar)"));
  }

  @Test
  public void testReadNumericLiteralValue() {
    assertEquals("100", readNumericLiteral("100"));
    assertEquals("100.1", readNumericLiteral("100.1"));
    assertEquals("100", readNumericLiteral("100 "));
    assertEquals("100.1", readNumericLiteral("100.1 "));
    assertEquals("100", readNumericLiteral("100,"));
    assertEquals("100.1", readNumericLiteral("100.1,"));
    // Note: We don't try to validate the numeric literal here, we only want to detect the end of
    // the literal.
    assertEquals("100a", readNumericLiteral("100a"));
    assertEquals("100.1a", readNumericLiteral("100.1a"));
    assertEquals("100", readNumericLiteral("100+100"));
    assertEquals("100.1", readNumericLiteral("100.1+100"));
    assertEquals("100e100", readNumericLiteral("100e100"));
    assertEquals("100.1e100", readNumericLiteral("100.1e100"));
    assertEquals("100e+100", readNumericLiteral("100e+100"));
    assertEquals("100.1e+100", readNumericLiteral("100.1e+100"));
    assertEquals("100e-100", readNumericLiteral("100e-100"));
    assertEquals("100.1e-100", readNumericLiteral("100.1e-100"));

    assertEquals("+100", readNumericLiteral("+100"));
    assertEquals("-100", readNumericLiteral("-100"));
    assertEquals("+", readNumericLiteral("+-100"));
    assertEquals("-", readNumericLiteral("-+100"));
    assertEquals("", readNumericLiteral("*100"));
    assertEquals("", readNumericLiteral("/100"));
  }

  @Test
  public void testReadLiteralValue() {
    assertEquals("100", readLiteralValue("100", false));
    assertEquals("100", readLiteralValue("100 200", false));
    assertEquals("test", readLiteralValue("'test'", false));
    assertEquals("test", readLiteralValue("e'test'", false));
    assertEquals("test", readLiteralValue("E'test'", false));
    assertEquals("test", readLiteralValue("'test'", true));
    assertEquals("test", readLiteralValue("e'test'", true));
    assertEquals("test", readLiteralValue("E'test'", true));
    assertEquals("test", readLiteralValue("'test', 100", false));
    assertEquals("test", readLiteralValue("'test'100", false));
    assertEquals("test", readLiteralValue("'test' 'test'", false));
    assertEquals("test", readLiteralValue("$$test$$", false));
    assertEquals("test", readLiteralValue("$tag$test$tag$", false));
    assertEquals("test", readLiteralValue("$$test$$", true));
    assertEquals("test", readLiteralValue("$tag$test$tag$", true));
  }

  @Test
  public void testReadConstantLiteralExpression() {
    assertEquals(Literal.of("100"), readConstantLiteralExpression("100"));
    assertEquals(Literal.of("100"), readConstantLiteralExpression("100 200"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("'test'"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("e'test'"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("E'test'"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("'test'"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("e'test'"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("E'test'"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("'test', 100"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("'test'100"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("'test' 'test'"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("$$test$$"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("$tag$test$tag$"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("$$test$$"));
    assertEquals(Literal.of("test"), readConstantLiteralExpression("$tag$test$tag$"));

    assertEquals(
        Literal.of("100", Oid.VARCHAR), readConstantLiteralExpression("cast(100 as varchar)"));
    assertEquals(
        Literal.of("100", Oid.VARCHAR), readConstantLiteralExpression("cast(100 as varchar(10))"));
    assertEquals(Literal.of("100", Oid.VARCHAR), readConstantLiteralExpression("varchar '100'"));
    assertEquals(
        Literal.of("2022-12-12T20:09:00+01:00", Oid.TIMESTAMPTZ),
        readConstantLiteralExpression("timestamptz '2022-12-12T20:09:00+01:00'"));
    assertEquals(
        Literal.of("test", Oid.VARCHAR), readConstantLiteralExpression("varchar $$test$$"));
    assertEquals(
        Literal.of("test", Oid.VARCHAR),
        readConstantLiteralExpression("cast($tag$test$tag$ as varchar)"));
    assertEquals(
        Literal.of("2022-12-12", Oid.DATE),
        readConstantLiteralExpression("$tag$2022-12-12$tag$::date"));
  }

  static String readNumericLiteral(String input) {
    LiteralParser literalParser = new LiteralParser(new SimpleParser(input));
    return literalParser.readNumericLiteralValue();
  }

  static String readLiteralValue(String input, boolean mustBeQuoted) {
    LiteralParser literalParser = new LiteralParser(new SimpleParser(input));
    return literalParser.readLiteralValue(mustBeQuoted);
  }

  static Literal readConstantLiteralExpression(String input) {
    LiteralParser literalParser = new LiteralParser(new SimpleParser(input));
    return literalParser.readConstantLiteralExpression();
  }
}
