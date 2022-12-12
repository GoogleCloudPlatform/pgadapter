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

import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.DOLLAR;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.DOUBLE_QUOTE;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.SINGLE_QUOTE;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.isValidIdentifierChar;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.isValidIdentifierFirstChar;

import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TypeDefinition;
import com.google.common.collect.ImmutableMap;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.commons.text.StringEscapeUtils;
import org.postgresql.core.Oid;

class LiteralParser {
  static final ImmutableMap<String, Integer> TYPE_NAME_TO_OID_MAPPING =
      ImmutableMap.<String, Integer>builder()
          .put("unknown", Oid.UNSPECIFIED)
          .put("bigint", Oid.INT8)
          .put("bigint[]", Oid.INT8_ARRAY)
          .put("int8", Oid.INT8)
          .put("int8[]", Oid.INT8_ARRAY)
          .put("int4", Oid.INT4)
          .put("int4[]", Oid.INT4_ARRAY)
          .put("int", Oid.INT4)
          .put("int[]", Oid.INT4_ARRAY)
          .put("integer", Oid.INT4)
          .put("integer[]", Oid.INT4_ARRAY)
          .put("boolean", Oid.BOOL)
          .put("boolean[]", Oid.BOOL_ARRAY)
          .put("bool", Oid.BOOL)
          .put("bool[]", Oid.BOOL_ARRAY)
          .put("bytea", Oid.BYTEA)
          .put("bytea[]", Oid.BYTEA_ARRAY)
          .put("character varying", Oid.VARCHAR)
          .put("character varying[]", Oid.VARCHAR_ARRAY)
          .put("varchar", Oid.VARCHAR)
          .put("varchar[]", Oid.VARCHAR_ARRAY)
          .put("date", Oid.DATE)
          .put("date[]", Oid.DATE_ARRAY)
          .put("double precision", Oid.FLOAT8)
          .put("double precision[]", Oid.FLOAT8_ARRAY)
          .put("float8", Oid.FLOAT8)
          .put("float8[]", Oid.FLOAT8_ARRAY)
          .put("jsonb", Oid.JSONB)
          .put("jsonb[]", Oid.JSONB_ARRAY)
          .put("numeric", Oid.NUMERIC)
          .put("numeric[]", Oid.NUMERIC_ARRAY)
          .put("decimal", Oid.NUMERIC)
          .put("decimal[]", Oid.NUMERIC_ARRAY)
          .put("text", Oid.TEXT)
          .put("text[]", Oid.TEXT_ARRAY)
          .put("timestamp with time zone", Oid.TIMESTAMPTZ)
          .put("timestamp with time zone[]", Oid.TIMESTAMPTZ_ARRAY)
          .put("timestamptz", Oid.TIMESTAMPTZ)
          .put("timestamptz[]", Oid.TIMESTAMPTZ_ARRAY)
          .build();

  static class Literal {
    final String value;
    final Integer castToOid;

    static Literal of(String value) {
      return new Literal(value, null);
    }

    static Literal of(String value, Integer castToOid) {
      return new Literal(value, castToOid);
    }

    private Literal(String value, Integer castToOid) {
      this.value = value;
      this.castToOid = castToOid;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Literal)) {
        return false;
      }
      Literal other = (Literal) o;
      return Objects.equals(value, other.value) && Objects.equals(castToOid, other.castToOid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, castToOid);
    }

    @Override
    public String toString() {
      return value + (castToOid == null ? "" : "::" + oidToDataTypeName(castToOid));
    }
  }

  static class QuotedString {
    final boolean escaped;
    final char quote;
    final String rawValue;
    private String value;

    QuotedString(boolean escaped, char quote, String rawValue) {
      this.escaped = escaped;
      this.quote = quote;
      this.rawValue = rawValue;
    }

    String getValue() {
      if (this.value == null) {
        this.value =
            this.escaped
                ? unescapeQuotedStringValue(this.rawValue, this.quote)
                : quotedStringValue(this.rawValue, this.quote);
      }
      return this.value;
    }

    static String quotedStringValue(String quotedString, char quoteChar) {
      if (quotedString.length() < 2
          || quotedString.charAt(0) != quoteChar
          || quotedString.charAt(quotedString.length() - 1) != quoteChar) {
        throw PGExceptionFactory.newPGException(
            quotedString + " is not a valid string", SQLState.SyntaxError);
      }
      String doubleQuotes = String.valueOf(quoteChar) + quoteChar;
      String singleQuote = String.valueOf(quoteChar);
      return quotedString
          .substring(1, quotedString.length() - 1)
          .replace(doubleQuotes, singleQuote);
    }

    static String unescapeQuotedStringValue(String quotedString, char quoteChar) {
      if (quotedString.length() < 2
          || quotedString.charAt(0) != quoteChar
          || quotedString.charAt(quotedString.length() - 1) != quoteChar) {
        throw PGExceptionFactory.newPGException(
            quotedString + " is not a valid string", SQLState.SyntaxError);
      }
      if (quotedString.startsWith(quoteChar + "\\x")) {
        throw PGExceptionFactory.newPGException(
            "PGAdapter does not support hexadecimal byte values in string literals",
            SQLState.SyntaxError);
      }
      String result =
          StringEscapeUtils.unescapeJava(quotedString.substring(1, quotedString.length() - 1));
      String doubleQuotes = String.valueOf(quoteChar) + quoteChar;
      String singleQuote = String.valueOf(quoteChar);
      return result.replace(doubleQuotes, singleQuote);
    }
  }

  static class DollarQuotedString {
    final String tag;
    final String value;

    DollarQuotedString(String tag, String value) {
      this.tag = tag;
      this.value = value;
    }
  }

  static QuotedString readSingleQuotedString(SimpleParser parser) {
    LiteralParser literalParser = new LiteralParser(parser);
    return literalParser.readQuotedString(SINGLE_QUOTE);
  }

  static QuotedString readDoubleQuotedString(SimpleParser parser) {
    LiteralParser literalParser = new LiteralParser(parser);
    return literalParser.readQuotedString(DOUBLE_QUOTE);
  }

  static int dataTypeNameToOid(String type) {
    SimpleParser parser = new SimpleParser(type);
    TypeDefinition typeDefinition = parser.readType();
    Integer oid =
        TYPE_NAME_TO_OID_MAPPING.get(typeDefinition.getNameAndArrayBrackets().toLowerCase());
    if (oid != null) {
      return oid;
    }
    throw PGExceptionFactory.newPGException("unknown type name: " + type);
  }

  static String oidToDataTypeName(int oid) {
    for (Entry<String, Integer> entry : TYPE_NAME_TO_OID_MAPPING.entrySet()) {
      if (entry.getValue() == oid) {
        return entry.getKey();
      }
    }
    throw PGExceptionFactory.newPGException("unknown oid: " + oid);
  }

  private final SimpleParser parser;

  LiteralParser(SimpleParser parser) {
    this.parser = parser;
  }

  /**
   * Reads a constant literal with a possible type cast. Does not support recursive casts or any
   * other constant expressions. That is; the following is supported:
   *
   * <ul>
   *   <li>'test'
   *   <li>e'test'
   *   <li>100
   *   <li>cast('test' as varchar)
   *   <li>varchar 'test'
   *   <li>varchar('test')
   *   <li>'test'::varchar
   * </ul>
   *
   * <p>The following is not supported:
   *
   * <ul>
   *   <li>`100+100`
   *   <li>`cast('100'::int as varchar)`
   *   <li>`(varchar '100')::int`
   * </ul>
   */
  Literal readConstantLiteralExpression() {
    parser.skipWhitespaces();
    boolean cast = false;
    TypeDefinition precedingTypeDefinition = null;
    boolean functionStyleTypeCast = false;
    if (parser.eatKeyword("cast")) {
      if (!parser.eatToken("(")) {
        throw PGExceptionFactory.newPGException(
            "Missing opening parentheses for CAST: " + parser.getSql(), SQLState.SyntaxError);
      }
      cast = true;
    } else {
      for (String typeName : TYPE_NAME_TO_OID_MAPPING.keySet()) {
        if (parser.peekKeyword(typeName)) {
          precedingTypeDefinition = parser.readType();
          functionStyleTypeCast = parser.eatToken("(");
          break;
        }
      }
    }
    String value = readLiteralValue(precedingTypeDefinition != null);
    TypeDefinition typeDefinition = null;
    if (precedingTypeDefinition != null) {
      if (functionStyleTypeCast) {
        if (!parser.eatToken(")")) {
          throw PGExceptionFactory.newPGException(
              String.format(
                  "Missing closing parentheses for %s: %s",
                  precedingTypeDefinition.name, parser.getSql()),
              SQLState.SyntaxError);
        }
      }
      typeDefinition = precedingTypeDefinition;
    } else if (cast) {
      typeDefinition = eatAsType();
      if (!parser.eatToken(")")) {
        throw PGExceptionFactory.newPGException(
            String.format("Missing closing parentheses for CAST: %s", parser.getSql()),
            SQLState.SyntaxError);
      }
    } else {
      // Check for the '::' cast operator.
      if (parser.eatToken("::")) {
        typeDefinition = parser.readType();
      }
    }
    if (typeDefinition != null) {
      return new Literal(value, dataTypeNameToOid(typeDefinition.name));
    }
    return new Literal(value, null);
  }

  String readLiteralValue(boolean mustBeQuoted) {
    parser.skipWhitespaces();
    if (parser.getPos() >= parser.getSql().length()) {
      throw PGExceptionFactory.newPGException("Invalid literal: " + parser.getSql());
    }
    if (parser.peekCharsIgnoreCase("'")
        || parser.peekCharsIgnoreCase("e'")
        || parser.peekCharsIgnoreCase("b'")
        || parser.peekCharsIgnoreCase("x'")
        || parser.peekCharsIgnoreCase("u&'")) {
      QuotedString quotedString = readQuotedString('\'');
      return quotedString.getValue();
    } else if (parser.getSql().charAt(parser.getPos()) == DOLLAR
        && parser.getSql().length() > (parser.getPos() + 1)
        && (parser.getSql().charAt(parser.getPos() + 1) == DOLLAR
            || isValidIdentifierFirstChar(parser.getSql().charAt(parser.getPos() + 1)))
        && parser.getSql().indexOf(DOLLAR, parser.getPos() + 1) > -1) {
      DollarQuotedString dollarQuotedString = readDollarQuotedString();
      return dollarQuotedString.value;
    } else if (mustBeQuoted) {
      throw PGExceptionFactory.newPGException(
          "Expression must be a quoted string", SQLState.SyntaxError);
    } else {
      return readNumericLiteralValue();
    }
  }

  String readNumericLiteralValue() {
    int startPos = parser.getPos();

    // Accept a leading sign.
    if (currentChar() == '+') {
      incPos();
    } else if (currentChar() == '-') {
      incPos();
    }
    // Note that this loop will continue as long as the literal contains valid characters for a
    // numeric literal, or other characters that would otherwise not indicate the end of the token.
    // That means that this method may return something like "100abc". This will then fail at a
    // later moment when it is being converted to an actual number.
    while (isValidPos()
        && (currentChar() == '.'
            || Character.isDigit(currentChar())
            || currentChar() == '+'
            || currentChar() == '-'
            || isValidIdentifierChar(currentChar()))) {
      if (currentChar() == '+' || currentChar() == '-') {
        if (!(prevChar() == 'e' || prevChar() == 'E')) {
          break;
        }
      }
      incPos();
    }
    return parser.getSql().substring(startPos, parser.getPos());
  }

  private char currentChar() {
    return parser.getSql().charAt(parser.getPos());
  }

  private char prevChar() {
    if (parser.getPos() == 0) {
      return 0;
    }
    return parser.getSql().charAt(parser.getPos() - 1);
  }

  private boolean isValidPos() {
    return parser.getPos() < parser.getSql().length();
  }

  private void incPos() {
    parser.setPos(parser.getPos() + 1);
  }

  private TypeDefinition eatAsType() {
    if (!parser.eatKeyword("as")) {
      throw PGExceptionFactory.newPGException("Missing AS keyword in CAST: " + parser.getSql());
    }
    return parser.readType();
  }

  QuotedString readQuotedString(char quote) {
    parser.skipWhitespaces();
    if (parser.getPos() >= parser.getSql().length()) {
      throw PGExceptionFactory.newPGException("Unexpected end of expression", SQLState.SyntaxError);
    }
    boolean escaped = parser.eatToken("e");
    if (parser.getSql().charAt(parser.getPos()) != quote) {
      throw PGExceptionFactory.newPGException(
          "Invalid quote character: " + parser.getSql().charAt(parser.getPos()),
          SQLState.SyntaxError);
    }
    int startPos = parser.getPos();
    if (parser.skipQuotedString(escaped)) {
      return new QuotedString(escaped, quote, parser.getSql().substring(startPos, parser.getPos()));
    }
    throw PGExceptionFactory.newPGException("Missing end quote character", SQLState.SyntaxError);
  }

  DollarQuotedString readDollarQuotedString() {
    int startPos = parser.getPos();
    if (!parser.eatToken("$")) {
      throw PGExceptionFactory.newPGException("Missing expected token: '$'");
    }
    String tag = parser.parseDollarQuotedTag();
    parser.setPos(startPos);
    if (!parser.skipDollarQuotedString()) {
      throw PGExceptionFactory.newPGException("Invalid dollar-quoted string: " + parser.getSql());
    }
    String rawValue = parser.getSql().substring(startPos, parser.getPos());
    String value = rawValue.substring(tag.length() + 2, rawValue.length() - tag.length() - 2);

    return new DollarQuotedString(tag, value);
  }
}
