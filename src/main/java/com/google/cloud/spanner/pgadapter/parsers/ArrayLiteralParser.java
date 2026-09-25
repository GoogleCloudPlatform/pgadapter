// Copyright 2026 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import java.util.ArrayList;
import java.util.List;

/**
 * Dedicated parser for PostgreSQL text-format array literals (e.g. {@code {1, 2, 3}}, {@code
 * {"foo", "bar"}}).
 *
 * <p>Implements the PostgreSQL array literal grammar as specified in PostgreSQL's {@code
 * arrayfuncs.c}. Specifically:
 *
 * <ul>
 *   <li>Supports optional dimension decorations (e.g. {@code [1:2]={...}}).
 *   <li>Quoted elements preserve adjacent quotes (unlike SQL string literals) and only unescape
 *       literal backslash escapes {@code \c -> c}.
 *   <li>Unquoted elements are not parsed as SQL expressions or comments, preserving tokens like
 *       {@code --} or {@code /*}.
 *   <li>Fast-paths elements without backslash escapes to avoid StringBuilder allocations.
 * </ul>
 */
@InternalApi
public class ArrayLiteralParser {
  private final String expression;
  private final int length;
  private int position;

  private ArrayLiteralParser(String expression) {
    this.expression = expression;
    this.length = expression.length();
    this.position = 0;
  }

  /**
   * Parses a PostgreSQL text-format array literal into a list of element strings. Elements that
   * represent SQL NULL are represented as {@code null} in the returned list.
   */
  public static List<String> readArrayLiteral(String expression) {
    if (expression == null) {
      throw PGExceptionFactory.newPGException(
          "Array value cannot be null", SQLState.InvalidParameterValue);
    }
    return new ArrayLiteralParser(expression).parse();
  }

  private List<String> parse() {
    skipWhitespace();
    if (position >= length) {
      throw PGExceptionFactory.newPGException(
          "Missing '{' at start of array value: " + expression, SQLState.InvalidParameterValue);
    }
    skipDimensions();
    if (position >= length || expression.charAt(position) != '{') {
      throw PGExceptionFactory.newPGException(
          "Missing '{' at start of array value: " + expression, SQLState.InvalidParameterValue);
    }
    position++; // skip '{'

    skipWhitespace();
    if (position < length && expression.charAt(position) == '}') {
      position++; // skip '}'
      checkEndOfArray();
      return new ArrayList<>(0);
    }

    List<String> result = new ArrayList<>();
    while (true) {
      skipWhitespace();
      if (position >= length) {
        throw PGExceptionFactory.newPGException(
            "Missing '}' at end of array value: " + expression, SQLState.InvalidParameterValue);
      }
      char character = expression.charAt(position);
      if (character == '"') {
        result.add(parseQuotedElement());
      } else if (character == '}' || character == ',') {
        throw PGExceptionFactory.newPGException(
            "Invalid element in array: " + expression, SQLState.InvalidParameterValue);
      } else if (character == '{') {
        throw PGExceptionFactory.newPGException(
            "Multidimensional arrays are not supported: " + expression,
            SQLState.InvalidParameterValue);
      } else {
        result.add(parseUnquotedElement());
      }

      if (expression.charAt(position) == ',') {
        position++; // skip ',' and continue
      } else {
        position++; // skip '}' and finish
        break;
      }
    }

    checkEndOfArray();
    return result;
  }

  private void skipWhitespace() {
    while (position < length && isWhitespace(expression.charAt(position))) {
      position++;
    }
  }

  private static boolean isWhitespace(char character) {
    return character == ' '
        || character == '\t'
        || character == '\n'
        || character == '\r'
        || character == '\u000B'
        || character == '\f';
  }

  private void skipDimensions() {
    if (expression.charAt(position) != '[') {
      return;
    }
    int closeBracket = expression.indexOf(']', position);
    if (closeBracket == -1) {
      throw PGExceptionFactory.newPGException(
          "Missing ']' in array dimensions: " + expression, SQLState.InvalidParameterValue);
    }
    validateDimension(expression.substring(position + 1, closeBracket));
    position = closeBracket + 1;
    skipWhitespace();
    if (position < length && expression.charAt(position) == '[') {
      throw PGExceptionFactory.newPGException(
          "Multidimensional arrays are not supported: " + expression,
          SQLState.InvalidParameterValue);
    }
    if (position >= length || expression.charAt(position) != '=') {
      throw PGExceptionFactory.newPGException(
          "Missing '=' after array dimensions: " + expression, SQLState.InvalidParameterValue);
    }
    position++; // skip '='
    skipWhitespace();
  }

  private void validateDimension(String dimension) {
    int colonIndex = dimension.indexOf(':');
    try {
      if (colonIndex == -1) {
        long upper = Long.parseLong(dimension.trim());
        if (upper < 1) {
          throw PGExceptionFactory.newPGException(
              "Upper bound cannot be less than lower bound: " + expression,
              SQLState.InvalidParameterValue);
        }
      } else {
        long lower = Long.parseLong(dimension.substring(0, colonIndex).trim());
        long upper = Long.parseLong(dimension.substring(colonIndex + 1).trim());
        if (upper < lower) {
          throw PGExceptionFactory.newPGException(
              "Upper bound cannot be less than lower bound: " + expression,
              SQLState.InvalidParameterValue);
        }
      }
    } catch (NumberFormatException exception) {
      throw PGExceptionFactory.newPGException(
          "Invalid array dimensions: " + expression, SQLState.InvalidParameterValue);
    }
  }

  private String parseQuotedElement() {
    position++; // skip opening quote '"'
    int start = position;

    // Fast path: scan for closing quote or escape character.
    while (position < length) {
      char character = expression.charAt(position);
      if (character == '"') {
        String element = expression.substring(start, position);
        position++; // skip closing quote
        validateAfterQuotedElement();
        return element;
      }
      if (character == '\\') {
        return parseQuotedElementWithEscapes(start);
      }
      position++;
    }

    throw PGExceptionFactory.newPGException(
        "Missing end quote character in array value: " + expression,
        SQLState.InvalidParameterValue);
  }

  private String parseQuotedElementWithEscapes(int start) {
    StringBuilder element = new StringBuilder(position - start + 16);
    element.append(expression, start, position);
    boolean closed = false;
    while (position < length) {
      char character = expression.charAt(position);
      if (character == '\\') {
        position++;
        if (position >= length) {
          throw PGExceptionFactory.newPGException(
              "Unexpected end of array value: " + expression, SQLState.InvalidParameterValue);
        }
        element.append(expression.charAt(position));
        position++;
      } else if (character == '"') {
        closed = true;
        position++; // skip closing quote
        break;
      } else {
        element.append(character);
        position++;
      }
    }
    if (!closed) {
      throw PGExceptionFactory.newPGException(
          "Missing end quote character in array value: " + expression,
          SQLState.InvalidParameterValue);
    }
    validateAfterQuotedElement();
    return element.toString();
  }

  private void validateAfterQuotedElement() {
    skipWhitespace();
    if (position >= length) {
      throw PGExceptionFactory.newPGException(
          "Missing '}' at end of array value: " + expression, SQLState.InvalidParameterValue);
    }
    char next = expression.charAt(position);
    if (next != ',' && next != '}') {
      throw PGExceptionFactory.newPGException(
          "Incorrectly quoted array element: " + expression, SQLState.InvalidParameterValue);
    }
  }

  private String parseUnquotedElement() {
    int start = position;
    int lastNonWhitespace = start;

    // Fast path: scan until delimiter or escape.
    while (position < length) {
      char character = expression.charAt(position);
      if (character == ',' || character == '}') {
        int elementLength = lastNonWhitespace - start;
        if (elementLength == 4 && expression.regionMatches(true, start, "null", 0, 4)) {
          return null;
        }
        return expression.substring(start, lastNonWhitespace);
      }
      if (character == '\\') {
        return parseUnquotedElementWithEscapes(start);
      }
      if (character == '"') {
        throw PGExceptionFactory.newPGException(
            "Incorrectly quoted array element: " + expression, SQLState.InvalidParameterValue);
      }
      if (character == '{') {
        throw PGExceptionFactory.newPGException(
            "Unexpected '{' character in array value: " + expression,
            SQLState.InvalidParameterValue);
      }
      if (!isWhitespace(character)) {
        lastNonWhitespace = position + 1;
      }
      position++;
    }

    throw PGExceptionFactory.newPGException(
        "Missing '}' at end of array value: " + expression, SQLState.InvalidParameterValue);
  }

  private String parseUnquotedElementWithEscapes(int start) {
    StringBuilder element = new StringBuilder(position - start + 16);
    element.append(expression, start, position);
    int nonWhitespaceLength = 0;
    while (position < length) {
      char character = expression.charAt(position);
      if (character == '\\') {
        position++;
        if (position >= length) {
          throw PGExceptionFactory.newPGException(
              "Unexpected end of array value: " + expression, SQLState.InvalidParameterValue);
        }
        element.append(expression.charAt(position));
        nonWhitespaceLength = element.length();
        position++;
      } else if (character == ',' || character == '}') {
        break;
      } else if (character == '"') {
        throw PGExceptionFactory.newPGException(
            "Incorrectly quoted array element: " + expression, SQLState.InvalidParameterValue);
      } else if (character == '{') {
        throw PGExceptionFactory.newPGException(
            "Unexpected '{' character in array value: " + expression,
            SQLState.InvalidParameterValue);
      } else {
        element.append(character);
        if (!isWhitespace(character)) {
          nonWhitespaceLength = element.length();
        }
        position++;
      }
    }

    if (position >= length) {
      throw PGExceptionFactory.newPGException(
          "Missing '}' at end of array value: " + expression, SQLState.InvalidParameterValue);
    }
    element.setLength(nonWhitespaceLength);
    return element.toString();
  }

  private void checkEndOfArray() {
    skipWhitespace();
    if (position < length) {
      throw PGExceptionFactory.newPGException(
          "Unexpected characters after array value: " + expression, SQLState.InvalidParameterValue);
    }
  }
}
