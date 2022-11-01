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

import static com.google.cloud.spanner.pgadapter.error.PGExceptionFactory.checkArgument;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.statements.VacuumStatement.ParsedVacuumStatement.Builder;
import com.google.cloud.spanner.pgadapter.statements.VacuumStatement.ParsedVacuumStatement.IndexCleanup;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;

public class VacuumStatement extends IntermediatePortalStatement {
  static final class ParsedVacuumStatement {
    enum IndexCleanup {
      AUTO,
      ON,
      OFF;
    }

    static final class Builder {
      final ImmutableList.Builder<TableOrIndexName> tables = ImmutableList.builder();
      final ImmutableMap.Builder<TableOrIndexName, ImmutableList<TableOrIndexName>> columns =
          ImmutableMap.builder();
      boolean full;
      boolean freeze;
      boolean verbose;
      boolean analyze;
      boolean disablePageSkipping;
      boolean skipLocked;
      IndexCleanup indexCleanup = IndexCleanup.OFF;
      boolean processToast;
      boolean truncate;
      Integer parallel;
    }

    final ImmutableList<TableOrIndexName> tables;
    final ImmutableMap<TableOrIndexName, ImmutableList<TableOrIndexName>> columns;
    final boolean full;
    final boolean freeze;
    final boolean verbose;
    final boolean analyze;
    final boolean disablePageSkipping;
    final boolean skipLocked;
    final IndexCleanup indexCleanup;
    final boolean processToast;
    final boolean truncate;
    final int parallel;

    private ParsedVacuumStatement(Builder builder) {
      this.tables = builder.tables.build();
      this.columns = builder.columns.build();
      this.full = builder.full;
      this.freeze = builder.freeze;
      this.analyze = builder.analyze;
      this.verbose = builder.verbose;
      this.disablePageSkipping = builder.disablePageSkipping;
      this.skipLocked = builder.skipLocked;
      this.indexCleanup = builder.indexCleanup;
      this.processToast = builder.processToast;
      this.truncate = builder.truncate;
      this.parallel =
          builder.parallel == null
              ? 0
              : checkArgument(
                  builder.parallel, builder.parallel > 0, "parallel must be greater than 0");
    }
  }

  private final ParsedVacuumStatement parsedVacuumStatement;

  public VacuumStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(connectionHandler, options, name, parsedStatement, originalStatement);
    this.parsedVacuumStatement = parse(originalStatement.getSql());
  }

  @Override
  public String getCommandTag() {
    return "VACUUM";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.DDL;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
    setFutureStatementResult(backendConnection.execute(this));
  }

  public ImmutableList<TableOrIndexName> getTables() {
    return parsedVacuumStatement.tables;
  }

  public ImmutableList<TableOrIndexName> getColumn(TableOrIndexName table) {
    return parsedVacuumStatement.columns.get(table);
  }

  @Override
  public Future<DescribePortalMetadata> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that this TRUNCATE statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement bind(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // TRUNCATE does not support binding any parameters, so we just return the same statement.
    return this;
  }

  static ParsedVacuumStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    SimpleParser parser = new SimpleParser(sql);
    ParsedVacuumStatement.Builder builder = new Builder();
    if (!parser.eatKeyword("vacuum")) {
      throw PGExceptionFactory.newPGException("not a valid VACUUM statement: " + sql);
    }
    if (parser.eatToken("(")) {
      List<String> options = parser.parseExpressionList();
      if (!parser.eatToken(")")) {
        throw PGExceptionFactory.newPGException(
            "missing closing parentheses for VACUUM options list");
      }
      for (String option : options) {
        SimpleParser optionParser = new SimpleParser(option);
        if (optionParser.eatKeyword("full")) {
          builder.full = readOptionalBooleanValue("full", optionParser, true);
        } else if (optionParser.eatKeyword("freeze")) {
          builder.freeze = readOptionalBooleanValue("freeze", optionParser, true);
        } else if (optionParser.eatKeyword("verbose")) {
          builder.verbose = readOptionalBooleanValue("verbose", optionParser, true);
        } else if (optionParser.eatKeyword("analyze")) {
          builder.analyze = readOptionalBooleanValue("analyze", optionParser, true);
        } else if (optionParser.eatKeyword("disable_page_skipping")) {
          builder.disablePageSkipping =
              readOptionalBooleanValue("disable_page_skipping", optionParser, true);
        } else if (optionParser.eatKeyword("skip_locked")) {
          builder.skipLocked = readOptionalBooleanValue("skip_locked", optionParser, true);
        } else if (optionParser.eatKeyword("index_cleanup")) {
          if (optionParser.eatKeyword("auto")) {
            builder.indexCleanup = IndexCleanup.AUTO;
          } else {
            boolean value = readOptionalBooleanValue("index_cleanup", optionParser, true);
            builder.indexCleanup = value ? IndexCleanup.ON : IndexCleanup.OFF;
          }
        } else if (optionParser.eatKeyword("process_toast")) {
          builder.processToast = readOptionalBooleanValue("process_toast", optionParser, true);
        } else if (optionParser.eatKeyword("truncate")) {
          builder.truncate = readOptionalBooleanValue("truncate", optionParser, true);
        } else if (optionParser.eatKeyword("parallel")) {
          String value = optionParser.parseExpression();
          builder.parallel = optionParser.parseInt(value);
        } else {
          throw PGExceptionFactory.newPGException("Unknown option: " + option);
        }
      }
    } else {
      builder.full = parser.eatKeyword("full");
      builder.freeze = parser.eatKeyword("freeze");
      builder.verbose = parser.eatKeyword("verbose");
      builder.analyze = parser.eatKeyword("analyze");
    }

    while (parser.getPos() < parser.getSql().length()) {
      TableOrIndexName table = parser.readTableOrIndexName();
      if (table == null) {
        throw PGExceptionFactory.newPGException("Invalid table name");
      }
      builder.tables.add(table);
      List<TableOrIndexName> columns = parser.readColumnListInParentheses(table.toString(), false);
      if (columns != null) {
        builder.columns.put(table, ImmutableList.copyOf(columns));
      }
      if (!parser.eatToken(",")) {
        break;
      }
    }
    parser.throwIfHasMoreTokens();
    return new ParsedVacuumStatement(builder);
  }

  static boolean readOptionalBooleanValue(String name, SimpleParser parser, boolean defaultValue) {
    parser.skipWhitespaces();
    if (parser.getPos() >= parser.getSql().length()) {
      return defaultValue;
    }
    try {
      return BooleanParser.toBoolean(parser.getSql().substring(parser.getPos()));
    } catch (PGException ignore) {
      throw PGExceptionFactory.newPGException(name + " requires a Boolean value");
    }
  }
}
