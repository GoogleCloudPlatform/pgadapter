// Copyright 2020 Google LLC
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

import com.google.cloud.spanner.ReadContext.QueryAnalyzeMode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribeStatementMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.postgresql.core.Oid;

/**
 * Intermediate representation for prepared statements (i.e.: statements before they become portals)
 */
public class IntermediatePreparedStatement extends IntermediateStatement {

  protected int[] parameterDataTypes;
  protected Statement statement;

  public IntermediatePreparedStatement(
      OptionsMetadata options, ParsedStatement parsedStatement, Connection connection) {
    super(options, parsedStatement, connection);
    this.parameterDataTypes = null;
  }

  /**
   * Given a set of parameters in byte format, return the designated type if stored by the user,
   * otherwise guess that type.
   *
   * @param parameters Array of all parameters in byte format.
   * @param index Index of the desired item.
   * @return The type of the item specified.
   */
  private int parseType(byte[][] parameters, int index) throws IllegalArgumentException {
    if (this.parameterDataTypes.length > index) {
      return this.parameterDataTypes[index];
    } else {
      return Oid.UNSPECIFIED;
    }
  }

  public int[] getParameterDataTypes() {
    return this.parameterDataTypes;
  }

  public void setParameterDataTypes(int[] parameterDataTypes) {
    this.parameterDataTypes = parameterDataTypes;
  }

  @Override
  public void execute() {
    this.executed = true;
    // If the portal has already been described, the statement has already been executed, and we
    // don't need to do that once more.
    if (getStatementResult(0) == null) {
      try {
        StatementResult result = connection.execute(this.statement);
        this.updateResultCount(0, result);
      } catch (SpannerException e) {
        handleExecutionException(0, e);
      }
    }
  }

  /**
   * Bind this statement (that is to say, transform it into a portal by giving it the data items to
   * complete the statement.
   *
   * @param parameters The array of parameters to be bound in byte format.
   * @param parameterFormatCodes A list of the format of each parameter.
   * @param resultFormatCodes A list of the desired format of each result.
   * @return An Intermediate Portal Statement (or rather a bound version of this statement)
   */
  public IntermediatePortalStatement bind(
      byte[][] parameters, List<Short> parameterFormatCodes, List<Short> resultFormatCodes) {
    IntermediatePortalStatement portal =
        new IntermediatePortalStatement(this.options, this.parsedStatement, this.connection);
    portal.setParameterFormatCodes(parameterFormatCodes);
    portal.setResultFormatCodes(resultFormatCodes);
    Statement.Builder builder = Statement.newBuilder(this.parsedStatement.getSqlWithoutComments());
    for (int index = 0; index < parameters.length; index++) {
      short formatCode = portal.getParameterFormatCode(index);
      int type = this.parseType(parameters, index);
      Parser<?> parser = Parser.create(parameters[index], type, FormatCode.of(formatCode));
      parser.bind(builder, "p" + (index + 1));
    }
    this.statement = builder.build();
    portal.setBoundStatement(statement);

    return portal;
  }

  @Override
  public DescribeMetadata describe() {
    Set<String> parameters =
        ImmutableSortedSet.<String>orderedBy(Comparator.comparing(o -> o.substring(1)))
            .addAll(PARSER.getQueryParameters(this.parsedStatement.getSqlWithoutComments()))
            .build();
    int[] paramTypes;
    if (parameters.isEmpty()) {
      paramTypes = new int[0];
    } else if (parameters.size() == this.parameterDataTypes.length
        && Arrays.stream(this.parameterDataTypes).noneMatch(p -> p == 0)) {
      // Note: We are only asking the backend to parse the types if there is at least one parameter
      // with unspecified type. Otherwise, we will rely on the types given in PARSE.
      paramTypes = this.parameterDataTypes;
    } else {
      // Transform the statement into a select statement that selects the parameters, and then
      // extract the types from the result set metadata.
      Statement selectParamsStatement = transformToSelectParams(parameters);
      if (selectParamsStatement == null) {
        // The transformation failed. Just rely on the types given in the PARSE message. If the
        // transformation failed because the statement was malformed, the backend will catch that at
        // a later stage.
        paramTypes = getParameterTypes();
      } else {
        try (ResultSet paramsResultSet =
            connection.analyzeQuery(selectParamsStatement, QueryAnalyzeMode.PLAN)) {
          paramTypes = getParameterTypes(paramsResultSet);
        } catch (SpannerException exception) {
          // Ignore this and rely on the types given in PARSE.
          paramTypes = getParameterTypes();
        }
      }
    }
    if (this.parsedStatement.isQuery()) {
      Statement statement = Statement.of(this.parsedStatement.getSqlWithoutComments());
      try (ResultSet columnsResultSet = connection.analyzeQuery(statement, QueryAnalyzeMode.PLAN)) {
        return new DescribeStatementMetadata(paramTypes, columnsResultSet);
      }
    }
    return new DescribeStatementMetadata(paramTypes, null);
  }

  @VisibleForTesting
  Statement transformToSelectParams(Set<String> parameters) {
    switch (this.parsedStatement.getType()) {
      case QUERY:
        return transformSelectToSelectParams(parameters);
      case UPDATE:
        return transformDmlToSelectParams(parameters);
      case CLIENT_SIDE:
      case DDL:
      case UNKNOWN:
      default:
        return Statement.of(this.parsedStatement.getSqlWithoutComments());
    }
  }

  private Statement transformSelectToSelectParams(Set<String> parameters) {
    return Statement.of(
        String.format(
            "select %s from (%s) p",
            String.join(", ", parameters), this.parsedStatement.getSqlWithoutComments()));
  }

  private Statement transformDmlToSelectParams(Set<String> parameters) {
    switch (getCommand(0)) {
      case "INSERT":
        return transformInsertToSelectParams(
            this.parsedStatement.getSqlWithoutComments(), parameters);
      case "UPDATE":
        return transformUpdateToSelectParams(
            this.parsedStatement.getSqlWithoutComments(), parameters);
      case "DELETE":
        return transformDeleteToSelectParams(
            this.parsedStatement.getSqlWithoutComments(), parameters);
      default:
        return null;
    }
  }

  @VisibleForTesting
  static @Nullable Statement transformInsertToSelectParams(String sql, Set<String> parameters) {
    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eat("insert")) {
      return null;
    }
    parser.eat("into");
    String table = parser.readIdentifier();
    if (table == null) {
      return null;
    }
    parser.skipWhitespaces();
    int potentialSelectStart = parser.getPos();
    if (parser.eat("select")) {
      // This is an `insert into <table> [(...)] select ...` statement. Then we can just use the
      // select statement as the result.
      return Statement.of(
          String.format(
              "select %s from (%s) p",
              String.join(", ", parameters), parser.getSql().substring(potentialSelectStart)));
    } else if (!parser.eat("(")) {
      return null;
    }
    List<String> columnsList = parser.parseExpressionListUntil();
    if (!parser.eat(")")) {
      return null;
    }
    // We currently only support `insert into <table> (col1, col2, ...) values (val1, val2, ...)`
    // statements with a columns list. The other option would be to query the information schema to
    // get the columns of the table, but that would happen in a separate transaction, which would
    // make the entire operation non-atomic.
    if (columnsList == null || columnsList.isEmpty()) {
      return null;
    }
    if (!parser.eat("values")) {
      return null;
    }
    List<List<String>> rows = new ArrayList<>();
    while (parser.eat("(")) {
      List<String> row = parser.parseExpressionListUntil();
      if (row == null || row.isEmpty() || !parser.eat(")") || row.size() != columnsList.size()) {
        return null;
      }
      rows.add(row);
      if (!parser.eat(",")) {
        break;
      }
    }
    if (rows.isEmpty()) {
      return null;
    }
    StringBuilder select = new StringBuilder("select ");
    select.append(String.join(", ", parameters)).append(" from (select ");

    int columnIndex = 0;
    int colCount = rows.size() * columnsList.size();
    for (List<String> row : rows) {
      for (int index = 0; index < row.size(); index++) {
        select.append(columnsList.get(index)).append("=").append(row.get(index));
        columnIndex++;
        if (columnIndex < colCount) {
          select.append(", ");
        }
      }
    }
    select.append(" from ").append(table).append(") p");

    return Statement.of(select.toString());
  }

  @VisibleForTesting
  static Statement transformUpdateToSelectParams(String sql, Set<String> parameters) {
    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eat("update")) {
      return null;
    }
    parser.eat("only");
    String table = parser.readIdentifier();
    if (table == null) {
      return null;
    }
    if (!parser.eat("set")) {
      return null;
    }
    List<String> assignmentsList = parser.parseExpressionListUntil("where");
    if (assignmentsList == null || assignmentsList.isEmpty()) {
      return null;
    }
    int whereStart = parser.getPos();
    if (!parser.eat("where")) {
      whereStart = -1;
    }

    StringBuilder select = new StringBuilder("select ");
    select
        .append(String.join(", ", parameters))
        .append(" from (select ")
        .append(String.join(", ", assignmentsList))
        .append(" from ")
        .append(table);
    if (whereStart > -1) {
      select.append(" ").append(sql.substring(whereStart));
    }
    select.append(") p");

    return Statement.of(select.toString());
  }

  @VisibleForTesting
  static Statement transformDeleteToSelectParams(String sql, Set<String> parameters) {
    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eat("delete")) {
      return null;
    }
    parser.eat("from");
    String table = parser.readIdentifier();
    if (table == null) {
      return null;
    }
    parser.skipWhitespaces();
    int whereStart = parser.getPos();
    if (!parser.eat("where")) {
      // Deletes must have a where clause, otherwise there cannot be any parameters.
      return null;
    }

    StringBuilder select = new StringBuilder("select ");
    select.append(String.join(", ", parameters)).append(" from (select 1 from ").append(table);
    if (whereStart > -1) {
      select.append(" ").append(sql.substring(whereStart));
    }
    select.append(") p");

    return Statement.of(select.toString());
  }

  /**
   * Returns the parameter types in the SQL string of this statement. The current implementation
   * always returns any parameters that may have been specified in the PARSE message, and
   * OID.Unspecified for all other parameters.
   */
  private int[] getParameterTypes(ResultSet paramsResultSet) {
    int[] paramTypes = Arrays.copyOf(this.parameterDataTypes, paramsResultSet.getColumnCount());
    for (int i = 0; i < paramsResultSet.getColumnCount(); i++) {
      if (paramTypes[i] == 0) {
        paramTypes[i] = Parser.toOid(paramsResultSet.getColumnType(i));
      }
    }
    return paramTypes;
  }

  /**
   * Returns the parameter types in the SQL string of this statement based only on the types given
   * during the PARSE phase + OID.Unspecified for all other parameters.
   */
  private int[] getParameterTypes() {
    Set<String> parameters =
        PARSER.getQueryParameters(this.parsedStatement.getSqlWithoutComments());
    if (this.parameterDataTypes == null) {
      return new int[parameters.size()];
    }
    return Arrays.copyOf(this.parameterDataTypes, parameters.size());
  }
}
