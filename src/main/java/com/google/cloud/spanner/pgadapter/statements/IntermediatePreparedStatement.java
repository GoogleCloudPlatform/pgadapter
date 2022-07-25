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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ReadContext.QueryAnalyzeMode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribeStatementMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import java.util.List;
import java.util.stream.Collectors;
import org.postgresql.core.Oid;

/**
 * Intermediate representation for prepared statements (i.e.: statements before they become portals)
 */
@InternalApi
public class IntermediatePreparedStatement extends IntermediateStatement {
  private final String name;
  protected int[] parameterDataTypes;
  protected Statement statement;

  public IntermediatePreparedStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(connectionHandler, options, parsedStatement, originalStatement);
    this.name = name;
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
  public void executeAsync(BackendConnection backendConnection) {
    // If the portal has already been described, the statement has already been executed, and we
    // don't need to do that once more.
    if (futureStatementResult == null && getStatementResult() == null) {
      this.executed = true;
      setFutureStatementResult(backendConnection.execute(parsedStatement, statement));
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
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    IntermediatePortalStatement portal =
        new IntermediatePortalStatement(
            this.connectionHandler,
            this.options,
            name,
            this.parsedStatement,
            this.originalStatement);
    portal.setParameterFormatCodes(parameterFormatCodes);
    portal.setResultFormatCodes(resultFormatCodes);
    Statement.Builder builder = this.originalStatement.toBuilder();
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
  public DescribeMetadata<?> describe() {
    ResultSet planResultSet = null;
    if (this.parsedStatement.isQuery()) {
      Statement statement = Statement.of(this.parsedStatement.getSqlWithoutComments());
      planResultSet = connection.analyzeQuery(statement, QueryAnalyzeMode.PLAN);
    }
    return new DescribeStatementMetadata(planResultSet);
  }

  /**
   * Returns a list of all columns in the given table. This is used to transform insert statements
   * without a column list. The query that is used does not use the INFORMATION_SCHEMA, but queries
   * the table directly, so it can use the same transaction as the actual insert statement.
   */
  static List<String> getAllColumns(Connection connection, TableOrIndexName table) {
    try (ResultSet resultSet =
        connection.analyzeQuery(
            Statement.of("SELECT * FROM " + table + " LIMIT 1"), QueryAnalyzeMode.PLAN)) {
      return resultSet.getType().getStructFields().stream()
          .map(StructField::getName)
          .collect(Collectors.toList());
    }
  }
}
