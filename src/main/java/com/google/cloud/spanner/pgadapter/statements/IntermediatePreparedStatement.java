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
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribeStatementMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.postgresql.core.Oid;

/**
 * Intermediate representation for prepared statements (i.e.: statements before they become portals)
 */
@InternalApi
public class IntermediatePreparedStatement extends IntermediateStatement {

  protected int[] parameterDataTypes;
  protected Statement statement;

  public IntermediatePreparedStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      ParsedStatement parsedStatement) {
    super(connectionHandler, options, parsedStatement);
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
        if (!connection.isInTransaction()
            // TODO(230579451): Refactor to use ClientSideStatement information.
            && this.parsedStatement.getType().equals(StatementType.CLIENT_SIDE)
            && (this.commands.get(0).equals("ROLLBACK") || this.commands.get(0).equals("COMMIT"))) {
          // TODO(230579929): Return warning that no transaction if connection status == IDLE.
          connectionHandler.setStatus(ConnectionStatus.IDLE);
        } else {
          StatementResult result = connection.execute(this.statement);
          this.updateResultCount(0, result);
          connectionHandler.setStatus(
              connection.isInTransaction() ? ConnectionStatus.TRANSACTION : ConnectionStatus.IDLE);
        }
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
        new IntermediatePortalStatement(this.connectionHandler, this.options, this.parsedStatement);
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
    if (this.parsedStatement.isQuery()) {
      Statement statement = Statement.of(this.parsedStatement.getSqlWithoutComments());
      try (ResultSet resultSet = connection.analyzeQuery(statement, QueryAnalyzeMode.PLAN)) {
        // TODO: Remove ResultSet.next() call once this is supported in the client library.
        // See https://github.com/googleapis/java-spanner/pull/1691
        resultSet.next();
        return new DescribeStatementMetadata(getParameterTypes(), resultSet);
      }
    }
    return new DescribeStatementMetadata(getParameterTypes(), null);
  }

  /**
   * Returns the parameter types in the SQL string of this statement. The current implementation
   * always returns any parameters that may have been specified in the PARSE message, and
   * OID.Unspecified for all other parameters.
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
