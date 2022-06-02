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
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ReadContext.QueryAnalyzeMode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribeStatementMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.postgresql.core.Oid;

/**
 * Intermediate representation for prepared statements (i.e.: statements before they become portals)
 */
@InternalApi
public class IntermediatePreparedStatement extends IntermediateStatement {
  private static final Logger logger =
      Logger.getLogger(IntermediatePreparedStatement.class.getName());

  private final String name;
  protected int[] parameterDataTypes;
  protected Statement statement;

  public IntermediatePreparedStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement) {
    super(connectionHandler, options, parsedStatement);
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
  public void execute() {
    // TODO(230579451): Refactor to use ClientSideStatement information.
    if (connectionHandler.getStatus() == ConnectionStatus.TRANSACTION_ABORTED) {
      handleTransactionAborted();
      return;
    }

    // If the portal has already been described, the statement has already been executed, and we
    // don't need to do that once more.
    if (getStatementResult(0) == null && futureStatementResult == null) {
      this.executedCount++;
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
      } catch (SpannerException exception) {
        handleExecutionExceptionAndTransactionStatus(0, exception);
      }
    }
  }

  private void handleTransactionAborted() {
    // TODO(230579451): Refactor to use ClientSideStatement information.
    String command = getCommand(0);
    if ("COMMIT".equals(command) || "ROLLBACK".equals(command)) {
      connectionHandler.setStatus(ConnectionStatus.IDLE);
      // COMMIT rollbacks aborted transaction
      commandTags.set(0, "ROLLBACK");
      if (connection.isInTransaction()) {
        connection.rollback();
      }
    } else {
      handleExecutionException(
          executedCount,
          SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT, TRANSACTION_ABORTED_ERROR));
      executedCount++;
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
            this.connectionHandler, this.options, name, this.parsedStatement);
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

    ResultSet columnsResultSet = null;
    try {
      if (this.parsedStatement.isQuery()) {
        Statement statement = Statement.of(this.parsedStatement.getSqlWithoutComments());
        columnsResultSet = connection.analyzeQuery(statement, QueryAnalyzeMode.PLAN);
      }

      boolean describeFailed = false;
      if (parameters.isEmpty()) {
        ensureParameterLength(0);
      } else if (parameters.size() != this.parameterDataTypes.length
          || Arrays.stream(this.parameterDataTypes).anyMatch(p -> p == 0)) {
        // Note: We are only asking the backend to parse the types if there is at least one
        // parameter with unspecified type. Otherwise, we will rely on the types given in PARSE.

        // Transform the statement into a select statement that selects the parameters, and then
        // extract the types from the result set metadata.
        Statement selectParamsStatement = transformToSelectParams(parameters);
        if (selectParamsStatement == null) {
          // The transformation failed. Just rely on the types given in the PARSE message. If the
          // transformation failed because the statement was malformed, the backend will catch that
          // at a later stage.
          describeFailed = true;
          ensureParameterLength(parameters.size());
        } else {
          try (ResultSet paramsResultSet =
              connection.analyzeQuery(selectParamsStatement, QueryAnalyzeMode.PLAN)) {
            extractParameterTypes(paramsResultSet);
          } catch (SpannerException exception) {
            // Ignore here and rely on the types given in PARSE.
            describeFailed = true;
            ensureParameterLength(parameters.size());
          }
        }
      }
      if (columnsResultSet != null) {
        return new DescribeStatementMetadata(this.parameterDataTypes, columnsResultSet);
      }

      if (this.parsedStatement.isUpdate()
          && (describeFailed || !Strings.isNullOrEmpty(this.name))) {
        // Let the backend analyze the statement if it is a named prepared statement or if the query
        // that was used to determine the parameter types failed, so we can return a reasonable
        // error message if the statement is invalid. If it is the unnamed statement or getting the
        // param types succeeded, we will let the following EXECUTE message handle that, instead of
        // sending the statement twice to the backend.
        connection.analyzeUpdate(
            Statement.of(this.parsedStatement.getSqlWithoutComments()), QueryAnalyzeMode.PLAN);
      }
      return new DescribeStatementMetadata(this.parameterDataTypes, null);
    } finally {
      if (columnsResultSet != null) {
        columnsResultSet.close();
      }
    }
  }

  /**
   * Transforms a query or DML statement into a SELECT statement that selects the parameters in the
   * statements. Examples:
   *
   * <ul>
   *   <li><code>select * from foo where id=$1</code> is transformed to <code>
   *       select $1 from (select * from foo where id=$1) p</code>
   *   <li><code>insert into foo (id, value) values ($1, $2)</code> is transformed to <code>
   *       select $1, $2 from (select id=$1, value=$2 from foo) p</code>
   * </ul>
   */
  @VisibleForTesting
  Statement transformToSelectParams(Set<String> parameters) {
    switch (this.parsedStatement.getType()) {
      case QUERY:
        return transformSelectToSelectParams(
            this.parsedStatement.getSqlWithoutComments(), parameters);
      case UPDATE:
        return transformDmlToSelectParams(parameters);
      case CLIENT_SIDE:
      case DDL:
      case UNKNOWN:
      default:
        return Statement.of(this.parsedStatement.getSqlWithoutComments());
    }
  }

  /**
   * Transforms a query into one that selects the parameters in the query.
   *
   * <p>Example: <code>select id, value from foo where value like $1</code> is transformed to <code>
   * select $1, $2 from (select id, value from foo where value like $1) p</code>
   */
  private static Statement transformSelectToSelectParams(String sql, Set<String> parameters) {
    return Statement.of(String.format("select %s from (%s) p", String.join(", ", parameters), sql));
  }

  /**
   * Transforms a DML statement into a SELECT statement that selects the parameters in the DML
   * statement.
   */
  private Statement transformDmlToSelectParams(Set<String> parameters) {
    switch (getCommand(0)) {
      case "INSERT":
        return transformInsertToSelectParams(
            this.connection, this.parsedStatement.getSqlWithoutComments(), parameters);
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

  /**
   * Transforms an INSERT statement into a SELECT statement that selects the parameters in the
   * insert statement. The way this is done depends on whether the INSERT statement uses a VALUES
   * clause or a SELECT statement. If the INSERT statement uses a SELECT clause, the same strategy
   * is used as for normal SELECT statements. For INSERT statements with a VALUES clause, a SELECT
   * statement is created that selects a comparison between the column where a value is inserted and
   * the expression that is used to insert a value in the column.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li><code>insert into foo (id, value) values ($1, $2)</code> is transformed to <code>
   *       select $1, $2 from (select id=$1, value=$2 from foo) p</code>
   *   <li><code>
   *       insert into bar (id, value, created_at) values (1, $1 + sqrt($2), current_timestamp())
   *       </code> is transformed to <code>
   *       select $1, $2 from (select value=$1 + sqrt($2) from bar) p</code>
   *   <li><code>insert into foo values ($1, $2)</code> is transformed to <code>
   *       select $1, $2 from (select id=$1, value=$2 from foo) p</code>
   * </ul>
   */
  @VisibleForTesting
  static @Nullable Statement transformInsertToSelectParams(
      Connection connection, String sql, Set<String> parameters) {
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

    List<String> columnsList = null;
    if (parser.eat("(")) {
      columnsList = parser.parseExpressionList();
      if (!parser.eat(")")) {
        return null;
      }
    }

    parser.skipWhitespaces();
    int potentialSelectStart = parser.getPos();
    if (parser.eat("select")) {
      // This is an `insert into <table> [(...)] select ...` statement. Then we can just use the
      // select statement as the result.
      return transformSelectToSelectParams(
          parser.getSql().substring(potentialSelectStart), parameters);
    } else if (!parser.eat("values")) {
      return null;
    }

    if (columnsList == null || columnsList.isEmpty()) {
      columnsList = getAllColumns(connection, table);
    }
    List<List<String>> rows = new ArrayList<>();
    while (parser.eat("(")) {
      List<String> row = parser.parseExpressionList();
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

  /**
   * Returns a list of all columns in the given table. This is used to transform insert statements
   * without a column list. The query that is used does not use the INFORMATION_SCHEMA, but queries
   * the table directly, so it can use the same transaction as the actual insert statement.
   */
  static List<String> getAllColumns(Connection connection, String table) {
    try (ResultSet resultSet =
        connection.analyzeQuery(
            Statement.of("SELECT * FROM \"" + table + "\" LIMIT 1"), QueryAnalyzeMode.PLAN)) {
      return resultSet.getType().getStructFields().stream()
          .map(StructField::getName)
          .collect(Collectors.toList());
    }
  }

  /**
   * Transforms an UPDATE statement into a SELECT statement that selects the parameters in the
   * update statement. This is done by creating a SELECT statement that selects the assignment
   * expressions in the UPDATE statement, followed by the WHERE clause of the UPDATE statement.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li><code>update foo set value=$1 where id=$2</code> is transformed to <code>
   *       select $1, $2 from (select value=$1 from foo where id=$2) p</code>
   *   <li><code>update bar set value=$1+sqrt($2), updated_at=current_timestamp()</code> is
   *       transformed to <code>select $1, $2 from (select value=$1+sqrt($2) from foo) p</code>
   * </ul>
   */
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

  /**
   * Transforms a DELETE statement into a SELECT statement that selects the parameters of the DELETE
   * statement. This is done by creating a SELECT 1 FROM table_name WHERE ... statement from the
   * DELETE statement.
   *
   * <p>Example:
   *
   * <ul>
   *   <li><code>DELETE FROM foo WHERE id=$1</code> is transformed to <code>
   *       SELECT $1 FROM (SELECT 1 FROM foo WHERE id=$1) p</code>
   * </ul>
   */
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

    StringBuilder select =
        new StringBuilder("select ")
            .append(String.join(", ", parameters))
            .append(" from (select 1 from ")
            .append(table)
            .append(" ")
            .append(sql.substring(whereStart))
            .append(") p");

    return Statement.of(select.toString());
  }

  /**
   * Returns the parameter types in the SQL string of this statement. The current implementation
   * always returns any parameters that may have been specified in the PARSE message, and
   * OID.Unspecified for all other parameters.
   */
  private void extractParameterTypes(ResultSet paramsResultSet) {
    ensureParameterLength(paramsResultSet.getColumnCount());
    for (int i = 0; i < paramsResultSet.getColumnCount(); i++) {
      // Only override parameter types that were not specified by the frontend.
      if (this.parameterDataTypes[i] == 0) {
        this.parameterDataTypes[i] = Parser.toOid(paramsResultSet.getColumnType(i));
      }
    }
  }

  /**
   * Enlarges the size of the parameter types of this statement to match the given count. Existing
   * parameter types are preserved. New parameters are set to OID.Unspecified.
   */
  private void ensureParameterLength(int parameterCount) {
    if (this.parameterDataTypes == null) {
      this.parameterDataTypes = new int[parameterCount];
    } else if (this.parameterDataTypes.length != parameterCount) {
      this.parameterDataTypes = Arrays.copyOf(this.parameterDataTypes, parameterCount);
    }
  }
}
