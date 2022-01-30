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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.utils.PgJdbcCatalog;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Data type to store simple SQL statement with designated metadata. Allows manipulation of
 * statement, such as execution, termination, etc. Represented as an intermediate representation for
 * statements which does not belong directly to Postgres, Spanner, etc.
 */
public class IntermediateStatement {
  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  protected Statement statement;
  protected ResultType resultType;
  protected ResultSet statementResult;
  protected boolean hasMoreData;
  protected Exception exception;
  protected String sql;
  protected String sqlWithoutComments;
  protected String command;
  protected boolean executed;
  protected Connection connection;
  protected Integer updateCount;
  protected List<String> statements;

  private static final char STATEMENT_DELIMITER = ';';
  private static final char SINGLE_QUOTE = '\'';

  public IntermediateStatement(String sql, Connection connection) throws SQLException {
    this.sql = replaceKnownUnsupportedTables(sql);
    this.sqlWithoutComments = PARSER.removeCommentsAndTrim(sql);
    this.statements = parseStatements(sql);
    this.command = parseCommand(sqlWithoutComments);
    this.connection = connection;
    this.statement = connection.createStatement();
    this.resultType = determineResultType(sql);
  }

  protected IntermediateStatement(String sql) {
    this.resultType = determineResultType(sql);
  }

  protected static String replaceKnownUnsupportedTables(String sql) {
    // TODO: Only call this if the server is running in JDBC compatibility mode.
    return replaceJdbcMetaDataQueries(sql);
  }

  private static String replaceJdbcMetaDataQueries(String sql) {
    // TODO: Check regex to verify that it is a Jdbc metadata query before applying any
    // replacements.
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_EXPORTED_IMPORTED_KEYS_PREFIX)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_EXPORTED_IMPORTED_KEYS_42_0_PREFIX)) {
      String replacedSql = PgJdbcCatalog.PG_JDBC_EXPORTED_IMPORTED_KEYS_REPLACEMENT;
      int startIndex;
      if (sql.contains(" AND pkn.nspname = ")) {
        startIndex = sql.indexOf(" AND pkn.nspname = ");
      } else if (sql.contains(" AND fkn.nspname = ")) {
        startIndex = sql.indexOf(" AND fkn.nspname = ");
      } else if (sql.contains(" AND pkc.relname = ")) {
        startIndex = sql.indexOf(" AND pkc.relname = ");
      } else if (sql.contains(" AND fkc.relname = ")) {
        startIndex = sql.indexOf(" AND fkc.relname = ");
      } else {
        startIndex = sql.indexOf(" ORDER BY ");
      }
      replacedSql += sql.substring(startIndex);
      return replacedSql
          .replace(" AND pkn.nspname = ", " AND PARENT.TABLE_SCHEMA = ")
          .replace(" AND fkn.nspname = ", " AND CHILD.TABLE_SCHEMA = ")
          .replace(" AND pkc.relname = ", " AND PARENT.TABLE_NAME = ")
          .replace(" AND fkc.relname = ", " AND CHILD.TABLE_NAME = ")
          .replace(
              " ORDER BY fkn.nspname,fkc.relname,con.conname,pos.n",
              " ORDER BY CHILD.TABLE_CATALOG, CHILD.TABLE_SCHEMA, CHILD.TABLE_NAME, KEY_SEQ")
          .replace(
              " ORDER BY pkn.nspname,pkc.relname, con.conname,pos.n",
              " ORDER BY PARENT.TABLE_CATALOG, PARENT.TABLE_SCHEMA, PARENT.TABLE_NAME, KEY_SEQ");
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_SCHEMAS_PREFIX)) {
      String replacedSql = PgJdbcCatalog.PG_JDBC_GET_SCHEMAS_REPLACEMENT;
      int startIndex;
      if (sql.contains(" AND nspname LIKE ")) {
        startIndex = sql.indexOf(" AND nspname LIKE ");
      } else {
        startIndex = sql.indexOf(" ORDER BY ");
      }
      replacedSql += sql.substring(startIndex);
      return replacedSql
          .replace(" AND nspname LIKE ", " AND schema_name LIKE ")
          .replace(" ORDER BY TABLE_SCHEM", " ORDER BY schema_name");
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_FUNCTIONS_WITH_FUNC_TYPE_PREFIX)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_FUNCTIONS_WITHOUT_FUNC_TYPE_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_FUNCTIONS_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_FUNCTION_COLUMNS_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_FUNCTION_COLUMNS_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_PROCEDURES_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_PROCEDURES_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_UDTS_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_UDTS_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_MAX_NAME_LENGTH_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_MAX_NAME_LENGTH_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_SQL_KEYWORDS_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_SQL_KEYWORDS_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_PREFIX_42_3)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_REPLACEMENT_42_3;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_PREFIX_42_2_22)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_REPLACEMENT_42_3;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_SIMPLE_PREFIX_42_3)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_REPLACEMENT_42_3;
    }

    // Rewrite the 42.3.x PK query to the version in 42.2.x.
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_PK_QUERY_PREFIX_42_3)) {
      String replacedSql = PgJdbcCatalog.PG_JDBC_PK_QUERY_PREFIX;
      String whereClause = "";
      if (sql.contains(" AND n.nspname = ")) {
        int startIndex = sql.indexOf(" AND n.nspname = ");
        int endIndex = sql.indexOf(" AND ct.relname = ", startIndex);
        whereClause += sql.substring(startIndex, endIndex);
      }
      if (sql.contains(" AND ct.relname = ")) {
        int startIndex = sql.indexOf(" AND ct.relname = ");
        int endIndex = sql.indexOf(" AND i.indisprimary ", startIndex);
        whereClause += sql.substring(startIndex, endIndex);
      }
      sql = replacedSql + whereClause + " ORDER BY ct.relname, pk_name, key_seq";
    }

    return sql
        // Replace fixed query prefixes.
        .replace(PgJdbcCatalog.PG_JDBC_PK_QUERY_PREFIX, PgJdbcCatalog.PG_JDBC_PK_QUERY_REPLACEMENT)
        .replace(
            PgJdbcCatalog.PG_JDBC_BEST_ROW_IDENTIFIER_PREFIX,
            PgJdbcCatalog.PG_JDBC_PK_QUERY_REPLACEMENT)

        // Replace unsupported pg_catalog tables with fixed sub-selects.
        .replace(
            " JOIN pg_catalog.pg_description",
            String.format(" JOIN (%s)", PgJdbcCatalog.PG_DESCRIPTION))
        .replace(" pg_catalog.pg_type ", String.format(" (%s) ", PgJdbcCatalog.PG_TYPE))
        .replace(" pg_catalog.pg_am", String.format(" (%s)", PgJdbcCatalog.PG_AM))
        .replaceAll(
            "\\s+FROM\\s+pg_catalog\\.pg_settings",
            String.format(" FROM (%s) pg_settings", PgJdbcCatalog.PG_SETTINGS))

        // Add joins for tables that miss information.
        .replace(
            " JOIN pg_catalog.pg_attribute a",
            String.format(
                " JOIN pg_catalog.pg_attribute a INNER JOIN (%s) a_spanner ON a.attrelid=a_spanner.attrelid AND a.attname=a_spanner.attname",
                PgJdbcCatalog.PG_ATTR_TYPE))

        // Replace select list expressions for those that might miss information.
        .replace(",a.atttypid,", ",coalesce(a.atttypid, a_spanner.spanner_atttypid) as atttypid,")

        // Replace expressions known to be in JDBC metadata queries that are not supported.
        // Replace !~ with NOT <expr> ~
        .replace("AND n.nspname !~ '^pg_'", "AND NOT n.nspname ~ '^pg_'")

        // Replace where and join conditions known to be in JDBC metadata queries that need
        // replacement.
        .replace("AND ci.relam=am.oid", "AND coalesce(ci.relam, 1)=am.oid")
        .replace(
            " ON (a.atttypid = t.oid)",
            " ON (coalesce(a.atttypid, a_spanner.spanner_atttypid) = t.oid)")
        .replace(
            " OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)",
            " OR FALSE ")

        // Replace function calls for DDL definitions with null literals.
        .replaceAll("pg_catalog\\.pg_get_expr\\(.+?\\)", "null")
        .replaceAll("pg_catalog\\.pg_get_indexdef\\(.+?\\)", "null")

        // Replace unsupported casts.
        .replaceAll("'pg_class'::regclass", "0");
  }

  protected static ResultType determineResultType(String sql) {
    if (PARSER.isUpdateStatement(sql)) {
      return ResultType.UPDATE_COUNT;
    } else if (PARSER.isQuery(sql)) {
      return ResultType.RESULT_SET;
    } else {
      return ResultType.NO_RESULT;
    }
  }

  // Split statements by ';' delimiter, but ignore anything that is nested with '' or "".
  private List<String> splitStatements(String sql) {
    List<String> statements = new ArrayList<>();
    boolean quoteEsacpe = false;
    int index = 0;
    for (int i = 0; i < sql.length(); ++i) {
      if (sql.charAt(i) == SINGLE_QUOTE) {
        quoteEsacpe = !quoteEsacpe;
      }
      if (sql.charAt(i) == STATEMENT_DELIMITER && !quoteEsacpe) {
        String stmt = sql.substring(index, i + 1).trim();
        // Statements with only ';' character are empty and dropped.
        if (stmt.length() > 1) {
          statements.add(stmt);
        }
        index = i + 1;
      }
    }

    if (index < sql.length()) {
      statements.add(sql.substring(index, sql.length()).trim());
    }
    return statements;
  }

  protected List<String> parseStatements(String sql) {
    Preconditions.checkNotNull(sql);
    List<String> statements = splitStatements(sql);
    return statements;
  }

  /** Determines the (update) command that was received from the sql string. */
  protected static String parseCommand(String sql) {
    Preconditions.checkNotNull(sql);
    String[] tokens = sql.split("\\s+", 2);
    if (tokens.length > 0) {
      return tokens[0].toUpperCase();
    }
    return null;
  }

  /**
   * Whether this is a bound statement (i.e.: ready to execute)
   *
   * @return True if bound, false otherwise.
   */
  public boolean isBound() {
    return true;
  }

  /**
   * Cleanly close the statement. Does nothing if the statement has not been executed or has no
   * result.
   *
   * @throws Exception if closing fails server-side.
   */
  public void close() throws Exception {
    if (this.getStatementResult() != null) {
      this.getStatementResult().close();
    }
  }

  /** @return True if this is a select statement, false otherwise. */
  public boolean containsResultSet() {
    return this.resultType == ResultType.RESULT_SET;
  }

  /** @return True if this statement was executed, False otherwise. */
  public boolean isExecuted() {
    return executed;
  }

  /** @return The number of items that were modified by this execution. */
  public Integer getUpdateCount() {
    return this.updateCount;
  }

  /** @return True if at some point in execution, and exception was thrown. */
  public boolean hasException() {
    return this.exception != null;
  }

  /** @return True if only a subset of the available data has been returned. */
  public boolean isHasMoreData() {
    return this.hasMoreData;
  }

  public void setHasMoreData(boolean hasMoreData) {
    this.hasMoreData = hasMoreData;
  }

  public Statement getStatement() {
    return this.statement;
  }

  public List<String> getStatements() {
    return this.statements;
  }

  public ResultSet getStatementResult() {
    return this.statementResult;
  }

  public ResultType getResultType() {
    return this.resultType;
  }

  public String getSql() {
    return this.sql;
  }

  public Exception getException() {
    Exception e = this.exception;
    this.exception = null;
    return e;
  }

  /**
   * Processes the results from an execute/executeBatch execution, extracting metadata from that
   * execution (including results and update counts). An array of updateCounts is needed in the case
   * of updateBatchResultCount.
   *
   * @throws SQLException If an issue occurred in extracting result metadata.
   */
  protected void updateResultCount() throws SQLException {
    if (this.containsResultSet()) {
      this.statementResult = this.statement.getResultSet();
      this.hasMoreData = this.statementResult.next();
    } else {
      this.updateCount = this.statement.getUpdateCount();
      this.hasMoreData = false;
      this.statementResult = null;
    }
  }

  protected void updateBatchResultCount(int[] updateCounts) throws SQLException {
    this.updateCount = 0;
    for (int i = 0; i < updateCounts.length; ++i) {
      this.updateCount += updateCounts[i];
    }
    this.hasMoreData = false;
    this.statementResult = null;
  }

  /**
   * Clean up and save metadata when an exception occurs.
   *
   * @param e The exception to store.
   */
  protected void handleExecutionException(SQLException e) {
    this.exception = e;
    this.hasMoreData = false;
    this.statementResult = null;
    this.resultType = ResultType.NO_RESULT;
  }

  /** Execute the SQL statement, storing metadata. */
  public void execute() {
    this.executed = true;
    int[] updateCounts = null;
    try {
      if (statements.size() > 1) {
        for (String stmt : statements) {
          this.statement.addBatch(stmt);
        }
        updateCounts = this.statement.executeBatch();
        this.updateBatchResultCount(updateCounts);
      } else {
        this.statement.execute(this.sql);
        this.updateResultCount();
      }
    } catch (SQLException e) {
      if (statements.size() > 1) {
        SQLException exception = new SQLException(e.getMessage() + " \"" + this.sql + "\"", e);
        handleExecutionException(exception);
      } else {
        handleExecutionException(e);
      }
    }
  }

  /**
   * Moreso meant for inherited classes, allows one to call describe on a statement. Since raw
   * statements cannot be described, throw an error.
   */
  public DescribeMetadata describe() throws Exception {
    throw new IllegalStateException(
        "Cannot describe a simple statement " + "(only prepared statements and portals)");
  }

  /**
   * Moreso intended for inherited classes (prepared statements et al) which allow the setting of
   * result format codes. Here we dafault to string.
   */
  public short getResultFormatCode(int index) {
    return 0;
  }

  /** @return the extracted command (first word) from the SQL statement. */
  public String getCommand() {
    return this.command;
  }

  /* Used for testing purposes */
  public boolean isBatchedQuery() {
    return (statements.size() > 1);
  }

  public enum ResultType {
    UPDATE_COUNT,
    RESULT_SET,
    NO_RESULT
  }
}
