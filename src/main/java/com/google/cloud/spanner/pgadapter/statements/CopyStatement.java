// Copyright 2021 Google LLC
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

import static com.google.cloud.spanner.pgadapter.parsers.copy.Copy.parse;

import com.google.cloud.spanner.pgadapter.parsers.copy.CopyTreeParser;
import com.google.cloud.spanner.pgadapter.utils.MutationBuilder;
import com.google.spanner.v1.TypeCode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;

public class CopyStatement extends IntermediateStatement {

  private static final String COLUMN_NAME = "column_name";
  private static final String SPANNER_TYPE = "spanner_type";
  private static final String CSV = "CSV";

  protected String tableName;
  protected List<String> copyColumnNames;
  protected String formatType;
  protected String nullString;
  protected char delimiterChar;
  protected char quoteChar;
  protected char escapeChar;
  protected boolean hasHeader;
  protected int formatCode;

  protected Map<String, TypeCode> tableColumns;
  protected MutationBuilder mutationBuilder;

  public CopyStatement(String sql, Connection connection) throws SQLException {
    super();
    this.sql = sql;
    this.command = parseCommand(sql);
    this.connection = connection;
    this.statement = connection.createStatement();
  }

  /** @return Mapping of table column names to column type. */
  public Map<String, TypeCode> getTableColumns() {
    return this.tableColumns;
  }

  protected void setTableColumns(Map tableColumns) {
    this.tableColumns = tableColumns;
  }

  /** @return CSVFormat for parsing copy data based on COPY statement options specified. */
  public CSVFormat getParserFormat() {
    CSVFormat format = CSVFormat.POSTGRESQL_TEXT;
    if (this.formatType != null && this.formatType.equalsIgnoreCase("CSV")) {
      format = CSVFormat.POSTGRESQL_CSV;
    }
    if (this.nullString != null && !this.nullString.isEmpty()) {
      format = format.withNullString(this.nullString);
    }
    if (this.delimiterChar != '\0') {
      format = format.withDelimiter(this.delimiterChar);
    }
    if (this.escapeChar != '\0') {
      format = format.withEscape(this.escapeChar);
    }
    if (this.quoteChar != '\0') {
      format = format.withQuote(this.quoteChar);
    }
    format = format.withHeader(this.tableColumns.keySet().toArray(new String[0]));
    return format;
  }

  public String getTableName() {
    return this.tableName;
  }

  protected void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /** @return List of column names specified in COPY statement, if provided. */
  public List<String> getCopyColumnNames() {
    return this.copyColumnNames;
  }

  protected void setCopyColumnNames(List<String> copyColumnNames) {
    this.copyColumnNames = copyColumnNames;
  }

  /** @return Format type specified in COPY statement, if provided. */
  public String getFormatType() {
    return this.formatType;
  }

  protected void setFormatType(String formatType) {
    this.formatType = formatType;
  }

  /** @return Null string specified in COPY statement, if provided. */
  public String getNullString() {
    return this.nullString;
  }

  protected void setNullString(String nullString) {
    this.nullString = nullString;
  }

  /** @return Delimiter character specified in COPY statement, if provided. */
  public char getDelimiterChar() {
    return this.delimiterChar;
  }

  protected void setDelimiterChar(char delimiterChar) {
    this.delimiterChar = delimiterChar;
  }

  /** @return Escape character specified in COPY statement, if provided. */
  public char getEscapeChar() {
    return this.escapeChar;
  }

  protected void setEscapeChar(char escapeChar) {
    this.escapeChar = escapeChar;
  }

  /** @return Quote character specified in COPY statement, if provided. */
  public char getQuoteChar() {
    return this.quoteChar;
  }

  protected void setQuoteChar(char quoteChar) {
    this.quoteChar = quoteChar;
  }

  /** @return True if copy data contains a header, false otherwise. */
  public boolean hasHeader() {
    return this.hasHeader;
  }

  protected void setHasHeader(boolean hasHeader) {
    this.hasHeader = hasHeader;
  }

  public int getFormatCode() {
    return this.formatCode;
  }

  protected void setFormatCode(int columnCount) {
    this.formatCode = columnCount;
  }

  public MutationBuilder getMutationBuilder() {
    return this.mutationBuilder;
  }

  protected void setMutationBuilder(MutationBuilder mutationBuilder) {
    this.mutationBuilder = mutationBuilder;
  }

  private void verifyCopyColumns() throws SQLException {
    if (this.copyColumnNames.size() > this.tableColumns.size()) {
      throw new SQLException("Number of copy columns provided exceed table column count");
    }
    LinkedHashMap<String, TypeCode> tempTableColumns = new LinkedHashMap<>();
    // Verify that every copy column given is a valid table column name
    for (String copyColumn : this.copyColumnNames) {
      if (!this.tableColumns.containsKey(copyColumn)) {
        throw new SQLException(
            "Column \"" + copyColumn + "\" of relation \"" + this.tableName + "\" does not exist");
      }
      // Update table column ordering with the copy column ordering
      tempTableColumns.put(copyColumn, tableColumns.get(copyColumn));
    }
    this.tableColumns = tempTableColumns;
  }

  private static TypeCode parseSpannerDataType(String columnType) {
    if (columnType.matches("(i?)STRING(?:\\((?:MAX|[0-9]+)\\))?")) {
      return TypeCode.STRING;
    } else if (columnType.matches("(i?)BYTES(?:\\((?:MAX|[0-9]+)\\))?")) {
      return TypeCode.BYTES;
    } else if (columnType.equalsIgnoreCase("INT64")) {
      return TypeCode.INT64;
    } else if (columnType.equalsIgnoreCase("FLOAT64")) {
      return TypeCode.FLOAT64;
    } else if (columnType.equalsIgnoreCase("BOOL")) {
      return TypeCode.BOOL;
    } else if (columnType.equalsIgnoreCase("TIMESTAMP")) {
      return TypeCode.TIMESTAMP;
    } else {
      throw new IllegalArgumentException(
          "Unrecognized or unsupported column data type: " + columnType);
    }
  }

  private void queryInformationSchema() throws SQLException {
    Map<String, TypeCode> tableColumns = new LinkedHashMap<>();
    ResultSet result =
        this.connection
            .createStatement()
            .executeQuery(
                "SELECT "
                    + COLUMN_NAME
                    + ", "
                    + SPANNER_TYPE
                    + " FROM information_schema.columns WHERE table_name = '"
                    + this.tableName
                    + "'");

    while (result.next()) {
      String columnName = result.getString(COLUMN_NAME);
      TypeCode type = parseSpannerDataType(result.getString(SPANNER_TYPE));
      tableColumns.put(columnName, type);
    }

    if (tableColumns.isEmpty()) {
      throw new SQLException("Table " + this.tableName + " is not found in information_schema");
    }

    setTableColumns(tableColumns);

    if (this.copyColumnNames != null) {
      verifyCopyColumns();
    }
  }

  @Override
  public void execute() {
    this.executed = true;
    try {
      parseCopyStatement();
      setMutationBuilder(
          new MutationBuilder(this.tableName, this.tableColumns, getParserFormat(), hasHeader()));
      this.updateResultCount(); // Gets update count, set hasMoreData false and statement result
    } catch (Exception e) {
      SQLException se = new SQLException(e.toString());
      handleExecutionException(se);
    }
  }

  private void parseCopyStatement() throws Exception {
    try {
      CopyTreeParser.CopyOptions options = new CopyTreeParser.CopyOptions();
      parse(sql, options);
      // Set options gathered from the Copy statement.
      tableName = options.getTableName();
      queryInformationSchema();
    } catch (Exception e) {
      throw new SQLException("Invalid COPY statement syntax: " + e.toString());
    }
  }
}
