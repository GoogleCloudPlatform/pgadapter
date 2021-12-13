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

import com.google.cloud.spanner.pgadapter.utils.MutationBuilder;
import com.google.cloud.spanner.pgadapter.parsers.copy.Copy;
import com.google.spanner.v1.TypeCode;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.json.JSONException;

public class CopyStatement extends IntermediateStatement {

  protected Map<String, TypeCode> tableColumns;

  protected String tableName;
  protected List<String> copyColumnNames;
  protected String formatType;
  protected String nullString;
  protected char delimiterChar;
  protected char quoteChar;
  protected char escapeChar;
  protected boolean hasHeader;
  protected int formatCode;

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

  @Override
  public void execute() {
    this.executed = true;
    try {
      parseCopy();
      setMutationBuilder(
          new MutationBuilder(this.tableName, this.tableColumns, getParserFormat(), hasHeader()));
      this.updateResultCount(); // Gets update count, set hasMoreData false and statement result null
    } catch (Exception e) {
      SQLException se = new SQLException(e.toString());
      handleExecutionException(se);
    }
  }

  private void parseCopy() throws Exception {
    try {
      parse(sql);
      //queryInformationSchema();
    } catch (Exception e) {
      throw new SQLException("Invalid COPY statement syntax.");
    }
  }
}
