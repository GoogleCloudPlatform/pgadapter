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

import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.SQLMetadata;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.cloud.spanner.pgadapter.utils.Converter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Intermediate representation for prepared statements (i.e.: statements before they become
 * portals)
 */
public class IntermediatePreparedStatement extends IntermediateStatement {

  private static final Charset UTF8 = StandardCharsets.UTF_8;
  protected final int parameterCount;
  protected List<Integer> parameterDataTypes;


  public IntermediatePreparedStatement(String sql, Connection connection) throws SQLException {
    super();
    SQLMetadata parsedSQL = Converter.toJDBCParams(sql);
    this.parameterCount = parsedSQL.getParameterCount();
    this.sql = parsedSQL.getSqlString();
    this.command = parseCommand(sql);
    this.connection = connection;
    this.statement = this.connection.prepareStatement(this.sql);
    this.parameterDataTypes = null;
  }

  IntermediatePreparedStatement(
      PreparedStatement statement, String sql, int totalParameters, Connection connection) {
    super();
    this.sql = sql;
    this.command = parseCommand(sql);
    this.parameterCount = totalParameters;
    this.statement = statement;
    this.connection = connection;
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
  private int parseType(byte[][] parameters, int index) {
    if (this.parameterDataTypes.size() > index) {
      return this.parameterDataTypes.get(index);
    } else {
      return Converter.guessPGDataType(new String(parameters[index], UTF8));
    }
  }

  public List<Integer> getParameterDataTypes() {
    return this.parameterDataTypes;
  }

  public void setParameterDataTypes(List<Integer> parameterDataTypes) {
    this.parameterDataTypes = parameterDataTypes;
  }

  public int getParameterCount() {
    return this.parameterCount;
  }

  @Override
  public void execute() {
    this.executed = true;
    try {
      ((PreparedStatement) this.statement).execute();
      this.executeHelper();
    } catch (SQLException e) {
      handleExecutionException(e);
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
   * @throws SQLException If the binding fails.
   */
  public IntermediatePortalStatement bind(
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) throws SQLException {
    IntermediatePortalStatement portal = new IntermediatePortalStatement(
        this.connection.prepareStatement(this.sql),
        this.sql,
        this.parameterCount,
        this.connection
    );
    portal.setParameterFormatCodes(parameterFormatCodes);
    portal.setResultFormatCodes(resultFormatCodes);
    for (int index = 0; index < parameters.length; index++) {
      short formatCode = portal.getParameterFormatCode(index);
      int type = this.parseType(parameters, index);
      if (formatCode == 0) {
        ((PreparedStatement) portal.statement).setObject(
            index + 1,
            Parser.create(
                parameters[index], type).getItem());
      } else {
        throw new IllegalStateException("Unimplemented");
      }
    }
    return portal;
  }

  @Override
  public DescribeMetadata describe() throws Exception {
    /* Currently, Spanner does not support description of prepared statements which may return
        result sets (that is to say, SELECT statements). We could return here the describe only for
        non-SELECT statements, but due to the difficulty in determining the statement type in proxy-
        side, as well as the unlikelihood of this method being used in production, we just return
        an exception. If at any point we are able to determine statement types, we can simply return
        "new DescribeStatementMetadata(
            Converter.getParams(
                this.parameterDataTypes,
                this.totalParameters)
            );"
        for non-SELECTs.
    */
    throw new IllegalStateException(
        "Describe statements for non-bound statements are not allowed in spanner.");
  }

}
