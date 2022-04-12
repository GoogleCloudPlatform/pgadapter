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

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An intermediate representation of a portal statement (that is, a prepared statement which
 * contains all relevant information for execution.
 */
public class IntermediatePortalStatement extends IntermediatePreparedStatement {

  private static final Logger logger =
      Logger.getLogger(IntermediatePreparedStatement.class.getName());
  protected List<Short> parameterFormatCodes;
  protected List<Short> resultFormatCodes;

  public IntermediatePortalStatement(
      OptionsMetadata options, ParsedStatement parsedStatement, Connection connection) {
    super(options, parsedStatement, connection);
    this.statement = Statement.of(parsedStatement.getSqlWithoutComments());
    this.parameterFormatCodes = new ArrayList<>();
    this.resultFormatCodes = new ArrayList<>();
  }

  void setBoundStatement(Statement statement) {
    this.statement = statement;
  }

  public short getParameterFormatCode(int index) {
    if (this.parameterFormatCodes.size() == 0) {
      return 0;
    } else if (index >= this.parameterFormatCodes.size()) {
      return this.parameterFormatCodes.get(0);
    } else {
      return this.parameterFormatCodes.get(index);
    }
  }

  @Override
  public short getResultFormatCode(int index) {
    if (this.resultFormatCodes == null || this.resultFormatCodes.isEmpty()) {
      return super.getResultFormatCode(index);
    } else if (this.resultFormatCodes.size() == 1) {
      return this.resultFormatCodes.get(0);
    } else {
      return this.resultFormatCodes.get(index);
    }
  }

  public void setParameterFormatCodes(List<Short> parameterFormatCodes) {
    this.parameterFormatCodes = parameterFormatCodes;
  }

  public void setResultFormatCodes(List<Short> resultFormatCodes) {
    this.resultFormatCodes = resultFormatCodes;
  }

  @Override
  public DescribeMetadata describe() {
    try {
      // Pre-emptively execute the statement, even though it is only asked to be described. This is
      // a lot more efficient than taking two round trips to the server, and getting a
      // DescribePortal message without a following Execute message is extremely rare, as that would
      // only happen if the client is ill-behaved, or if the client crashes between the
      // DescribePortal and Execute.
      ResultSet statementResult = connection.executeQuery(this.statement);
      setStatementResult(0, statementResult);
      return new DescribePortalMetadata(statementResult);
    } catch (SpannerException e) {
      logger.log(Level.SEVERE, e, e::getMessage);
      throw e;
    }
  }
}
