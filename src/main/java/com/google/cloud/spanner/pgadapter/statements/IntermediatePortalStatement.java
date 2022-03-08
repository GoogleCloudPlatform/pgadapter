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
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
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

  public IntermediatePortalStatement(String sql, Connection connection) {
    super(sql, connection);
    this.statement = Statement.of(sql);
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
      return 0;
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
  public DescribeMetadata describe() throws Exception {
    // TODO: Consider replacing this with an execute call, so we don't take two round-trips to the
    // backend just to first describe and then execute a query.
    try (ResultSet resultSet = connection.analyzeQuery(this.statement, QueryAnalyzeMode.PLAN)) {
      // TODO: Remove ResultSet.next() call once this is supported in the client library.
      // See https://github.com/googleapis/java-spanner/pull/1691
      resultSet.next();
      return new DescribePortalMetadata(resultSet);
    } catch (SpannerException e) {
      /* Generally this error will occur when a non-SELECT portal statement is described in Spanner,
        however, it could occur when a statement is incorrectly formatted. Though we could catch
        this early if we could parse the type of statement, it is a significant burden on the
        proxy. As such, we send the user a descriptive message to help them understand the issue
        in case they misuse the method.
      */
      logger.log(Level.SEVERE, e.toString());
      throw new IllegalStateException(
          "Something went wrong in Describing this statement."
              + "Note that non-SELECT result types in Spanner cannot be described.");
    }
  }
}
