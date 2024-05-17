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
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ResultType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.DescribeResult;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.postgresql.core.Oid;

/**
 * Intermediate representation for prepared statements (i.e.: statements before they become portals)
 */
@InternalApi
public class IntermediatePreparedStatement extends IntermediateStatement {
  static final int[] NO_PARAMETER_TYPES = new int[0];

  private final String name;
  protected final int[] givenParameterDataTypes;
  protected Statement statement;
  private Future<DescribeResult> describeResult;

  public IntermediatePreparedStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      int[] givenParameterDataTypes,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(connectionHandler, options, parsedStatement, originalStatement);
    this.name = name;
    this.givenParameterDataTypes = givenParameterDataTypes;
    this.statement = originalStatement;
  }

  public int[] getGivenParameterDataTypes() {
    return this.givenParameterDataTypes;
  }

  /**
   * Returns the parameter type of the given parameter. This will only use the given parameter types
   * if the statement has not been described, and the described parameter types if the statement has
   * been described.
   *
   * @param index Index of the desired parameter.
   * @return The type of the parameter specified.
   */
  int getParameterDataType(int index) throws IllegalArgumentException {
    int[] parameterDataTypes = this.givenParameterDataTypes;
    if (this.described) {
      parameterDataTypes = describe().getParameters();
    }
    if (parameterDataTypes.length > index) {
      return parameterDataTypes[index];
    } else {
      return Oid.UNSPECIFIED;
    }
  }

  /**
   * Creates a portal from this statement.
   *
   * @param parameterFormatCodes A list of the format of each parameter.
   * @param resultFormatCodes A list of the desired format of each result.
   * @return An Intermediate Portal Statement (or rather a bound version of this statement)
   */
  public IntermediatePortalStatement createPortal(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    return new IntermediatePortalStatement(
        name, this, parameters, parameterFormatCodes, resultFormatCodes);
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    ListenableFuture<StatementResult> statementResultFuture =
        backendConnection.analyze(this.command, this.parsedStatement, this.statement);
    setFutureStatementResult(statementResultFuture);
    this.describeResult =
        Futures.transform(
            statementResultFuture,
            result -> {
              DescribeResult describeResult;
              if (result.getResultType() == ResultType.RESULT_SET) {
                describeResult =
                    new DescribeResult(this.givenParameterDataTypes, result.getResultSet());
                result.getResultSet().close();
              } else {
                describeResult = new DescribeResult(this.givenParameterDataTypes, null);
              }
              return describeResult;
            },
            MoreExecutors.directExecutor());
    this.described = true;
    return statementResultFuture;
  }

  @Override
  public DescribeResult describe() {
    if (this.describeResult == null) {
      // Just return a DescribeResult that contains whatever information we were given in the
      // PARSE message.
      return new DescribeResult(this.givenParameterDataTypes, null);
    }
    try {
      return this.describeResult.get();
    } catch (ExecutionException exception) {
      throw PGExceptionFactory.toPGException(exception.getCause());
    } catch (InterruptedException interruptedException) {
      throw PGExceptionFactory.newQueryCancelledException();
    }
  }

  /** Describe the parameters of this statement. */
  public void autoDescribeParameters(
      byte[][] parameterValues, BackendConnection backendConnection) {
    // Don't bother to auto-describe statements without any parameter values or with only
    // null-valued parameters.
    if (parameterValues == null
        || parameterValues.length == 0
        || hasOnlyNullValues(parameterValues)) {
      return;
    }
    if (parameterValues.length != this.givenParameterDataTypes.length
        || Arrays.stream(this.givenParameterDataTypes).anyMatch(p -> p == 0)) {
      // Note: We are only asking the backend to parse the types if there is at least one
      // parameter with unspecified type. Otherwise, we will rely on the types given in PARSE.
      // There is also no need to auto-describe if only null values are untyped.
      boolean mustAutoDescribe = false;
      for (int index = 0; index < parameterValues.length; index++) {
        if (parameterValues[index] != null && getParameterDataType(index) == Oid.UNSPECIFIED) {
          mustAutoDescribe = true;
          break;
        }
      }
      if (!mustAutoDescribe) {
        return;
      }

      // As this describe-request is an auto-describe request, we can safely try to look it up in a
      // cache.
      Future<DescribeResult> cachedDescribeResult =
          getConnectionHandler().getAutoDescribedStatement(this.originalStatement.getSql());
      if (cachedDescribeResult != null) {
        this.described = true;
        this.describeResult = cachedDescribeResult;
        return;
      }
      // No cached result found. Add a describe-statement message to the queue.
      describeAsync(backendConnection);
      getConnectionHandler()
          .registerAutoDescribedStatement(this.originalStatement.getSql(), this.describeResult);
    }
  }

  boolean hasOnlyNullValues(byte[][] parameterValues) {
    for (byte[] parameterValue : parameterValues) {
      if (parameterValue != null) {
        return false;
      }
    }
    return true;
  }
}
