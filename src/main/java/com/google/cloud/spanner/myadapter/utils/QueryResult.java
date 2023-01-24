// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.myadapter.utils;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.connection.StatementResult;

/*
 * Implementation of {@link StatementResult} for statements that return zero or more rows(e.g. DQL).
 */
@InternalApi
public final class QueryResult implements StatementResult {
  private final ResultSet resultSet;

  public QueryResult(ResultSet resultSet) {
    this.resultSet = resultSet;
  }

  @Override
  public ResultType getResultType() {
    return ResultType.RESULT_SET;
  }

  @Override
  public ClientSideStatementType getClientSideStatementType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSet getResultSet() {
    return resultSet;
  }

  @Override
  public Long getUpdateCount() {
    throw new UnsupportedOperationException();
  }
}
