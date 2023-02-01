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

package com.google.cloud.spanner.pgadapter.statements;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ForwardingResultSet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;

/** Wrapper class for query results that are handled directly in PGAdapter. */
@InternalApi
public class ClientSideResultSet extends ForwardingResultSet {
  public static ResultSet forRows(Type type, Iterable<Struct> rows) {
    return new ClientSideResultSet(ResultSets.forRows(type, rows), type);
  }

  private final Type type;

  private ClientSideResultSet(ResultSet delegate, Type type) {
    super(delegate);
    this.type = type;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public ResultSetStats getStats() {
    return ResultSetStats.getDefaultInstance();
  }

  @Override
  public ResultSetMetadata getMetadata() {
    return ResultSetMetadata.getDefaultInstance();
  }
}
