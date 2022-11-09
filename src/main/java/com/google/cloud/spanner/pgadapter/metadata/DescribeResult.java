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

package com.google.cloud.spanner.pgadapter.metadata;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.protobuf.Struct;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import java.util.List;

@InternalApi
public class DescribeResult implements AutoCloseable {
  private static final int[] NO_PARAMS = new int[0];
  private final ResultSet resultSet;
  private final int[] parameters;

  public DescribeResult(ResultSet resultMetadata) {
    this.resultSet = resultMetadata;
    this.parameters = NO_PARAMS;
  }

  public DescribeResult(int[] givenParameterTypes, ResultSet resultMetadata) {
    this.resultSet = resultMetadata;
    this.parameters = extractParameters(givenParameterTypes, resultMetadata);
  }

  static int[] extractParameters(int[] givenParameterTypes, ResultSet resultSet) {
    if (!resultSet.getMetadata().hasUndeclaredParameters()) {
      return givenParameterTypes;
    }
    return extractParameterTypes(givenParameterTypes, resultSet.getMetadata().getUndeclaredParameters().getFieldsList());
  }

  static int[] extractParameterTypes(int[] givenParameterTypes, List<Field> parameters) {
    if (parameters.size() == givenParameterTypes.length) {
      return givenParameterTypes;
    }
    int[] result = new int[parameters.size()];
    for (int i = 0; i < parameters.size(); i++) {
      // Only override parameter types that were not specified by the frontend.
      if (givenParameterTypes[i] == 0) {
        result[i] = Parser.toOid(parameters.get(i).getType());
      }
    }
    return result;
  }

  public int[] getParameters() {
    return parameters;
  }

  public ResultSet getResultSet() {
    return resultSet;
  }

  @Override
  public void close() {
    if (getResultSet() != null) {
      getResultSet().close();
    }
  }
}
