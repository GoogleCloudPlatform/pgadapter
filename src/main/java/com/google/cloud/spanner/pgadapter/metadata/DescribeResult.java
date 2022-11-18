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
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.spanner.v1.StructType;
import java.util.Arrays;

@InternalApi
public class DescribeResult implements AutoCloseable {
  private final ResultSet resultSet;
  private final int[] parameters;

  public DescribeResult(int[] givenParameterTypes, ResultSet resultMetadata) {
    this.resultSet = resultMetadata;
    this.parameters = extractParameters(givenParameterTypes, resultMetadata);
  }

  static int[] extractParameters(int[] givenParameterTypes, ResultSet resultSet) {
    if (!resultSet.getMetadata().hasUndeclaredParameters()) {
      return givenParameterTypes;
    }
    return extractParameterTypes(
        givenParameterTypes, resultSet.getMetadata().getUndeclaredParameters());
  }

  static int[] extractParameterTypes(int[] givenParameterTypes, StructType parameters) {
    int[] result;
    int maxParamIndex = maxParamNumber(parameters);
    if (maxParamIndex == givenParameterTypes.length) {
      result = givenParameterTypes;
    } else {
      result =
          Arrays.copyOf(givenParameterTypes, Math.max(givenParameterTypes.length, maxParamIndex));
    }
    for (int i = 0; i < parameters.getFieldsCount(); i++) {
      // Only override parameter types that were not specified by the frontend.
      int paramIndex = Integer.parseInt(parameters.getFields(i).getName().substring(1)) - 1;
      if (paramIndex >= givenParameterTypes.length || givenParameterTypes[paramIndex] == 0) {
        result[paramIndex] = Parser.toOid(parameters.getFields(i).getType());
      }
    }
    return result;
  }

  static int maxParamNumber(StructType parameters) {
    int max = 0;
    for (int i = 0; i < parameters.getFieldsCount(); i++) {
      try {
        int paramIndex = Integer.parseInt(parameters.getFields(i).getName().substring(1));
        if (paramIndex > max) {
          max = paramIndex;
        }
      } catch (NumberFormatException numberFormatException) {
        throw PGExceptionFactory.newPGException(
            "Invalid parameter name: " + parameters.getFields(i).getName(), SQLState.InternalError);
      }
    }
    return max;
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
