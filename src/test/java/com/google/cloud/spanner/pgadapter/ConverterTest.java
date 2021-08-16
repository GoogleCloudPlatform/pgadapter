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

package com.google.cloud.spanner.pgadapter;

import com.google.cloud.spanner.pgadapter.metadata.SQLMetadata;
import com.google.cloud.spanner.pgadapter.utils.Converter;
import com.google.common.collect.ImmutableSetMultimap;
import org.junit.Assert;
import org.junit.Test;

public class ConverterTest {

  @Test
  public void testToJDBCParams() {
    String sqlStatement =
        "SELECT * FROM users WHERE name = $1 AND age > $2 AND age < $3"; // Simple case
    String expectedResult = "SELECT * FROM users WHERE name = ? AND age > ? AND age < ?";
    int parameterCount = 3;
    SQLMetadata result = Converter.toJDBCParams(sqlStatement);
    Assert.assertEquals(result.getSqlString(), expectedResult);
    Assert.assertEquals(result.getParameterCount(), parameterCount);
    Assert.assertEquals(
        result.getParameterIndexToPositions(), ImmutableSetMultimap.of(0, 1, 1, 2, 2, 3));

    // Unexpected case, but should follow logic
    sqlStatement = "SELECT * FROM users WHERE name = $$1 AND AGE = $1$";
    expectedResult = "SELECT * FROM users WHERE name = $? AND AGE = ?$";
    parameterCount = 1;
    result = Converter.toJDBCParams(sqlStatement);
    Assert.assertEquals(result.getSqlString(), expectedResult);
    Assert.assertEquals(result.getParameterCount(), parameterCount);
    Assert.assertEquals(result.getParameterIndexToPositions(), ImmutableSetMultimap.of(0, 1, 0, 2));

    sqlStatement =
        "SELECT * FROM users WHERE data = \"this data should not be changed $123 $$\" AND name = $1"; // Text should not be modified
    expectedResult =
        "SELECT * FROM users WHERE data = \"this data should not be changed $123 $$\" AND name = ?";
    parameterCount = 1;
    result = Converter.toJDBCParams(sqlStatement);
    Assert.assertEquals(result.getSqlString(), expectedResult);
    Assert.assertEquals(result.getParameterCount(), parameterCount);
    Assert.assertEquals(result.getParameterIndexToPositions(), ImmutableSetMultimap.of(0, 1));

    sqlStatement =
        "SELECT * FROM users WHERE data = \'this data should not be changed $123 $$\' AND name = $1"; // idem
    expectedResult =
        "SELECT * FROM users WHERE data = \'this data should not be changed $123 $$\' AND name = ?";
    parameterCount = 1;
    result = Converter.toJDBCParams(sqlStatement);
    Assert.assertEquals(result.getSqlString(), expectedResult);
    Assert.assertEquals(result.getParameterCount(), parameterCount);
    Assert.assertEquals(result.getParameterIndexToPositions(), ImmutableSetMultimap.of(0, 1));

    sqlStatement = "SELECT * FROM users WHERE data = \\$1234"; // unexpected, but let's test escapes
    parameterCount = 0;
    result = Converter.toJDBCParams(sqlStatement);
    Assert.assertEquals(result.getSqlString(), sqlStatement);
    Assert.assertEquals(result.getParameterCount(), parameterCount);

    // Out of order and multiple instances of the same index.
    sqlStatement = "SELECT * FROM users WHERE (name = $2 OR surname = $2) AND age = $1";
    expectedResult = "SELECT * FROM users WHERE (name = ? OR surname = ?) AND age = ?";
    parameterCount = 2;
    result = Converter.toJDBCParams(sqlStatement);
    Assert.assertEquals(result.getSqlString(), expectedResult);
    Assert.assertEquals(result.getParameterCount(), parameterCount);
    Assert.assertEquals(
        result.getParameterIndexToPositions(), ImmutableSetMultimap.of(0, 3, 1, 1, 1, 2));

    // Some parameter indices missing, this expects 123 parameters and would use only the last one.
    sqlStatement = "SELECT $123";
    expectedResult = "SELECT ?";
    parameterCount = 123;
    result = Converter.toJDBCParams(sqlStatement);
    Assert.assertEquals(result.getSqlString(), expectedResult);
    Assert.assertEquals(result.getParameterCount(), parameterCount);
    Assert.assertEquals(result.getParameterIndexToPositions(), ImmutableSetMultimap.of(122, 1));
  }
}
