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

import com.google.common.collect.SetMultimap;

/**
 * Simple POJO for storing SQL statement metadata (usually extracted proxy-side), meant generally
 * for prepared statements/portals.
 */
public class SQLMetadata {

  private final String sqlString;
  private final int parameterCount;
  // Multimap from the 0-based parameter index to the associated set of JDBC parameter positions
  // (1-based).
  private final SetMultimap<Integer, Integer> parameterIndexToPositions;

  public SQLMetadata(
      String sqlString,
      int totalParameters,
      SetMultimap<Integer, Integer> parameterIndexToPositions) {
    this.sqlString = sqlString;
    this.parameterCount = totalParameters;
    this.parameterIndexToPositions = parameterIndexToPositions;
  }

  public int getParameterCount() {
    return this.parameterCount;
  }

  public String getSqlString() {
    return this.sqlString;
  }

  public SetMultimap<Integer, Integer> getParameterIndexToPositions() {
    return this.parameterIndexToPositions;
  }
}
