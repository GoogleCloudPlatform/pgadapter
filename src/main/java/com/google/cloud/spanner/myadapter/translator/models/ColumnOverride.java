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

package com.google.cloud.spanner.myadapter.translator.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ColumnOverride {

  private String prefix;
  private String suffix;
  @JsonIgnore private Map<String, ColumnNameOverride> columnNameOverrides = Collections.emptyMap();

  public String getPrefix() {
    return prefix;
  }

  public String getSuffix() {
    return suffix;
  }

  public String getColumnNameOverride(String resultSetColumnName) {
    if (columnNameOverrides.containsKey(resultSetColumnName)) {
      return columnNameOverrides.get(resultSetColumnName).getOutputColumnName();
    }
    return resultSetColumnName;
  }

  public void setColumnNameOverrides(List<ColumnNameOverride> columnNameOverrides) {
    this.columnNameOverrides =
        columnNameOverrides.stream()
            .collect(
                Collectors.toMap(ColumnNameOverride::getResultSetColumnName, Function.identity()));
  }

  @Override
  public String toString() {
    return "ColumnOverride{"
        + "prefix='"
        + prefix
        + '\''
        + ", suffix='"
        + suffix
        + '\''
        + ", columnNameOverrides="
        + columnNameOverrides
        + '}';
  }
}
