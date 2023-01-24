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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.cloud.spanner.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryReplacement {

  private String inputCommand;
  @JsonIgnore private Statement outputQuery;
  @JsonIgnore private Set<MatcherType> matcherSet = Collections.emptySet();
  private QueryAction action;
  private OverrideOperation columnOverrideType = OverrideOperation.NOT_APPLICABLE;
  private ColumnOverride columnOverride;
  private OverrideOperation tableOverrideType = OverrideOperation.NOT_APPLICABLE;
  private String tableOverrideValue;
  private OverrideOperation schemaOverrideType = OverrideOperation.NOT_APPLICABLE;
  private String schemaOverrideValue;

  public QueryReplacement(Statement sourceStatement) {
    this.inputCommand = sourceStatement.getSql();
    this.outputQuery = sourceStatement;
  }

  public QueryReplacement() {}

  public String getInputCommand() {
    return inputCommand;
  }

  public void setOutputCommand(String outputCommand) {
    this.outputQuery = Statement.of(outputCommand);
  }

  public Set<MatcherType> getMatcherSet() {
    return matcherSet;
  }

  public void setMatcherArray(List<MatcherType> matcherArray) {
    this.matcherSet = new HashSet<>(matcherArray);
  }

  public void setAction(QueryAction action) {
    this.action = action;
  }

  public QueryAction getAction() {
    return action;
  }

  public void setColumnOverrideType(OverrideOperation columnOverrideType) {
    this.columnOverrideType = columnOverrideType;
  }

  public void setColumnOverride(ColumnOverride columnOverride) {
    this.columnOverride = columnOverride;
  }

  public Statement getOutputQuery() {
    return outputQuery;
  }

  public String overrideSchema(String schemaName) {
    return schemaOverrideType == OverrideOperation.NAME_OVERRIDE ? schemaOverrideValue : schemaName;
  }

  public String overrideTableName(String tableName) {
    return tableOverrideType == OverrideOperation.NAME_OVERRIDE ? tableOverrideValue : tableName;
  }

  public String overrideColumn(String resultSetColumnName) {
    switch (columnOverrideType) {
      case NOT_APPLICABLE:
        return resultSetColumnName;
      case PREFIX:
        return columnOverride.getPrefix().concat(resultSetColumnName);
      case SUFFIX:
        return resultSetColumnName.concat(columnOverride.getSuffix());
      case NAME_OVERRIDE:
        return columnOverride.getColumnNameOverride(resultSetColumnName);
      default:
        return resultSetColumnName;
    }
  }

  @Override
  public String toString() {
    return "QueryReplacement{"
        + "inputCommand='"
        + inputCommand
        + '\''
        + ", outputQuery="
        + outputQuery
        + ", matcherSet="
        + matcherSet
        + ", action="
        + action
        + ", columnOverrideType="
        + columnOverrideType
        + ", columnOverride="
        + columnOverride
        + '}';
  }
}
