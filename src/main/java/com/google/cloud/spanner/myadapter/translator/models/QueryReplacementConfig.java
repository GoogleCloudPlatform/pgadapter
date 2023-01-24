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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryReplacementConfig {

  private List<QueryReplacement> commands;

  public List<QueryReplacement> getCommands() {
    return commands;
  }

  public Map<String, QueryReplacement> getCompleteMatcherReplacementMap() {
    return commands.stream()
        .filter(command -> command.getMatcherSet().contains(MatcherType.COMPLETE))
        .collect(Collectors.toMap(QueryReplacement::getInputCommand, Function.identity()));
  }

  @Override
  public String toString() {
    return "QueryReplacementConfig{" + "commands=" + commands + '}';
  }
}
