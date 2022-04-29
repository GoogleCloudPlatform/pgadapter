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

package com.google.cloud.spanner.pgadapter.commands;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.metadata.DynamicCommandMetadata;
import com.google.cloud.spanner.pgadapter.utils.StatementParser;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Dynamic command allows the specification of user generated commands. Via the generation of a JSON
 * file, a user may determine input and output parameters as well as matchers counts, which will be
 * used to dynamically generate a matcher for a specific run.
 */
@InternalApi
public class DynamicCommand extends Command {
  private final DynamicCommandMetadata metadata;

  DynamicCommand(String sql, DynamicCommandMetadata metadata) {
    super(getPattern(metadata).matcher(sql));
    this.metadata = metadata;
  }

  @Override
  public Pattern getPattern() {
    return getPattern(this.metadata);
  }

  private static Pattern getPattern(DynamicCommandMetadata metadata) {
    return Pattern.compile(metadata.getInputPattern());
  }

  @Override
  public String translate() {
    if (this.metadata.getMatcherOrder().isEmpty()) {
      return this.matcher.replaceAll(this.metadata.getOutputPattern());
    } else {
      List<String> matcherList = new ArrayList<>();
      for (String argumentPosition : this.metadata.getMatcherOrder()) {
        matcherList.add(StatementParser.singleQuoteEscape(this.matcher.group(argumentPosition)));
      }
      return String.format(this.metadata.getOutputPattern(), matcherList.toArray());
    }
  }
}
