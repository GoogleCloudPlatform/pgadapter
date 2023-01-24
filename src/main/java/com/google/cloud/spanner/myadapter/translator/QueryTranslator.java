// Copyright 2022 Google LLC
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
package com.google.cloud.spanner.myadapter.translator;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.myadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.myadapter.translator.models.QueryReplacement;
import com.google.cloud.spanner.myadapter.translator.models.QueryReplacementConfig;
import java.util.Map;
import java.util.logging.Logger;

public class QueryTranslator {

  private static final Logger logger = Logger.getLogger(QueryTranslator.class.getName());

  private final QueryReplacementConfig queryReplacementConfig;
  private final Map<String, QueryReplacement> completeMatcherQueryTranslatorMap;

  public QueryTranslator(OptionsMetadata optionsMetadata) {
    this.queryReplacementConfig = optionsMetadata.getQueryReplacementConfig();
    this.completeMatcherQueryTranslatorMap =
        queryReplacementConfig.getCompleteMatcherReplacementMap();
  }

  public QueryReplacement translatedQuery(
      ParsedStatement parsedStatement, Statement originalStatement) {
    return completeMatcherQueryTranslatorMap.getOrDefault(
        parsedStatement.getSqlWithoutComments(), new QueryReplacement(originalStatement));
  }
}
