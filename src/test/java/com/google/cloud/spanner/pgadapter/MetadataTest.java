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

import com.google.cloud.spanner.pgadapter.metadata.DynamicCommandMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Test;

public class MetadataTest {

  @Test
  public void testDynamicCommandMetadataSingleCommand() throws Exception {
    String inputJSON = ""
        + "{"
        + " \"commands\": "
        + "   [ "
        + "     {"
        + "       \"input_pattern\": \"this is the input SQL query\", "
        + "       \"output_pattern\": \"this is the output SQL query\", "
        + "       \"matcher_array\": []"
        + "     }"
        + "   ]"
        + "}";

    JSONParser parser = new JSONParser();

    List<DynamicCommandMetadata> result = DynamicCommandMetadata
        .fromJSON((JSONObject) parser.parse(inputJSON));
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("this is the output SQL query", result.get(0).getOutputPattern());
    Assert.assertEquals("this is the input SQL query", result.get(0).getInputPattern());
    Assert.assertEquals(new ArrayList<>(), result.get(0).getMatcherOrder());
  }

  @Test
  public void testDynamicCommandMetadataMultipleCommands() throws Exception {
    String inputJSON = ""
        + "{"
        + " \"commands\": "
        + "   [ "
        + "     {"
        + "       \"input_pattern\": \"this is the input SQL query\", "
        + "       \"output_pattern\": \"this is the output SQL query\", "
        + "       \"matcher_array\": []"
        + "     },"
        + "     {"
        + "       \"input_pattern\": \"this is another input SQL query\", "
        + "       \"output_pattern\": \"this is another output SQL query\", "
        + "       \"matcher_array\": [\"1\", \"2\", \"3\", \"4\", \"5\", \"6\", \"7\", \"8\", \"9\", \"10\"]"
        + "     }"
        + "   ]"
        + "}";

    JSONParser parser = new JSONParser();

    List<String> firstResult = new ArrayList<>();
    List<String> secondResult = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

    List<DynamicCommandMetadata> result = DynamicCommandMetadata
        .fromJSON((JSONObject) parser.parse(inputJSON));
    Assert.assertEquals(2, result.size());
    Assert.assertEquals("this is the output SQL query", result.get(0).getOutputPattern());
    Assert.assertEquals("this is the input SQL query", result.get(0).getInputPattern());
    Assert.assertEquals(firstResult, result.get(0).getMatcherOrder());
    Assert.assertEquals("this is another output SQL query", result.get(1).getOutputPattern());
    Assert.assertEquals("this is another input SQL query", result.get(1).getInputPattern());
    Assert.assertEquals(secondResult, result.get(1).getMatcherOrder());
  }

}
