/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.pgadapter.metadata;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/** Parses command metadata json files. */
public class CommandMetadataParser {
  private static final String DEFAULT_FILE = "metadata/default_command_metadata.json";
  private static final String EMPTY_FILE = "metadata/empty_command_metadata.json";
  private final JSONParser jsonParser;

  public CommandMetadataParser() {
    this.jsonParser = new JSONParser();
  }

  public JSONObject parse(String fileName) throws IOException, ParseException {
    return parse(new FileInputStream(fileName));
  }

  public JSONObject emptyCommands() throws IOException, ParseException {
    return parse(this.getClass().getClassLoader().getResourceAsStream(EMPTY_FILE));
  }

  public JSONObject defaultCommands() throws IOException, ParseException {
    return parse(this.getClass().getClassLoader().getResourceAsStream(DEFAULT_FILE));
  }

  private JSONObject parse(InputStream inputStream) throws IOException, ParseException {
    try (InputStreamReader reader = new InputStreamReader(inputStream)) {
      final JSONObject commandsJson = (JSONObject) jsonParser.parse(reader);
      if (commandsJson == null) {
        throw new IllegalArgumentException("Commands metadata must not be null");
      }
      return commandsJson;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          "Unable to parse commands metadata file, object expected at the start of file", e);
    }
  }
}
