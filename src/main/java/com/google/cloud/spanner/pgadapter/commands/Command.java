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
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.pgadapter.metadata.DynamicCommandMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.simple.JSONObject;

/**
 * This abstract class concerns itself with representing a method of matching a specific statement
 * and translating it to a form that Spanner understands. The user should call is() to determine
 * whether it matches before trying to translate.
 */
@InternalApi
public abstract class Command {

  protected Matcher matcher;
  protected String sql;

  Command(String sql) {
    this.sql = sql;
    this.matcher = getPattern().matcher(sql);
  }

  /**
   * This constructor should only be called if you do wish to do your own matching (i.e.: set up
   * this.matcher). Useful if the pattern is not static, or not pattern related (in which case you
   * may set it to null).
   */
  Command(Matcher matcher) {
    this.matcher = matcher;
  }

  /**
   * The list of all subcommands to this command. When subclassing this, make sure to add the new
   * command to the below list, if applicable (i.e.: if you want it to be run). Commands may also be
   * specified via Dynamic Commands, which are defined from an input JSON file. These commands
   * precede the hard-coded ones in execution.
   *
   * @param sql The SQL command to be translated.
   * @param connection The connection currently in use.
   * @return A list of all Command subclasses.
   */
  public static List<Command> getCommands(
      String sql, Connection connection, JSONObject commandMetadataJSON) {
    List<Command> commandList = new ArrayList<>();

    for (DynamicCommandMetadata currentMetadata :
        DynamicCommandMetadata.fromJSON(commandMetadataJSON)) {
      commandList.add(new DynamicCommand(sql, currentMetadata));
    }

    commandList.addAll(
        Arrays.asList(new ListCommand(sql, connection), new InvalidMetaCommand(sql)));

    return commandList;
  }

  /**
   * The pattern expected by this command: is() will only return true if it matches this pattern.
   *
   * @return The pattern apposite (in relation to) the command.
   */
  protected abstract Pattern getPattern();

  /** @return True if the SQL statement matches the pattern. */
  public boolean is() {
    return this.matcher.find();
  }

  /** @return The equivalent Spanner statement for the specified SQL statement. */
  public abstract String translate();
}
