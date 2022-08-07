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

package com.google.cloud.spanner.pgadapter.utils;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import java.util.Random;
import javax.annotation.Nullable;

/**
 * Generates tips for PGAdapter usage and options. This is used to show hints for new features and
 * general tips for psql connections. Hints are not generated for other (non-text) clients.
 */
@InternalApi
public class HintGenerator {
  public static class Hint {
    private final String message;
    private final String hint;

    private Hint(String message, String hint) {
      // Add an extra space to align the 'TIP' with the 'HINT'.
      this.message = " " + message;
      this.hint = hint;
    }

    public String getMessage() {
      return message;
    }

    public String getHint() {
      return hint;
    }
  }

  private HintGenerator() {}

  /** Generates a random hint for the given connection. */
  public static @Nullable Hint getRandomHint(ConnectionHandler connection) {
    OptionsMetadata options = connection.getServer().getOptions();
    if (!options.showHints()) {
      return null;
    }

    ImmutableList.Builder<Hint> builder = ImmutableList.builder();
    // Authentication hints.
    if (!options.shouldAuthenticate()) {
      builder.add(
          new Hint(
              "Start PGAdapter with -a to enable authentication.",
              "See https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/authentication.md"));
    }

    // Connection hints.
    if (options.hasDefaultConnectionUrl()) {
      builder.add(
          new Hint(
              "Start PGAdapter without the -d option to enable the use of the \\c meta-command.",
              "See https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/connection_options.md"));
    } else {
      builder.add(
          new Hint(
              "You can connect to any Cloud Spanner database using a fully qualified database name.",
              "Example: 'psql -d \"projects/my-project/instances/my-instance/databases/my-database\".\n"
                  + "See https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/connection_options.md"));
    }
    if (options.isDomainSocketEnabled() && connection.isTcpConnection()) {
      builder.add(
          new Hint(
              "PGAdapter supports Unix domain sockets.",
              String.format(
                  "Use '-h %s' instead of '-h localhost' to connect using Unix domain sockets.",
                  options.getSocketDir())));
    }

    // COPY hints.
    builder.add(
        new Hint(
            "Use COPY my_table FROM STDIN [BINARY] to bulk insert data into a table from a local file or a PostgreSQL database.",
            "See https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/copy.md"));
    builder.add(
        new Hint(
            "COPY my_table FROM STDIN supports non-atomic operations to copy more than 20,000 mutations.",
            "Execute 'SET SPANNER.AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC;' before executing the COPY command.\n"
                + "See https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/copy.md#non-atomic-copy-from-stdin-example"));
    builder.add(
        new Hint(
            "Use COPY my_table TO STDOUT [BINARY] to copy all data from a Cloud Spanner table to a PostgreSQL database or to a local file.",
            "See https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/copy.md"));

    // DDL hints.
    builder.add(
        new Hint(
            "Use the PGAdapter '-ddl' command line argument to emulate support for DDL transactions.",
            "See https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/ddl.md"));
    ImmutableList<Hint> possibleHints = builder.build();
    if (possibleHints.isEmpty()) {
      return null;
    }
    return possibleHints.get(new Random().nextInt(possibleHints.size()));
  }
}
