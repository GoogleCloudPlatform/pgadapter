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

import static com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.EMPTY_LOCAL_STATEMENTS;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.statements.local.LocalStatement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import javax.annotation.Nonnull;

@InternalApi
public class ApplicationDetector {
  /**
   * A list of all well-known applications that have custom handling in PGAdapter. The difference
   * between a {@link com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient}
   * and a {@link WellKnownApplication} is that an application is executed using a client. That is;
   * the application handling comes on top of the client handling. Liquibase for example always uses
   * JDBC as the client, and adds the specific application handling for Liquibase on top of the JDBC
   * handling.
   */
  public enum WellKnownApplication {
    LIQUIBASE,
    UNSPECIFIED;

    String getApplicationName() {
      return name();
    }

    /** Returns the additional local statements for an application. */
    public ImmutableList<LocalStatement> getLocalStatements(ConnectionHandler connectionHandler) {
      return EMPTY_LOCAL_STATEMENTS;
    }
  }

  /** Returns the {@link WellKnownApplication} that has the specified application name. */
  public static @Nonnull WellKnownApplication detectApplication(@Nonnull String applicationName) {
    Preconditions.checkNotNull(applicationName);
    for (WellKnownApplication client : WellKnownApplication.values()) {
      if (applicationName.equalsIgnoreCase(client.getApplicationName())) {
        return client;
      }
    }
    return WellKnownApplication.UNSPECIFIED;
  }
}
