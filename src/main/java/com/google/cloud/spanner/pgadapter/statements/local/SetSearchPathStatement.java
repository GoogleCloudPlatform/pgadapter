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

package com.google.cloud.spanner.pgadapter.statements.local;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;

/**
 * A no-op SET search_path implementation. This should be removed once support has been added to the
 * Connection API.
 */
// TODO: Remove this once search_path support has been added to the Connection API.
@InternalApi
public class SetSearchPathStatement implements LocalStatement {
  public static final SetSearchPathStatement INSTANCE = new SetSearchPathStatement();

  private SetSearchPathStatement() {}

  @Override
  public String[] getSql() {
    return new String[] {
      "set search_path to public",
      "SET search_path TO public",
      "SET SEARCH_PATH TO public",
      "set search_path to \"public\"",
      "SET search_path TO \"public\"",
      "SET SEARCH_PATH TO \"public\"",
      "set search_path to \"$user\", public",
      "SET search_path TO \"$user\", public",
      "SET SEARCH_PATH TO \"$user\", public",
      "set search_path to \"$user\", \"public\"",
      "SET search_path TO \"$user\", \"public\"",
      "SET SEARCH_PATH TO \"$user\", \"public\"",
      // Some tools prepend the current search_path with 'public' without properly checking whether
      // 'public' is already part of the existing search path.
      "set search_path to public, public",
      "SET search_path TO public, public",
      "SET SEARCH_PATH TO public, public",
      "set search_path to public, \"public\"",
      "SET search_path TO public, \"public\"",
      "SET SEARCH_PATH TO public, \"public\"",
      "set search_path to public, \"$user\", public",
      "SET search_path TO public, \"$user\", public",
      "SET SEARCH_PATH TO public, \"$user\", public",
      "set search_path to public, \"$user\", \"public\"",
      "SET search_path TO public, \"$user\", \"public\"",
      "SET SEARCH_PATH TO public, \"$user\", \"public\"",
    };
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    return new NoResult("SET");
  }
}
