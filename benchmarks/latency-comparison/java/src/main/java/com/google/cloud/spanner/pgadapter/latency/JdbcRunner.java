// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.latency;

import com.google.cloud.spanner.DatabaseId;

public class JdbcRunner extends AbstractJdbcRunner {

  JdbcRunner(DatabaseId databaseId) {
    super(databaseId);
  }

  @Override
  protected String createUrl() {
    return String.format("jdbc:cloudspanner:/%s", databaseId.getName());
  }
}
