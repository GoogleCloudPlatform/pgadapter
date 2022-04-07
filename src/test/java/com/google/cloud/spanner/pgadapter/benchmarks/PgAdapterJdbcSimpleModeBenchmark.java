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

package com.google.cloud.spanner.pgadapter.benchmarks;

public class PgAdapterJdbcSimpleModeBenchmark extends AbstractPgAdapterJdbcBenchmark {

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the simple mode for
   * queries and DML statements. This makes the JDBC driver behave in (much) the same way as psql.
   */
  protected String createUrl(PgAdapterBenchmarkState server) {
    return String.format(
        "jdbc:postgresql://localhost:%d/?preferQueryMode=simple", server.pgServer.getLocalPort());
  }
}
