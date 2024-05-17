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

import java.time.Duration;
import java.util.List;

public interface BenchmarkRunner {
  enum TransactionType {
    READ_ONLY {
      @Override
      String getJdbcSql() {
        return "select col_varchar from latency_test where col_bigint=?";
      }

      @Override
      String getSql() {
        return "select col_varchar from latency_test where col_bigint=$1";
      }
    },
    READ_WRITE {
      @Override
      String getJdbcSql() {
        return "update latency_test set col_varchar=? where col_bigint=?";
      }

      @Override
      String getSql() {
        return "update latency_test set col_varchar=$1 where col_bigint=$2";
      }
    };

    abstract String getJdbcSql();

    abstract String getSql();
  }

  List<Duration> execute(
      TransactionType transactionType, int numClients, int numOperations, int waitMillis);
}
