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

package com.google.cloud.spanner.pgadapter.sample.service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Supplier;
import org.hibernate.Session;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class StaleReadService {
  @PersistenceContext private EntityManager entityManager;

  public OffsetDateTime getCurrentTimestamp() {
    return entityManager
        .unwrap(Session.class)
        .doReturningWork(
            connection -> {
              try (ResultSet resultSet =
                  connection.createStatement().executeQuery("select current_timestamp")) {
                if (resultSet.next()) {
                  return resultSet.getObject(1, OffsetDateTime.class);
                }
                // This should not happen.
                throw new IllegalStateException("No timestamp returned by the server");
              }
            });
  }

  @Transactional(readOnly = true)
  public <T> T executeReadOnlyTransactionAtTimestamp(
      OffsetDateTime timestamp, Supplier<T> transaction) {
    return executeReadOnlyTransactionWithStaleness(
        "read_timestamp " + timestamp.format(DateTimeFormatter.ISO_DATE_TIME), transaction);
  }

  @Transactional(readOnly = true)
  public <T> T executeReadOnlyTransactionWithStaleness(String staleness, Supplier<T> transaction) {
    return entityManager
        .unwrap(Session.class)
        .doReturningWork(
            connection -> {
              try (Statement statement = connection.createStatement()) {
                try {
                  statement.execute(
                      String.format("set spanner.read_only_staleness='%s'", staleness));
                  return transaction.get();
                } catch (Throwable t) {
                  statement.execute("rollback");
                  throw t;
                } finally {
                  // NOTE: Calling 'commit' if there is no active transaction is a no-op. That means
                  // that if the transaction was rolled back in case of an exception, this commit
                  // will be a no-op.
                  statement.execute("commit");
                  // Reset the read_only_staleness of the connection.
                  connection.createStatement().execute("set spanner.read_only_staleness='strong'");
                }
              }
            });
  }
}
