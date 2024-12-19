// Copyright 2024 Google LLC
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

import com.google.cloud.spanner.connection.DirectedReadOptionsUtil;
import com.google.spanner.v1.DirectedReadOptions;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.sql.Statement;
import java.util.function.Supplier;
import org.hibernate.Session;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DirectedReadService {
  @PersistenceContext private EntityManager entityManager;

  /**
   * Runs a read-only transaction with the given {@link DirectedReadOptions}. The given {@link
   * DirectedReadOptions} will be activated on the connection that is used to run the transaction,
   * and reset after finishing the transaction.
   */
  @Transactional(readOnly = true)
  public <T> T executeReadOnlyTransactionWithDirectedRead(
      DirectedReadOptions directedReadOptions, Supplier<T> transaction) {
    return entityManager
        .unwrap(Session.class)
        .doReturningWork(
            connection -> {
              try (Statement statement = connection.createStatement()) {
                try {
                  statement.execute(
                      String.format(
                          "set spanner.directed_read='%s'",
                          DirectedReadOptionsUtil.toString(directedReadOptions)));
                  return transaction.get();
                } catch (Throwable t) {
                  // Note: Rolling back or committing a read-only transaction is semantically the
                  // same, as no changes to the database can be made in a read-only transaction.
                  // This method chooses to rollback in case of an error for readability.
                  statement.execute("rollback");
                  throw t;
                } finally {
                  // NOTE: Calling 'commit' if there is no active transaction is a no-op. That means
                  // that if the transaction was rolled back in case of an exception, this commit
                  // will be a no-op.
                  statement.execute("commit");
                  // Reset the read_only_staleness of the connection.
                  statement.execute("set spanner.directed_read=''");
                }
              }
            });
  }
}
