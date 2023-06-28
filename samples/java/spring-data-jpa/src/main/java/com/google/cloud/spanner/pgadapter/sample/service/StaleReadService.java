package com.google.cloud.spanner.pgadapter.sample.service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.hibernate.Session;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class StaleReadService {
  @PersistenceContext
  private EntityManager entityManager;

  public <T> T executeReadOnlyTransactionWithStaleness(Supplier<T> transaction) {
    return entityManager.unwrap(Session.class).doReturningWork(connection -> {
      try {
        connection.createStatement().execute("begin transaction");
        connection.createStatement().execute("set transaction read only");
        connection.createStatement().execute("set spanner.read_only_staleness='exact_staleness 10s'");
        T result = transaction.get();
        connection.commit();
        return result;
      } catch (Throwable t) {
        connection.rollback();
        throw t;
      } finally {
        connection.createStatement().execute("set spanner.read_only_staleness='strong'");
      }
    });
  }

}
