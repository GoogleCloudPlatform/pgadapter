package com.google.cloud.spanner.pgadapter.sample.repository;

import com.google.cloud.spanner.pgadapter.sample.model.Concert;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.util.Optional;
import org.hibernate.Session;

public class ConcertStaleReadRepositoryImpl implements ConcertStaleReadRepository {
  @PersistenceContext
  private EntityManager entityManager;

  public ConcertStaleReadRepositoryImpl() {
  }

  @Override
  public Optional<Concert> staleFindById(Long id) {
    return Optional.empty();
//    entityManager.unwrap(Session.class).doWork(work -> {
//      work.createStatement().execute("set spanner.read_only_staleness='max_staleness 10s'");
//    });
//    try {
//      return repository.findById(id);
//    } finally {
//      entityManager.unwrap(Session.class).doWork(work -> {
//        work.createStatement().execute("set spanner.read_only_staleness='strong'");
//      });
//    }
  }
}
