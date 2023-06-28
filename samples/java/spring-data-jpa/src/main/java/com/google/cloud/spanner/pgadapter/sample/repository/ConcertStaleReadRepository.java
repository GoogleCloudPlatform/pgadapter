package com.google.cloud.spanner.pgadapter.sample.repository;

import com.google.cloud.spanner.pgadapter.sample.model.Concert;
import java.util.Optional;

public interface ConcertStaleReadRepository {

  Optional<Concert> staleFindById(Long id);

}
