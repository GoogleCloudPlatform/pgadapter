package com.google.cloud.spanner.pgadapter.sample.repository;

import com.google.cloud.spanner.pgadapter.sample.model.Album;
import org.springframework.data.repository.CrudRepository;

public interface AlbumRepository extends CrudRepository<Album, String> {
  
}
