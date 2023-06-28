package com.google.cloud.spanner.pgadapter.sample.repository;

import com.google.cloud.spanner.pgadapter.sample.model.Album;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AlbumRepository extends JpaRepository<Album, String> {}
