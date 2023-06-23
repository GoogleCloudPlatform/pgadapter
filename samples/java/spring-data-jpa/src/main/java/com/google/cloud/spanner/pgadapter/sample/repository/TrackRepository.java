package com.google.cloud.spanner.pgadapter.sample.repository;

import com.google.cloud.spanner.pgadapter.sample.model.Track;
import com.google.cloud.spanner.pgadapter.sample.model.Track.TrackId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.CrudRepository;

public interface TrackRepository extends JpaRepository<Track, TrackId> {}
