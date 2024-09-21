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

import com.google.cloud.spanner.pgadapter.sample.model.Album;
import com.google.cloud.spanner.pgadapter.sample.model.Concert;
import com.google.cloud.spanner.pgadapter.sample.model.Singer;
import com.google.cloud.spanner.pgadapter.sample.model.Venue;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;
import java.sql.Statement;
import java.util.List;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * This service shows how to use DML batching to execute a large number of DML statements as a
 * single ExecuteBatchDml request when using Spring Data JPA / Hibernate with PGAdapter.
 */
@Service
public class BatchService {
  private static final Logger log = LoggerFactory.getLogger(BatchService.class);

  private final SingerService singerService;
  private final AlbumService albumService;
  private final VenueService venueService;
  private final ConcertService concertService;

  @PersistenceContext private EntityManager entityManager;

  public BatchService(
      SingerService singerService,
      AlbumService albumService,
      VenueService venueService,
      ConcertService concertService) {
    this.singerService = singerService;
    this.albumService = albumService;
    this.venueService = venueService;
    this.concertService = concertService;
  }

  /**
   * Generates a batch of random singers, albums, venues and concerts in one DML batch. This method
   * shows how to unwrap the underlying JDBC connection and use this to execute custom SQL
   * statements. This is used to create a single batch of DML statements that is sent as one request
   * to Spanner. This can significantly reduce the number of round-trips between PGAdapter and
   * Spanner when executing a large number of DML statements.
   *
   * <p>See also <a
   * href="https://cloud.google.com/spanner/docs/pgadapter-session-mgmt-commands#batch-statements">
   * PGAdapter Batch Statements </a>.
   */
  @Transactional
  public void generateRandomDataInOneBatch(int count) {
    log.info("Generating {} singers and other random data in one batch", count);

    // Get hold of the underlying Hibernate session.
    Session session = entityManager.unwrap(Session.class);
    // Get hold of the underlying JDBC connection that is used by the current Hibernate session.
    // This connection can be used to execute custom SQL statements to change the state of the
    // connection.
    session.doWork(
        connection -> {
          try (Statement statement = connection.createStatement()) {
            // The 'spanner.dml_batch_update_count' variable determines the update count that
            // PGAdapter returns for DML statements that are buffered in a DML batch.
            // The default is 0. This method sets that to 1, as Hibernate expects all inserts to
            // return update count 1.
            // Note that 'set local' means 'change the value of this variable for the duration of
            // the
            // current transaction'. The value is automatically reset to the default when the
            // transaction ends.
            statement.execute("set local spanner.dml_batch_update_count=1");
            // Start a DML batch. From this point, only DML statements are allowed, and all DML
            // statements will be buffered locally in PGAdapter and not actually sent to Spanner.
            statement.execute("start batch dml");

            // Generate some random data.
            List<Singer> singers = singerService.generateRandomSingers(count);
            List<Album> albums = albumService.generateRandomAlbums(count, singers);
            List<Venue> venues = venueService.generateRandomVenues(count);
            List<Concert> concerts = concertService.generateRandomConcerts(count, singers, venues);

            // Make sure that we flush everything that has been cached by Hibernate. This ensures
            // that all DML statements are actually sent to PGAdapter. PGAdapter still buffers these
            // in memory, and does not yet send them to Spanner.
            session.flush();

            // Execute the DML batch. This will flush all buffered DML statements from PGAdapter to
            // Spanner. Note that this does not commit the transaction. That happens when the method
            // call to generateRandomDataInOneBatch(int) has finished, as that method is marked as
            // transactional.
            connection.createStatement().execute("run batch");
            log.info(
                "Generated {} singers, {} albums, {} venues, and {} concerts in one batch",
                singers.size(),
                albums.size(),
                venues.size(),
                concerts.size());
          }
        });
  }
}
