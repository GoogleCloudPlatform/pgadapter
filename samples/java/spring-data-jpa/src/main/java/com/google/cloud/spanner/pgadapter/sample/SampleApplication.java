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

package com.google.cloud.spanner.pgadapter.sample;

import com.google.cloud.spanner.pgadapter.sample.model.Concert;
import com.google.cloud.spanner.pgadapter.sample.repository.ConcertRepository;
import com.google.cloud.spanner.pgadapter.sample.service.AlbumService;
import com.google.cloud.spanner.pgadapter.sample.service.ConcertService;
import com.google.cloud.spanner.pgadapter.sample.service.SingerService;
import com.google.cloud.spanner.pgadapter.sample.service.StaleReadService;
import com.google.cloud.spanner.pgadapter.sample.service.TrackService;
import com.google.cloud.spanner.pgadapter.sample.service.VenueService;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Random;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Sample application using Spring Boot Data JPA with PGAdapter and a Cloud Spanner PostgreSQL
 * database.
 *
 * <p>This sample shows how to do the following:
 *
 * <ol>
 *   <li>Start PGAdapter in-process together with teh main application
 *   <li>Configure and use Spring Boot Data JPA with PGAdapter
 *   <li>Configure and use Liquibase with PGAdapter to create the schema of the database
 *   <li>Use UUID primary key values
 *   <li>Use auto-generated sequential primary key values without the risk of creating hotspots
 *   <li>Use interleaved tables with Spring Boot Data JPA
 *   <li>How to map all supported data types to the corresponding Java types
 *   <li>How to execute read/write and read-only transactions
 *   <li>How to execute stale reads on Cloud Spanner
 * </ol>
 */
@SpringBootApplication
public class SampleApplication implements CommandLineRunner {
  private static final Logger log = LoggerFactory.getLogger(SampleApplication.class);

  /**
   * {@link PGAdapter} is a small utility class for starting and stopping PGAdapter in-process with
   * the application.
   */
  private static final PGAdapter pgAdapter = new PGAdapter();

  private final SingerService singerService;
  private final AlbumService albumService;
  private final TrackService trackService;
  private final VenueService venueService;
  private final ConcertService concertService;
  /**
   * The {@link StaleReadService} is a generic service that can be used to execute workloads using
   * stale reads. Stale reads can perform better than strong reads. See <a
   * href="https://cloud.google.com/spanner/docs/timestamp-bounds">https://cloud.google.com/spanner/docs/timestamp-bounds</a>
   * for more information.
   */
  private final StaleReadService staleReadService;

  private final ConcertRepository concertRepository;

  public SampleApplication(
      SingerService singerService,
      AlbumService albumService,
      TrackService trackService,
      VenueService venueService,
      ConcertService concertService,
      StaleReadService staleReadService,
      ConcertRepository concertRepository) {
    this.singerService = singerService;
    this.albumService = albumService;
    this.trackService = trackService;
    this.venueService = venueService;
    this.concertService = concertService;
    this.staleReadService = staleReadService;
    this.concertRepository = concertRepository;
  }

  public static void main(String[] args) {
    SpringApplication.run(SampleApplication.class, args);
  }

  @Override
  public void run(String... args) throws Exception {
    // First clear the current tables.
    log.info("Deleting all existing data");
    concertService.deleteAllConcerts();
    albumService.deleteAllAlbums();
    singerService.deleteAllSingers();

    // Generate some random data.
    singerService.generateRandomSingers(10);
    log.info("Created 10 singers");
    albumService.generateRandomAlbums(30);
    log.info("Created 30 albums");
    trackService.generateRandomTracks(30, 15);
    log.info("Created 20 tracks each for 30 albums");
    venueService.generateRandomVenues(20);
    log.info("Created 20 venues");
    concertService.generateRandomConcerts(50);
    log.info("Created 50 concerts");

    // Print some of the randomly inserted data.
    printData();
    // Show how to do a stale read.
    staleRead();
  }

  void printData() {
    Random random = new Random();
    // Fetch and print some data using a read-only transaction.
    for (int n = 0; n < 3; n++) {
      char c = (char) (random.nextInt(26) + 'a');
      singerService.printSingersWithLastNameStartingWith(String.valueOf(c).toUpperCase());
    }
  }

  void staleRead() {
    // Check the number of concerts at this moment in the database.
    log.info("Found {} concerts using a strong read", concertRepository.findAll().size());
    // Insert a new concert and then do a stale read. That concert should then not be included in
    // the result of the stale read.
    OffsetDateTime currentTime = staleReadService.getCurrentTimestamp();
    log.info("Inserting a new concert");
    concertService.generateRandomConcerts(1);
    // List all concerts using a stale read. The read timestamp is before the insert of the latest
    // concert, which means that it will not be included in the query result, and the number of
    // concerts returned should be the same as the first query in this method.
    List<Concert> concerts =
        staleReadService.executeReadOnlyTransactionAtTimestamp(
            currentTime, concertRepository::findAll);
    log.info("Found {} concerts using a stale read.", concerts.size());
  }

  @PreDestroy
  public void onExit() {
    // Stop PGAdapter when the application is shut down.
    pgAdapter.stopPGAdapter();
  }
}
