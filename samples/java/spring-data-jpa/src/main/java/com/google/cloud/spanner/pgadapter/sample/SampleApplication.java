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

import com.google.cloud.spanner.pgadapter.sample.service.AlbumService;
import com.google.cloud.spanner.pgadapter.sample.service.SingerService;
import com.google.cloud.spanner.pgadapter.sample.service.TrackService;
import com.google.cloud.spanner.pgadapter.sample.service.VenueService;
import java.util.Random;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SampleApplication implements CommandLineRunner {
  private static final Logger log = LoggerFactory.getLogger(SampleApplication.class);
  
  private static final PGAdapter pgAdapter = new PGAdapter();
  
  public static void main(String[] args) {
    SpringApplication.run(SampleApplication.class, args);
  }
  
  private final SingerService singerService;
  
  private final AlbumService albumService;
  
  private final TrackService trackService;
  
  private final VenueService venueService;
  
  public SampleApplication(SingerService singerService, AlbumService albumService, TrackService trackService, VenueService venueService) {
    this.singerService = singerService;
    this.albumService = albumService;
    this.trackService = trackService;
    this.venueService = venueService;
  }
  
  @Override
  public void run(String... args) throws Exception {
    // First clear the current tables.
    albumService.deleteAllAlbums();
    singerService.deleteAllSingers();
    
    // Generate some random data.
    singerService.generateRandomSingers(10);
    log.info("Created 10 singers");
    albumService.generateRandomAlbums(30);
    log.info("Created 30 albums");
    trackService.generateRandomTracks(30, 20);
    log.info("Created 20 tracks each for 30 albums");
    venueService.generateRandomVenues(20);
    log.info("Created 20 venues");

    Random random = new Random();
    // Fetch and print some data using a read-only transaction.
    char c = (char)(random.nextInt(26) + 'a');
    singerService.printSingersWithLastNameStartingWith(String.valueOf(c).toUpperCase());
  }

  @PreDestroy
  public void onExit() {
    // Stop PGAdapter when the application is shut down.
    pgAdapter.stopPGAdapter();
  }

}
