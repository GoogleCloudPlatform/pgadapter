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

package com.google.cloud.spanner.pgadapter.sample.service;

import com.google.cloud.spanner.pgadapter.sample.model.Album;
import com.google.cloud.spanner.pgadapter.sample.model.Concert;
import com.google.cloud.spanner.pgadapter.sample.model.Singer;
import com.google.cloud.spanner.pgadapter.sample.model.Track;
import com.google.cloud.spanner.pgadapter.sample.repository.SingerRepository;
import jakarta.transaction.Transactional;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SingerService {
  private static final Logger log = LoggerFactory.getLogger(SingerService.class);

  private final RandomDataService randomDataService;

  private final SingerRepository repository;

  public SingerService(RandomDataService randomDataService, SingerRepository repository) {
    this.randomDataService = randomDataService;
    this.repository = repository;
  }

  /**
   * Prints all singers whose last name start with the given prefix. Also prints the related albums
   * and tracks.
   *
   * <p>This method uses a read-only transaction. It is highly recommended to use a read-only
   * transaction for workloads that only read, as these do not take locks on Cloud Spanner.
   */
  @org.springframework.transaction.annotation.Transactional(readOnly = true)
  public void printSingersWithLastNameStartingWith(String prefix) {
    log.info("Fetching all singers whose last name start with an '{}'", prefix);
    for (Singer singer : repository.searchByLastNameStartsWith(prefix)) {
      log.info("Singer: {}", singer.getFullName());
      log.info("# albums: {}", singer.getAlbums().size());
      for (Album album : singer.getAlbums()) {
        log.info("  Album: {}", album.getTitle());
        log.info("  # tracks: {}", album.getTracks().size());
        for (Track track : album.getTracks()) {
          log.info("    Track #{}: {}", track.getTrackId().getTrackNumber(), track.getTitle());
        }
      }
      log.info("# concerts: {}", singer.getConcerts().size());
      for (Concert concert : singer.getConcerts()) {
        log.info("  Concert: {} starts at {}", concert.getName(), concert.getStartTime());
      }
    }
  }

  @Transactional
  public void deleteAllSingers() {
    repository.deleteAll();
  }

  @Transactional
  public List<Singer> generateRandomSingers(int count) {
    List<Singer> singers = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      Singer singer = new Singer();
      singer.setFirstName(randomDataService.getRandomFirstName());
      singer.setLastName(randomDataService.getRandomLastName());
      singers.add(singer);
    }
    return repository.saveAll(singers);
  }
}
